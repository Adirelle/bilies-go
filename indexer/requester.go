package indexer

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	hostpool "github.com/bitly/go-hostpool"
)

type Requester interface {
	Send(io.Reader) (io.ReadCloser, error)
}

type requester struct {
	backendPool hostpool.HostPool
	client      http.Client

	username string
	password string

	MaxRetries int
}

func NewRequester(client http.Client, hosts []string, port int, protocol string, username string, password string) Requester {
	urls := make([]string, len(hosts))
	for host := range hosts {
		urls = append(urls, fmt.Sprintf("%s://%s:%d/_bulk", protocol, host, port))
	}
	return NewRequesterPool(client, hostpool.New(urls), username, password)
}

func NewRequesterPool(client http.Client, backendPool hostpool.HostPool, username string, password string) Requester {
	return &requester{
		backendPool: backendPool,
		client:      client,
		username:    username,
		password:    password,
		MaxRetries:  5,
	}
}

func (r *requester) Send(body io.Reader) (reply io.ReadCloser, err error) {
	for i := -1; i < r.MaxRetries; i++ {
		backendR := r.backendPool.Get()
		if backendR == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		reply, err = r.requestBackend(backendR.Host(), body)
		if isBackendError(err) {
			backendR.Mark(err)
		} else {
			backendR.Mark(nil)
			break
		}
	}
	return
}

func (r *requester) requestBackend(url string, body io.Reader) (reply io.ReadCloser, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)

	if req, err = http.NewRequest("POST", url, body); err != nil {
		return
	}
	//req.Header.Set("Expect", "100-Continue")
	if r.username != "" {
		req.SetBasicAuth(r.username, r.password)
	}

	if resp, err = r.client.Do(req); err == nil {
		reply = resp.Body
		err = newHttpError(*resp)
	}

	return
}

func isBackendError(err error) bool {
	switch e := err.(type) {
	case *HttpError:
		return e.StatusCode >= 500
	case *url.Error:
		return e.Temporary() || e.Timeout()
	}
	return false
}

type HttpError struct {
	http.Response
}

func newHttpError(resp http.Response) error {
	if resp.StatusCode < 400 {
		return nil
	}
	return &HttpError{Response: resp}
}

func (e HttpError) Temporary() bool {
	return e.Response.StatusCode >= 500
}

func (e HttpError) Timeout() bool {
	return false
}

func (e HttpError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.Response.Request.Method, e.Response.Request.URL.String(), e.Response.Status)
}
