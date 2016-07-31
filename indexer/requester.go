package indexer

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Requester interface {
	Send(io.Reader) (io.ReadCloser, error)
}

type requester struct {
	backendPool Pool
	client      http.Client

	username string
	password string

	MaxRetries int
}

func NewRequester(client http.Client, hosts []string, port int, protocol string, username string, password string) Requester {
	urls := make([]interface{}, 0, len(hosts))
	for _, host := range hosts {
		urls = append(urls, fmt.Sprintf("%s://%s:%d/_bulk", protocol, host, port))
	}
	return NewRequesterPool(client, NewPool(urls), username, password)
}

func NewRequesterPool(client http.Client, backendPool Pool, username string, password string) Requester {
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
		e, ok := r.backendPool.Acquire()
		if !ok {
			return
		}
		reply, err = r.requestBackend(e.Object().(string), body)
		if isBackendError(err) {
			e.Release(true)
		} else {
			e.Release(false)
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
		err = newHTTPError(*resp)
	}

	return
}

func isBackendError(err error) bool {
	switch e := err.(type) {
	case *HTTPError:
		return e.StatusCode >= 500
	case *url.Error:
		return e.Temporary() || e.Timeout()
	}
	return false
}

type HTTPError struct {
	http.Response
}

func newHTTPError(resp http.Response) error {
	if resp.StatusCode < 400 {
		return nil
	}
	return &HTTPError{Response: resp}
}

func (e HTTPError) Temporary() bool {
	return e.Response.StatusCode >= 500
}

func (e HTTPError) Timeout() bool {
	return false
}

func (e HTTPError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.Response.Request.Method, e.Response.Request.URL.String(), e.Response.Status)
}
