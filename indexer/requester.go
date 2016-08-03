/*
bilies-go - Bulk Insert Logs Into ElasticSearch
<one line to give the program's name and a brief idea of what it does.>
Copyright (C) 2016 Adirelle <adirelle@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
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
		log.Debugf("Try #%d", i+2)
		e, ok := r.backendPool.Acquire()
		if !ok {
			log.Warningf("Could not acquire backend, bailing out")
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
		log.Errorf("Could not create request: %s", err.Error())
		return
	}
	//req.Header.Set("Expect", "100-Continue")
	if r.username != "" {
		req.SetBasicAuth(r.username, r.password)
	}

	log.Debugf("Sending request to %s", url)
	if resp, err = r.client.Do(req); err == nil {
		log.Debugf("Response: %s", resp.Status)
		reply = resp.Body
		err = newHTTPError(*resp)
	} else {
		log.Warningf("Request error: %s", err.Error())
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
