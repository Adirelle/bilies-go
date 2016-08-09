/*
bilies-go - Bulk Insert Logs Into ElasticSearch
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
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type requester struct {
	urls     []string
	username string
	password string
	timeout  time.Duration
	maxTries int
	urlIndex int

	client http.Client
}

var errTimeout = errors.New("Request timeout")

func newRequester(protocol string, hosts []string, port int, username string, password string) *requester {
	urls := make([]string, 0, len(hosts))
	for _, host := range hosts {
		url := fmt.Sprintf("%s://%s:%d/_bulk", protocol, host, port)
		urls = append(urls, url)
	}
	return &requester{
		urls:     urls,
		username: username,
		password: password,
		timeout:  10 * time.Second,
		maxTries: 5,
		client:   http.Client{},
	}
}

func (r *requester) send(body io.Reader) (io.ReadCloser, error) {
	timeLimit := time.Now().Add(r.timeout)
	for i := 0; i < r.maxTries && timeLimit.After(time.Now()); i++ {
		url := r.urls[r.urlIndex]
		r.urlIndex = (r.urlIndex + 1) % len(r.urls)

		rep, err := r.sendTo(url, body)
		if err == nil || !isTemporary(err) {
			return rep, err
		}
	}
	return nil, errTimeout
}

type netError interface {
	Temporary() bool
	Timeout() bool
}

func isTemporary(err error) bool {
	switch e := err.(type) {
	case httpError:
		return e.StatusCode < 400
	case netError:
		return e.Temporary() || e.Timeout()
	}
	return false
}

func (r *requester) sendTo(url string, body io.Reader) (io.ReadCloser, error) {
	log.Debugf("Sending to %s", url)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		log.Warningf("Failed to create request: %s", err)
		return nil, err
	}
	req.Header.Add("Expect", "100-continue")
	if r.username != "" {
		req.SetBasicAuth(r.username, r.password)
	}

	resp, err := r.client.Do(req)
	if err == nil {
		err = newHTTPError(resp)
	}
	if err != nil {
		log.Warningf("Request failed: %s", err)
		return nil, err
	}

	log.Warningf("Request succeeded: %s", resp.Status)
	return resp.Body, nil
}

type httpError struct {
	Status     string
	StatusCode int
	Req        string
}

func newHTTPError(rep *http.Response) error {
	if rep.StatusCode >= 400 {
		return httpError{
			Status:     rep.Status,
			StatusCode: rep.StatusCode,
			Req:        fmt.Sprintf("%s %s", rep.Request.Method, rep.Request.URL.String()),
		}
	}
	return nil
}

func (e httpError) Error() string {
	return fmt.Sprintf("%s: %s", e.Req, e.Status)
}
