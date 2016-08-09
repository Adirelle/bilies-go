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

	metrics "github.com/rcrowley/go-metrics"
)

type requester struct {
	urls     []string
	username string
	password string
	timeout  time.Duration
	maxTries int
	urlIndex int

	client http.Client

	mBytes    metrics.Sample
	mTries    metrics.Sample
	mTime     metrics.Timer
	mRequests metrics.Meter
	mErrors   metrics.Meter
	mStatus   metrics.Registry
}

var errTimeout = errors.New("Request timeout")

func newRequester(protocol string, hosts []string, port int, username string, password string, mp metrics.Registry) *requester {
	urls := make([]string, 0, len(hosts))
	for _, host := range hosts {
		url := fmt.Sprintf("%s://%s:%d/_bulk", protocol, host, port)
		urls = append(urls, url)
	}

	m := metrics.NewPrefixedChildRegistry(mp, "requester.")
	r := &requester{
		urls:     urls,
		username: username,
		password: password,
		timeout:  10 * time.Second,
		maxTries: 5,

		client: http.Client{},

		mBytes:    metrics.NewUniformSample(1e5),
		mTries:    metrics.NewUniformSample(1e5),
		mTime:     metrics.GetOrRegisterTimer("time", m),
		mRequests: metrics.GetOrRegisterMeter("count", m),
		mErrors:   metrics.GetOrRegisterMeter("errors", m),
		mStatus:   metrics.NewPrefixedChildRegistry(m, "status."),
	}

	metrics.GetOrRegisterHistogram("bytes", m, r.mBytes)
	metrics.GetOrRegisterHistogram("tries", m, r.mTries)

	return r
}

func (r *requester) send(body io.Reader) (rep io.ReadCloser, err error) {
	timeLimit := time.Now().Add(r.timeout)
	numTries := 1
	for {
		url := r.urls[r.urlIndex]
		r.urlIndex = (r.urlIndex + 1) % len(r.urls)

		rep, err = r.sendTo(url, body)
		if err == nil || !isTemporary(err) {
			break
		}
		numTries++
		if numTries > r.maxTries {
			err = errTimeout
			break
		}
		if time.Now().After(timeLimit) {
			err = errTimeout
			break
		}
		log.Info("Temporary failure, retrying")
	}
	if err != nil {
		log.Warningf("Failed after %d tries: %s", numTries, err)
	} else {
		r.mTries.Update(int64(numTries))
		log.Infof("Succeeded after %d tries", numTries)
	}
	return
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

	var resp *http.Response
	r.mRequests.Mark(1)
	r.mTime.Time(func() { resp, err = r.client.Do(req) })
	r.mBytes.Update(int64(req.ContentLength))
	if err == nil {
		r.mStatus.GetOrRegister(resp.Status[:3], metrics.NewCounter).(metrics.Counter).Inc(1)
		err = newHTTPError(resp)
	}
	if err != nil {
		r.mErrors.Mark(1)
		log.Warningf("Request failed: %s", err)
		return nil, err
	}

	log.Infof("Request succeeded: %s", resp.Status)
	return resp.Body, nil
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

func isBadRequest(err error) bool {
	e, ok := err.(httpError)
	return ok && e.StatusCode == 400
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
