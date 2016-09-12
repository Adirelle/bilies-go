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

/*

Requests

bilies-go retries the requests indefinitively on network or 5xx errors. In case of 400 error, batchs are split
in smaller parts and send independently to find the culprit.

The following switchs control requests:

	-h --host=STRING [default: localhost]
		Hostname of a ElasticSearch servers. This switch can be used multiple times to add more severs.

	-P --protocol=(http|https) [default: http]
		Protocol to of the ElasticSearch servers.

	-p --port=INT [default: 9200]
		Port of the ElasticSearch servers.

	-u --user=STRING [default: none]
		Username for basic authentification.

	-w --passwd=STRING [default: none]
		Password for basic authentification.
*/
package main

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/rcrowley/go-metrics"
	"github.com/spf13/pflag"
	"github.com/ugorji/go/codec"

	"github.com/Adirelle/bilies-go/data"
)

var (
	hosts    = []string{"localhost"}
	protocol = "http"
	port     = 9200
	username string
	password string

	client      = http.Client{}
	backendURLs BackendURLPool

	mRequester     = metrics.NewPrefixedChildRegistry(mRoot, "requests.")
	mRequestSize   = metrics.NewRegisteredHistogram("size", mRequester, NewSample())
	mRequestTries  = metrics.NewRegisteredHistogram("tries", mRequester, NewSample())
	mRequestTime   = metrics.NewRegisteredTimer("time", mRequester)
	mRequestBytes  = metrics.NewRegisteredMeter("bytes", mRequester)
	mRequestCount  = metrics.NewRegisteredMeter("count", mRequester)
	mRequestErrors = metrics.NewRegisteredMeter("errors", mRequester)
	mRequestStatus = metrics.NewPrefixedChildRegistry(mRequester, "status.")
)

func init() {
	pflag.StringSliceVarP(&hosts, "host", "h", hosts, "Hostname of ElasticSearch server")
	pflag.StringVarP(&protocol, "protocol", "P", protocol, "Protocol : http | https")
	pflag.IntVarP(&port, "port", "p", port, "ElasticSearch port")
	pflag.StringVarP(&username, "user", "u", username, "Username for authentication")
	pflag.StringVarP(&password, "passwd", "w", password, "Password for authentication")

	AddTask("Requester", Requester)
}

func Requester() {
	backendURLs = NewBackendURLPool(hosts, protocol, port)
	for buf := range batchs {
		SendSlice(&buf, 0, buf.Count())
	}
}

func SendSlice(buf *IndexedBuffer, i, j int) (err error) {
	if i == j {
		return
	}
	logger.Debugf("Sending slice [%d:%d]", i, j)
	err = Send(buf, i, j)
	if err == nil {
		logger.Debugf("Successfully sent slice [%d:%d]", i, j)
		AckRecords(j - i)
		return
	}
	if e, ok := err.(HTTPError); !ok || e.StatusCode != 400 {
		logger.Errorf("Permanent error: %s", err)
		return
	}
	if j-i == 1 {
		logger.Errorf("Action rejected:\n%s", buf.Slice(i, j))
		AckRecords(1)
		return
	}

	h := (i + j) / 2
	logger.Debugf("Sending subslices [%d:%d] & [%d:%d]", i, h, h, j)
	if err = SendSlice(buf, i, h); err != nil {
		return
	}
	return SendSlice(buf, h, j)
}

func AckRecords(n int) {
	logger.Debugf("Acking %d records", n)
	queue.DropC <- n
}

func Send(buf *IndexedBuffer, i, j int) (err error) {
	var body = buf.Slice(i, j)
	for tries := 1; ; tries++ {
		select {
		case url := <-backendURLs.Get():
			var resp *data.ESResponse
			resp, err = SendTo(url.String(), body)
			if err == nil || !IsBackendError(err) {
				mRequestTries.Update(int64(tries))
				url.Release(false)
				if err == nil {
					logger.Debugf("Successfully sent %d bytes to %s", len(body), url)
				} else {
					logger.Errorf("%s replied with an error, bailing out. Cause: %s", url, err)
				}
				ReportItemFailures(buf, resp)
				return
			}
			url.Release(true)
			logger.Errorf("%s is failing, trying another backend: Cause: %s", url, err)
		case <-done:
			return errors.New("Shutting down")
		}
	}
}

func ReportItemFailures(buf *IndexedBuffer, resp *data.ESResponse) {
	if resp.Items == nil {
		return
	}
	for _, r := range resp.Items {
		s := r.Status()
		if s == nil {
			continue
		}
		if err := s.ToError(); err != nil {
			b, _ := buf.GetByKey(s.ID)
			logger.Warningf("Error: %s, ID: %s, Data:\n%s", err, s.ID, b)
		}
	}
}

func SendTo(url string, body []byte) (esResp *data.ESResponse, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)
	logger.Debugf("Sending %d bytes to %s:", len(body), url)
	req, err = http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Add("Expect", "100-continue")
	req.Header.Add("Accept", "application/json")
	if username != "" {
		req.SetBasicAuth(username, password)
	}

	mRequestTime.Time(func() { resp, err = client.Do(req) })
	if err == nil {
		mRequestCount.Mark(1)
		mRequestSize.Update(int64(len(body)))
		mRequestBytes.Mark(req.ContentLength)
		metrics.GetOrRegisterMeter(fmt.Sprintf("%d", resp.StatusCode), mRequestStatus).Mark(1)
	}
	if resp != nil {
		defer resp.Body.Close()
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
			if err = codec.NewDecoder(resp.Body, &codec.JsonHandle{}).Decode(&esResp); err != nil {
				logger.Errorf("Could not parse response: %s", err)
			} else {
				err = esResp.ToError()
			}
		} else {
			logger.Warningf("Unsupported content-type: %q", resp.Header.Get("Content-Type"))
		}
	}
	if err == nil {
		err = NewHTTPError(resp)
	}
	if err != nil {
		mRequestErrors.Mark(1)
	}
	return
}

func IsBackendError(err error) (is bool) {
	switch e := err.(type) {
	case HTTPError:
		is = e.StatusCode >= 500
	case *url.Error:
		is = true
	default:
		is = false
	}
	return
}

type HTTPError struct {
	Status     string
	StatusCode int
	Req        string
}

func NewHTTPError(rep *http.Response) error {
	if rep.StatusCode >= 400 {
		return HTTPError{
			Status:     rep.Status,
			StatusCode: rep.StatusCode,
			Req:        fmt.Sprintf("%s %s", rep.Request.Method, rep.Request.URL.String()),
		}
	}
	return nil
}

func (e HTTPError) Error() string {
	return fmt.Sprintf("%s: %s", e.Req, e.Status)
}

func (e HTTPError) Temporary() bool {
	return e.StatusCode >= 500
}

func (e HTTPError) Timeout() bool {
	return false
}
