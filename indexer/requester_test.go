package indexer

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"

	. "gopkg.in/check.v1"
)

func TestRequester(t *testing.T) { TestingT(t) }

type RequesterSuite struct{}

var _ = Suite(&RequesterSuite{})

func (s *RequesterSuite) TestSendAllOk(c *C) {
	r := setupRequester(func(r *http.Request, n int) (*http.Response, error) {
		c.Check(n, Equals, 1) // Succeed on first response
		c.Check(r.Method, Equals, "POST")
		c.Check(r.URL.String(), Equals, "bla://a:9200/_bulk")

		u, p, ok := r.BasicAuth()
		c.Check(ok, Equals, true)
		c.Check(u, Equals, "bob")
		c.Check(p, Equals, "ricard")

		b, err := ioutil.ReadAll(r.Body)
		c.Check(err, IsNil)
		c.Check(string(b), Equals, "data")

		return response(r, 200), nil
	})

	resp, err := r.Send(readerFrom("data"))
	c.Assert(err, IsNil)

	data, err := ioutil.ReadAll(resp)
	c.Assert(err, IsNil)
	c.Check(string(data), Equals, "reply")
}

func (s *RequesterSuite) TestSendPermanentNetError(c *C) {
	r := setupRequester(func(r *http.Request, n int) (*http.Response, error) {
		c.Check(n, Equals, 1) // Fail on first permanent error
		return nil, &net.DNSError{Err: "failed !"}
	})

	_, err := r.Send(readerFrom("data"))
	c.Check(err, ErrorMatches, ".*failed !")
}

func (s *RequesterSuite) TestSendTemporaryNetError(c *C) {
	r := setupRequester(func(r *http.Request, n int) (*http.Response, error) {
		if n < 3 {
			return nil, &net.DNSError{IsTemporary: true}
		}
		c.Check(n, Equals, 3) // Succeed on third requist
		return response(r, 200), nil
	})

	_, err := r.Send(readerFrom("data"))
	c.Check(err, IsNil)
}

func (s *RequesterSuite) TestSendStatus4xxError(c *C) {
	r := setupRequester(func(r *http.Request, n int) (*http.Response, error) {
		c.Check(n, Equals, 1) // Fail on first 400 response
		return response(r, 400), nil
	})

	_, err := r.Send(readerFrom("data"))
	c.Check(err, ErrorMatches, ".*400 status")
}

func (s *RequesterSuite) TestSendStatus5xxError(c *C) {
	r := setupRequester(func(r *http.Request, n int) (*http.Response, error) {
		if n < 3 {
			return response(r, 500), nil
		}
		c.Check(n, Equals, 3) // Succeed on third requist
		return response(r, 200), nil
	})

	_, err := r.Send(readerFrom("data"))
	c.Check(err, IsNil)
}

// Setup helper

func setupRequester(roundTrip RoundTripFunc) Requester {
	trn := testingTransport{roundTrip, new(int)}
	clt := http.Client{Transport: trn}
	return NewRequester(clt, []string{"a", "b"}, 9200, "bla", "bob", "ricard")
}

func response(r *http.Request, s int) *http.Response {
	return &http.Response{
		StatusCode: s,
		Request:    r,
		Status:     fmt.Sprintf("%d status", s),
		Body:       readerFrom("reply"),
	}
}

// RoundTripper mock

type RoundTripFunc func(*http.Request, int) (*http.Response, error)

type testingTransport struct {
	roundTrip    RoundTripFunc
	requestCount *int
}

func (t testingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	(*t.requestCount)++
	return t.roundTrip(r, *t.requestCount)
}
