package indexer

import (
	"errors"
	"io"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func TestBatcher(t *testing.T) { TestingT(t) }

type BatcherTestSuite struct{}

var _ = Suite(&BatcherTestSuite{})

var (
	actionA = SimpleAction{"5", "idx", "tp", []byte("data_a")}
	actionB = SimpleAction{"6", "idx", "tp", []byte("data_b")}
	actionC = SimpleAction{"7", "idx", "tp", []byte("data_c")}
)

func (_ *BatcherTestSuite) TestBasicSend(c *C) {
	req := newFakeRequester(c, "{\"took\":1}")
	b := NewBatcher(req, 10*time.Second, 10, 10*1024)

	go func() {
		for r := range b.Results() {
			c.Check(r.Err, IsNil)
		}
	}()

	b.Send(actionA)
	b.Send(actionB)

	b.Stop()

	req.Check(c, 1)
	c.Check(b.Received(), Equals, uint64(2))
	c.Check(b.Sent(), Equals, uint64(2))
	c.Check(b.Errors(), Equals, uint64(0))
	c.Check(b.Pending(), Equals, 0)
}

func (_ *BatcherTestSuite) TestActionRefused(c *C) {
	req := newFakeRequester(c, "{\"took\":1,\"items\":[{\"index\":{\"_id\":\"5\",\"error\":\"Because !\"}}]}")
	b := NewBatcher(req, 10*time.Second, 10, 10*1024)

	go func() {
		for r := range b.Results() {
			switch r.Action.GetID() {
			case actionA.GetID():
				c.Check(r.Err, IsNil)
			case actionB.GetID():
				c.Check(r.Err, ErrorMatches, "Because !")
			default:
				c.Logf("Unexpected action: %#v", r.Action)
				c.Fail()
			}
		}
	}()

	b.Send(actionA)
	b.Send(actionB)

	b.Stop()

	req.Check(c, 1)
	c.Check(b.Received(), Equals, uint64(2))
	c.Check(b.Sent(), Equals, uint64(1))
	c.Check(b.Errors(), Equals, uint64(1))
	c.Check(b.Pending(), Equals, 0)
}

func (_ *BatcherTestSuite) TestGlobalError(c *C) {
	req := newFakeRequester(c, errors.New("Because !"))
	b := NewBatcher(req, 10*time.Second, 10, 10*1024)

	go func() {
		for r := range b.Results() {
			c.Check(r.Err, ErrorMatches, "Because !")
		}
	}()

	b.Send(actionA)
	b.Send(actionB)

	b.Stop()

	req.Check(c, 1)
	c.Check(b.Received(), Equals, uint64(2))
	c.Check(b.Sent(), Equals, uint64(0))
	c.Check(b.Errors(), Equals, uint64(2))
	c.Check(b.Pending(), Equals, 0)
}

func (_ *BatcherTestSuite) TestInvalidJsonResponse(c *C) {
	req := newFakeRequester(c, "garbage !")
	b := NewBatcher(req, 10*time.Second, 10, 10*1024)

	go func() {
		for r := range b.Results() {
			c.Check(r.Err, NotNil)
		}
	}()

	b.Send(actionA)
	b.Send(actionB)

	b.Stop()

	req.Check(c, 1)
	c.Check(b.Received(), Equals, uint64(2))
	c.Check(b.Sent(), Equals, uint64(0))
	c.Check(b.Errors(), Equals, uint64(2))
	c.Check(b.Pending(), Equals, 0)
}

func (_ *BatcherTestSuite) TestFlushCount(c *C) {
	req := newFakeRequester(c, "{\"took\":1}", "{\"took\":1}")
	b := NewBatcher(req, 10*time.Second, 1, 10*1024)

	go func() {
		for r := range b.Results() {
			c.Check(r.Err, IsNil)
		}
	}()

	b.Send(actionA)
	b.Send(actionB)

	b.Stop()

	req.Check(c, 2)
	c.Check(b.Received(), Equals, uint64(2))
	c.Check(b.Sent(), Equals, uint64(2))
	c.Check(b.Errors(), Equals, uint64(0))
	c.Check(b.Pending(), Equals, 0)
}

func (_ *BatcherTestSuite) TestFlushDelay(c *C) {
	req := newFakeRequester(c, "{\"took\":1}", "{\"took\":1}")
	b := NewBatcher(req, 1*time.Second, 10, 10*1024)

	as := []Action{actionA, actionB}
	rs := b.Results()
	for _, a := range as {
		c.Log("sending action")
		b.Send(a)
		c.Log("waiting for result")
		r := <-rs
		c.Log("got result")
		c.Check(r.Err, IsNil)
	}

	c.Log("Closing")
	b.Stop()

	req.Check(c, 2)
	c.Check(b.Received(), Equals, uint64(2))
	c.Check(b.Sent(), Equals, uint64(2))
	c.Check(b.Errors(), Equals, uint64(0))
	c.Check(b.Pending(), Equals, 0)
}

// Requester mock

type testingRequester struct {
	replies      []interface{}
	requestCount int
	c            *C
}

func newFakeRequester(c *C, replies ...interface{}) *testingRequester {
	c.Log("New requester")
	return &testingRequester{replies, 0, c}
}

func (r *testingRequester) Send(body io.Reader) (io.ReadCloser, error) {
	r.requestCount++
	r.c.Logf("Request #%d sent", r.requestCount)
	r.c.Assert(len(r.replies) > 0, Equals, true) // More requests than expected
	head := r.replies[0]
	r.replies = r.replies[1:]
	switch value := head.(type) {
	case string:
		r.c.Logf("Replying with string: %q", value)
		return readerFrom(value), nil
	case error:
		r.c.Logf("Replying with error: %s", value)
		return nil, value
	default:
		r.c.Logf("Unexpected reply %T: %#v", head, head)
		r.c.FailNow()
		return nil, nil
	}
}

func (r *testingRequester) Check(c *C, n int) {
	c.Check(len(r.replies), Equals, 0) // All replies must be used
	c.Check(r.requestCount, Equals, n) // The actual number of requests must match the expected one
}
