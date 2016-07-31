package indexer

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type PoolTestSuite struct{}

var _ = Suite(&PoolTestSuite{})

func (_ *PoolTestSuite) TestRoundRobin(c *C) {
	p := NewPool([]interface{}{"a", "b"})

	a, ok := p.Acquire()
	c.Check(ok, Equals, true)
	c.Check(a.Object(), Equals, "a")
	a.Release(false)

	b, ok := p.Acquire()
	c.Check(ok, Equals, true)
	c.Check(b.Object(), Equals, "b")
	b.Release(false)

	p.Close()
}

func (_ *PoolTestSuite) TestFailure(c *C) {
	p := NewPool([]interface{}{"a", "b"})

	a, ok := p.Acquire()
	c.Check(ok, Equals, true)
	c.Check(a.Object(), Equals, "a")
	a.Release(true)

	b, ok := p.Acquire()
	c.Check(ok, Equals, true)
	c.Check(b.Object(), Equals, "b")
	b.Release(false)

	d, ok := p.Acquire()
	c.Check(ok, Equals, true)
	c.Check(d.Object(), Equals, "b")
	d.Release(false)

	p.Close()
}

func (_ *PoolTestSuite) TestWaitForResource(c *C) {
	p := NewPool([]interface{}{"a"})

	a, ok := p.Acquire()
	c.Check(ok, Equals, true)
	c.Check(a.Object(), Equals, "a")

	done := make(chan bool)

	go func() {
		b, ok := p.Acquire()
		c.Check(ok, Equals, true)
		c.Check(b.Object(), Equals, "a")
		b.Release(false)
		done <- true
	}()

	a.Release(false)

	v := <-done
	c.Check(v, Equals, true)

	p.Close()
}

func (_ *PoolTestSuite) TestClose(c *C) {
	p := NewPool([]interface{}{"a"})

	a, ok := p.Acquire()
	c.Check(ok, Equals, true)
	c.Check(a.Object(), Equals, "a")

	done := make(chan bool)

	go func() {
		_, ok := p.Acquire()
		done <- ok
	}()

	p.Close()

	v := <-done
	c.Check(v, Equals, false)
}
