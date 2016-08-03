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
	"testing"

	. "gopkg.in/check.v1"
)

func TestPool(t *testing.T) { TestingT(t) }

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
