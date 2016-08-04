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
package indexer

import (
	"time"
)

type Pool interface {
	Acquire() (PoolEntry, bool)
	Close()
}

type PoolEntry interface {
	Object() interface{}
	Release(bool)
}

type pool struct {
	queue     chan PoolEntry
	done      chan struct{}
	baseDelay time.Duration
	maxDelay  time.Duration
}

type poolEntry struct {
	object interface{}
	owner  pool
	delay  time.Duration
}

func NewPool(objects []interface{}) Pool {
	q := make(chan PoolEntry, len(objects))
	p := pool{q, make(chan struct{}), 1 * time.Second, 2 * time.Minute}
	for _, o := range objects {
		q <- poolEntry{o, p, p.baseDelay}
	}
	return p
}

func (p pool) Close() {
	close(p.done)
	close(p.queue)
}

func (p pool) Acquire() (PoolEntry, bool) {
	select {
	case e := <-p.queue:
		return e, e != nil
	case <-p.done:
		return nil, false
	}
}

func (p pool) release(e poolEntry, failure bool) {
	if failure {
		log.Infof("Ignoring %q for %s", e.object, e.delay)
		go func() {
			select {
			case <-time.After(e.delay):
				e.delay *= 2
				if e.delay > p.maxDelay {
					e.delay = p.maxDelay
				}
				p.queue <- e
			case <-p.done:
			}
		}()
	} else {
		e.delay = p.baseDelay
		p.queue <- e
	}
}

func (e poolEntry) Object() interface{} {
	return e.object
}

func (e poolEntry) Release(failure bool) {
	e.owner.release(e, failure)
}
