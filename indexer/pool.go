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
	queue chan PoolEntry
	done  chan struct{}
}

type poolEntry struct {
	object interface{}
	owner  pool
}

func NewPool(objects []interface{}) Pool {
	q := make(chan PoolEntry, len(objects))
	p := pool{q, make(chan struct{})}
	for _, o := range objects {
		q <- poolEntry{o, p}
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
		go func() {
			select {
			case <-time.After(1 * time.Second):
				p.queue <- e
			case <-p.done:
			}
		}()
	} else {
		p.queue <- e
	}
}

func (e poolEntry) Object() interface{} {
	return e.object
}

func (e poolEntry) Release(failure bool) {
	e.owner.release(e, failure)
}
