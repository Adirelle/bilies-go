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
	"io"
	"sync"
)

var asyncWritePool = sync.Pool{New: makeBuffer}

func makeBuffer() interface{} {
	return make([]byte, 1024)
}

type asyncWriteCloser struct {
	c          chan []byte
	underlying io.WriteCloser
	wg         sync.WaitGroup
}

func newAsyncWriteCloser(w io.WriteCloser) io.WriteCloser {
	a := &asyncWriteCloser{c: make(chan []byte, 5), underlying: w}
	a.wg.Add(1)
	go a.process()
	return a
}

func (a *asyncWriteCloser) process() {
	defer func() {
		a.underlying.Close()
		a.wg.Done()
	}()
	for b := range a.c {
		a.underlying.Write(b)
		asyncWritePool.Put(b)
	}
}

func (a *asyncWriteCloser) Write(b []byte) (int, error) {
	l := len(b)
	b2 := asyncWritePool.Get().([]byte)
	if cap(b2) < l {
		b2 = make([]byte, l)
	} else {
		b2 = b2[:l]
	}
	copy(b2, b)
	a.c <- b2
	return len(b), nil
}

func (a *asyncWriteCloser) Close() error {
	close(a.c)
	a.wg.Wait()
	return nil
}
