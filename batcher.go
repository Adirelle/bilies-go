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
	"fmt"
	"time"

	"github.com/beeker1121/goque"
)

type batcher struct {
	queue      *goque.Queue
	flushDelay time.Duration
	batchSize  uint64
	readerSpv  supervisor
	buffer     *indexedBuffer
	ticker     *time.Ticker
}

func newBatcher(q *goque.Queue, d time.Duration, s int, r supervisor) service {
	return &batcher{queue: q, flushDelay: d, batchSize: uint64(s), readerSpv: r}
}

func (b *batcher) Init() {
	b.buffer = &indexedBuffer{}
	b.ticker = time.NewTicker(b.flushDelay)
}

func (b *batcher) Continue() bool {
	return b.readerSpv.IsRunning() || b.queue.Length() > 0
}

func (b *batcher) Iterate() {
	len := b.queue.Length()
	if len == 0 {
		log.Debug("sleeping")
		<-b.ticker.C
		return
	}
	if len > b.batchSize {
		len = b.batchSize
	}

	var (
		rec inputRecord
		i   uint64
	)
	for i = 0; i < len; i++ {
		item, err := b.queue.PeekByOffset(uint64(i))
		if err == goque.ErrEmpty || err == goque.ErrOutOfBounds {
			break
		}
		if err != nil {
			log.Debugf("Could not peek at %d: %s (%#v)", i, err, err)
			break
		}
		err = item.ToObject(&rec)
		if err != nil {
			log.Debugf("Could not unmarshal, %s: %q", err, item.Value)
			continue
		}
		_, err = fmt.Fprintf(b.buffer, `{"index":{"_index":"log-%s","_type":"log"}}`+"\n%s\n", rec.Suffix, rec.Document)
		if err != nil {
			log.Debugf("Could convert record: %s", err)
			continue
		}
		b.buffer.Mark()
	}

	rdy := b.buffer.Count()
	if rdy == 0 {
		return
	}

	log.Debugf("buffer len=%d count=%d", b.buffer.Len(), rdy)
	sent, err := b.bulkIndex(0, rdy)
	log.Debugf("Submit result: %d, %s", sent, err)
	for j := 0; j < sent; j++ {
		b.queue.Dequeue()
	}
	b.buffer.Reset()
}

func (b *batcher) Cleanup() {
	b.buffer.Reset()
	b.ticker.Stop()
}

func (b *batcher) String() string {
	return "batcher"
}

func (b *batcher) bulkIndex(i int, j int) (int, error) {
	if i == j {
		return 0, nil
	}
	err := b.sendRequest(b.buffer.Slice(i, j))
	if err == nil {
		return j - i, nil
	}
	if i+1 == j {
		return 0, err
	}
	h := (i + j) / 2
	n, err := b.bulkIndex(i, h)
	if err != nil {
		return n, err
	}
	n2, err := b.bulkIndex(h, j)
	return n + n2, err
}

func (b *batcher) sendRequest(buf []byte) (err error) {
	log.Infof("Sent %d bytes", len(buf))
	return
}
