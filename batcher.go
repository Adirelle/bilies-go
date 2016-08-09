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
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beeker1121/goque"
	metrics "github.com/rcrowley/go-metrics"
)

type batcher struct {
	queue     *goque.Queue
	requester *requester

	flushDelay time.Duration
	batchSize  uint64
	readerSpv  supervisor

	buffer *indexedBuffer
	ticker *time.Ticker

	mInRecords  metrics.Meter
	mInErrors   metrics.Meter
	mOutRecords metrics.Meter
	mOutBytes   metrics.Meter
	mOutErrors  metrics.Meter
	mBatchSize  metrics.Sample
}

func newBatcher(q *goque.Queue, req *requester, d time.Duration, s int, r supervisor, mp metrics.Registry) service {
	m := metrics.NewPrefixedChildRegistry(mp, "batcher.")
	b := &batcher{
		queue:     q,
		requester: req,

		flushDelay: d,
		batchSize:  uint64(s),
		readerSpv:  r,

		mInRecords:  metrics.NewRegisteredMeter("in.records", m),
		mInErrors:   metrics.NewRegisteredMeter("in.errors", m),
		mOutRecords: metrics.NewRegisteredMeter("out.records", m),
		mOutBytes:   metrics.NewRegisteredMeter("out.bytes", m),
		mOutErrors:  metrics.NewRegisteredMeter("out.errors", m),
		mBatchSize:  metrics.NewUniformSample(1e5),
	}
	metrics.NewRegisteredHistogram("batch.size", m, b.mBatchSize)
	return b
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
			b.mInErrors.Mark(1)
			log.Debugf("Could not peek at %d: %s (%#v)", i, err, err)
			break
		}
		err = item.ToObject(&rec)
		if err != nil {
			b.mInErrors.Mark(1)
			log.Debugf("Could not unmarshal, %s: %q", err, item.Value)
			continue
		}
		_, err = fmt.Fprintf(b.buffer, `{"index":{"_index":"log-%s","_type":"log"}}`+"\n%s\n", rec.Suffix, rec.Document)
		if err != nil {
			b.mInErrors.Mark(1)
			log.Debugf("Could convert record: %s", err)
			continue
		}
		b.mInRecords.Mark(1)
		b.buffer.Mark()
	}

	rdy := b.buffer.Count()
	if rdy == 0 {
		return
	}
	b.mBatchSize.Update(int64(rdy))

	log.Debugf("buffer len=%d count=%d", b.buffer.Len(), rdy)
	sent, err := b.bulkIndex(0, rdy)
	if err == nil {
		b.mOutBytes.Mark(int64(b.buffer.Len()))
		b.mOutRecords.Mark(int64(rdy))
		for j := 0; j < sent; j++ {
			b.queue.Dequeue()
		}
	} else {
		b.mOutErrors.Mark(1)
		log.Debugf("Could not submit: %s", err)
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

type bulkResponse struct {
	Took   int                `json:"took"`
	Err    *bulkError         `json:"error"`
	Items  []bulkItemResponse `json:"items"`
	Status int                `json:"status"`
}

type bulkItemResponse struct {
	Op bulkOpResponse `json:"create"`
}

type bulkOpResponse struct {
	Status int             `json:"status"`
	Data   json.RawMessage `json:"data"`
	Err    *bulkError      `json:"error"`
}

type bulkError struct {
	Reason string     `json:"reason"`
	Cause  *bulkError `json:"caused_by"`
}

func (e *bulkError) Error() string {
	if e == nil {
		return "No error"
	}
	if e.Cause == nil {
		return e.Reason
	}
	return fmt.Sprintf("%s, cause by: %s", e.Reason, e.Cause)
}

func (b *batcher) sendRequest(buf []byte) (err error) {
	rep, err := b.requester.send(bytes.NewReader(buf))
	defer rep.Close()
	if err != nil {
		log.Warningf("Request failed: %s", err)
		return
	}
	dec := json.NewDecoder(rep)
	var resp bulkResponse
	err = dec.Decode(&resp)
	if err != nil {
		log.Warningf("Error decoding response: %s", err)
		return
	}
	for _, r := range resp.Items {
		if r.Op.Err != nil {
			log.Warningf("Record error: %s", r.Op.Err)
		}
	}
	return
}
