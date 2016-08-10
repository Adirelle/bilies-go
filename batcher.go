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
	metrics "github.com/rcrowley/go-metrics"
	"github.com/spf13/pflag"
)

var (
	indexPrefix = "logs"
	docType     = "log"

	batchSize  = 500
	flushDelay = 1 * time.Second

	mBatcher      = metrics.NewPrefixedChildRegistry(mRoot, "batcher.")
	mPeekRecords  = metrics.NewRegisteredMeter("in.records", mBatcher)
	mPeekErrors   = metrics.NewRegisteredMeter("in.errors", mBatcher)
	mBatchRecords = metrics.NewRegisteredMeter("out.records", mBatcher)
	mBatchBytes   = metrics.NewRegisteredMeter("out.bytes", mBatcher)
	mBatchErrors  = metrics.NewRegisteredMeter("out.errors", mBatcher)
	mBatchSize    = metrics.NewRegisteredHistogram("batch.size", mBatcher, metrics.NewUniformSample(1e5))

	batchs = make(chan indexedBuffer)
)

func init() {
	pflag.StringVarP(&indexPrefix, "index", "i", indexPrefix, "Index prefix")
	pflag.StringVarP(&docType, "type", "t", docType, "Document type")
	pflag.IntVarP(&batchSize, "batch-size", "n", batchSize, "Maximum number of events in a batch")
	pflag.DurationVarP(&flushDelay, "flush-delay", "f", flushDelay, "Maximum delay between flushs")
}

func StartBatcher() {
	Start("Queue poller", QueuePoller)
	StartMain("Batch sender", BatchSender)
}

func QueuePoller() {
	var (
		now    = make(chan time.Time)
		buffer = indexedBuffer{}

		timeout <-chan time.Time
		rec     InputRecord
	)
	defer close(batchs)
	close(now)

	for {
		if queue.Length() > 0 {
			timeout = now
		} else {
			timeout = time.After(flushDelay)
		}
		select {
		case <-timeout:
		case <-readerDone:
			log.Notice("End of input reached and the queue is empty")
			return
		case <-done:
			return
		}

		log.Debugf("Flush tick, queue length=%d", queue.Length())
		buffer.Reset()
		for offset := uint64(0); buffer.Count() < batchSize && PeekRecord(offset, &rec); offset++ {
			_, err := fmt.Fprintf(&buffer, `{"index":{"_index":"log-%s","_type":"log"}}`+"\n%s\n", rec.Suffix, rec.Document)
			if err != nil {
				mBatchErrors.Mark(1)
				log.Errorf("Invalid record: %s", err)
			}
			buffer.Mark()
		}
		if buffer.Count() > 0 {
			log.Debugf("Sending batch, %d records, %d bytes", buffer.Count(), buffer.Len())
			batchs <- buffer
		}
	}
}

func PeekRecord(offset uint64, rec *InputRecord) (ok bool) {
	item, err := queue.PeekByOffset(offset)
	if err == goque.ErrEmpty || err == goque.ErrOutOfBounds {
		log.Debug("Queue is empty")
		return
	}
	if err != nil {
		mPeekErrors.Mark(1)
		log.Errorf("Could not peek: %s", err)
		return
	}
	err = item.ToObject(rec)
	if err != nil {
		mPeekErrors.Mark(1)
		log.Errorf("Could not unmarshal, %s: %q", err, item.Value)
		return
	}
	mPeekRecords.Mark(1)
	ok = true
	return
}

func BatchSender() {
	for buffer := range batchs {
		log.Debugf("%s", buffer.Bytes())
		for i, l := 0, buffer.Count(); i < l; i++ {
			queue.Dequeue()
		}
	}
}

/*

func (b *batcher) bulkIndex(i int, j int) (int, error) {
	if i == j {
		return 0, nil
	}
	log.Debugf("Sending slice [%d:%d]", i, j)
	err := b.sendRequest(b.buffer.Slice(i, j))
	if err == nil {
		return j - i, nil
	}
	if i+1 == j || !isBadRequest(err) {
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
	if rep != nil {
		defer rep.Close()
	}
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
}*/
