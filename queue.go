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

/*
Messages queueing

Incoming messages are enqueued into LevelDB database.

The following switch control queueing:

	-q --queue-dir=STRING [default: $PWD/.queue]
		Sets the path to the directoy hosting the LevelDB.
*/
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/ugorji/go/codec"

	"github.com/Adirelle/bilies-go/data"
)

var (
	converter = binary.BigEndian

	mQueue               = metrics.NewPrefixedChildRegistry(mRoot, "queue.")
	mQueueWrittenBytes   = metrics.GetOrRegisterMeter("write.bytes", mQueue)
	mQueueWrittenRecords = metrics.GetOrRegisterMeter("write.records", mQueue)
	mQueueReadBytes      = metrics.GetOrRegisterMeter("read.bytes", mQueue)
	mQueueReadRecords    = metrics.GetOrRegisterMeter("read.records", mQueue)
	mQueueLastWrittenID  = metrics.GetOrRegisterGauge("lastID.written", mQueue)
	mQueueLastReadID     = metrics.GetOrRegisterGauge("lastID.read", mQueue)
	mQueueLastDeletedID  = metrics.GetOrRegisterGauge("lastID.deleted", mQueue)
	mQueueLength         = metrics.GetOrRegisterHistogram("length.records", mQueue, metrics.NewUniformSample(1e6))
	mQueuePending        = metrics.GetOrRegisterHistogram("pending.records", mQueue, metrics.NewUniformSample(1e6))

	queueCodecHandle = &codec.SimpleHandle{}
)

type Queue struct {
	WriteC chan<- data.Record
	ReadC  <-chan data.Record
	DropC  chan<- int

	db    *leveldb.DB
	close chan bool
	ended sync.WaitGroup
}

func OpenQueue(path string) (*Queue, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	writeChan := make(chan data.Record)
	readChan := make(chan data.Record)
	dropChan := make(chan int)

	q := &Queue{
		db:     db,
		WriteC: writeChan,
		ReadC:  readChan,
		DropC:  dropChan,
		close:  make(chan bool),
	}

	started := &sync.WaitGroup{}
	started.Add(4)

	log.Debug("Starting queue")

	go q.processReads(readChan, started)
	go q.processWrites(writeChan, started)
	go q.processDrops(dropChan, started)
	go q.updateMetrics(started)

	started.Wait()
	log.Debug("Queue started")

	return q, nil
}

func (q *Queue) Close() {
	close(q.close)
	q.ended.Wait()
	q.db.Close()
}

func (q *Queue) processWrites(input chan data.Record, started *sync.WaitGroup) {
	var (
		iter = q.db.NewIterator(nil, nil)

		lastID DbKey

		buf bytes.Buffer
		enc = codec.NewEncoder(&buf, queueCodecHandle)
	)

	if iter.Last() {
		lastID = FromBytes(iter.Key())
	}
	iter.Release()

	q.ended.Add(1)
	defer q.ended.Done()
	started.Done()

	for {
		select {
		case rec := <-input:
			lastID++
			buf.Reset()
			enc.Reset(&buf)
			if err := enc.Encode(&rec); err != nil {
				log.Errorf("Could not marshall record: %s", err)
				break
			}
			if err := q.db.Put(lastID.Bytes(), buf.Bytes(), nil); err == nil {
				mQueueWrittenBytes.Mark(int64(buf.Len()))
				mQueueWrittenRecords.Mark(1)
				mQueueLastWrittenID.Update(int64(lastID))
			} else {
				log.Errorf("Could not write record to queue: %s", err)
			}
		case <-q.close:
			return
		}
	}
}

func (q *Queue) processReads(output chan data.Record, started *sync.WaitGroup) {
	var (
		iter   iterator.Iterator
		lastID DbKey
		rec    data.Record

		ch    chan data.Record
		delay <-chan time.Time

		dec = codec.NewDecoderBytes(nil, queueCodecHandle)
	)

	q.ended.Add(1)
	defer q.ended.Done()
	started.Done()

	for {
		if ch == nil {
			if iter == nil {
				iter = q.db.NewIterator(&util.Range{Start: lastID.Bytes()}, nil)
				iter.First()
			}
			if iter.Next() {
				lastID = FromBytes(iter.Key())
				dec.ResetBytes(iter.Value())
				if err := dec.Decode(&rec); err != nil {
					log.Errorf("Could not unmarshall record: %s", err)
				} else {
					mQueueReadBytes.Mark(int64(len(iter.Value())))
					mQueueReadRecords.Mark(1)
					mQueueLastReadID.Update(int64(lastID))
					ch = output
				}
			} else {
				iter.Release()
				iter = nil
				delay = time.After(flushDelay)
			}
		}
		select {
		case ch <- rec:
			ch = nil
		case <-delay:
			delay = nil
		case <-q.close:
			return
		}
	}
}

func (q *Queue) processDrops(input chan int, started *sync.WaitGroup) {
	q.ended.Add(1)
	defer q.ended.Done()
	started.Done()

	for {
		select {
		case n := <-input:
			iter := q.db.NewIterator(nil, nil)
			b := leveldb.Batch{}
			var (
				i      int
				lastID DbKey
			)
			for found := iter.First(); i < n && found; found = iter.Next() {
				b.Delete(iter.Key())
				lastID = FromBytes(iter.Key())
				i++
			}
			if lastID > 0 {
				mQueueLastDeletedID.Update(int64(lastID))
			}
			if err := q.db.Write(&b, nil); err == nil {
				log.Debugf("Removed %d/%d records", i, n)
			} else {
				log.Debugf("Error removing record %d records: %s", n, err)
			}
			iter.Release()
		case <-q.close:
			return
		}
	}
}

func (q *Queue) updateMetrics(started *sync.WaitGroup) {
	q.ended.Add(1)
	defer q.ended.Done()
	started.Done()

	iter := q.db.NewIterator(nil, nil)
	if iter.First() {
		firstID := int64(FromBytes(iter.Key()))
		mQueueLastReadID.Update(firstID)
		mQueueLastDeletedID.Update(firstID)
	}
	if iter.Last() {
		mQueueLastWrittenID.Update(int64(FromBytes(iter.Key())))
	}
	iter.Release()

	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			lastWritten := mQueueLastWrittenID.Value()
			lastRead := mQueueLastReadID.Value()
			lastDeleted := mQueueLastDeletedID.Value()
			if lastDeleted > 0 {
				if lastWritten >= lastDeleted {
					mQueueLength.Update(lastWritten - lastDeleted)
				}
				if lastRead >= lastDeleted {
					mQueuePending.Update(lastRead - lastDeleted)
				}
			}
		case <-q.close:
			return
		}
	}
}

type DbKey uint64

func FromBytes(b []byte) DbKey {
	if len(b) != 8 || b == nil {
		return DbKey(0)
	}
	return DbKey(converter.Uint64(b))
}

func (k DbKey) Bytes() []byte {
	if k == 0 {
		return nil
	}
	b := make([]byte, 8)
	converter.PutUint64(b, uint64(k))
	return b
}

func (k DbKey) String() string {
	if k == 0 {
		return "<nil>"
	}
	return fmt.Sprintf("#%08x", uint64(k))
}
