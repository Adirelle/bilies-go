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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	converter = binary.BigEndian

	mQueue               = metrics.NewPrefixedChildRegistry(mRoot, "queue.")
	mQueueWrittenBytes   = metrics.GetOrRegisterMeter("write.bytes", mQueue)
	mQueueWrittenRecords = metrics.GetOrRegisterMeter("write.records", mQueue)
	mQueueReadBytes      = metrics.GetOrRegisterMeter("read.bytes", mQueue)
	mQueueReadRecords    = metrics.GetOrRegisterMeter("read.records", mQueue)
)

type Queue struct {
	WriteC chan<- InputRecord
	ReadC  <-chan InputRecord
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

	writeChan := make(chan InputRecord)
	readChan := make(chan InputRecord)
	dropChan := make(chan int)

	q := &Queue{
		db:     db,
		WriteC: writeChan,
		ReadC:  readChan,
		DropC:  dropChan,
		close:  make(chan bool),
	}

	started := &sync.WaitGroup{}
	started.Add(3)

	log.Debug("Starting queue")

	go q.processReads(readChan, started)
	go q.processWrites(writeChan, started)
	go q.processDrops(dropChan, started)

	started.Wait()
	log.Debug("Queue started")

	return q, nil
}

func (q *Queue) Close() {
	close(q.close)
	q.ended.Wait()
	q.db.Close()
}

func (q *Queue) processWrites(input chan InputRecord, started *sync.WaitGroup) {
	var (
		iter = q.db.NewIterator(nil, nil)
		buf  = bytes.Buffer{}

		lastID DbKey
	)

	if iter.First() {
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
			rec.Marshall(&buf)
			if err := q.db.Put(lastID.Bytes(), buf.Bytes(), nil); err == nil {
				mQueueWrittenBytes.Mark(int64(buf.Len()))
				mQueueWrittenRecords.Mark(1)
			}
			buf.Reset()
		case <-q.close:
			return
		}
	}
}

func (q *Queue) processReads(output chan InputRecord, started *sync.WaitGroup) {
	var (
		iter   iterator.Iterator
		lastID DbKey
		rec    InputRecord
		ch     chan InputRecord
		delay  <-chan time.Time
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
				rec.Unmarshall(iter.Value())
				mQueueReadBytes.Mark(int64(len(iter.Value())))
				mQueueReadRecords.Mark(1)
				ch = output
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
			var i int
			for found := iter.First(); i < n && found; found = iter.Next() {
				b.Delete(iter.Key())
				i++
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

// InputRecord defines the expected schema of input.
type InputRecord struct {
	Suffix   string          `json:"date"`
	Document json.RawMessage `json:"log"`
}

func (i InputRecord) String() string {
	return fmt.Sprintf("suffix=%s doc=%q", i.Suffix, i.Document)
}

func (i InputRecord) Marshall(w io.Writer) {
	var buf [4]byte
	converter.PutUint32(buf[:], uint32(len(i.Suffix)))
	w.Write(buf[:])
	w.Write([]byte(i.Suffix))
	w.Write(i.Document)
}

func (i *InputRecord) Unmarshall(buf []byte) {
	len := converter.Uint32(buf[0:4])
	i.Suffix = string(buf[4 : 4+len])
	i.Document = json.RawMessage(buf[4+len:])
}

type IteratorWrapper struct {
	underlying iterator.Iterator
	hadFirst   bool
}

func WrapIterator(i iterator.Iterator) IteratorWrapper {
	return IteratorWrapper{underlying: i}
}

func (i *IteratorWrapper) Next() (key []byte, value []byte, ok bool) {
	if i.hadFirst {
		ok = i.underlying.Next()
	} else {
		ok = i.underlying.First()
		i.hadFirst = ok
	}
	if ok {
		key = i.underlying.Key()
		value = i.underlying.Value()
	}
	log.Debugf("Has record: %t, key=%s, len=%d bytes", ok, FromBytes(key), len(value))
	return
}

func (i *IteratorWrapper) Release() {
	i.underlying.Release()
}
