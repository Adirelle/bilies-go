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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	queueBytesPool = sync.Pool{New: makeBytes}

	converter = binary.BigEndian

	mQueue               = metrics.NewPrefixedChildRegistry(mRoot, "queue.")
	mQueueWrittenBytes   = metrics.GetOrRegisterMeter("write.bytes", mQueue)
	mQueueWrittenRecords = metrics.GetOrRegisterMeter("write.records", mQueue)
	mQueueReadBytes      = metrics.GetOrRegisterMeter("read.bytes", mQueue)
	mQueueReadRecords    = metrics.GetOrRegisterMeter("read.records", mQueue)
	mQueueLen            = metrics.GetOrRegisterHistogram("size", mQueue, metrics.NewUniformSample(1e5))
	mQueuePending        = metrics.GetOrRegisterHistogram("pending", mQueue, metrics.NewUniformSample(1e5))
)

type Queue struct {
	db *leveldb.DB

	WriteC chan<- InputRecord
	ReadC  <-chan InputRecord
	DropC  chan<- int

	sync chan bool
}

// InputRecord defines the expected schema of input.
type InputRecord struct {
	Suffix   string          `json:"date"`
	Document json.RawMessage `json:"log"`
}

func OpenQueue(path string) (*Queue, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	q := &Queue{db: db, sync: make(chan bool)}
	go q.process()
	<-q.sync
	return q, nil
}

func (q *Queue) Close() {
	q.sync <- false
	<-q.sync
}

func (q *Queue) process() {
	var (
		writeChan = make(chan InputRecord)
		readChan  = make(chan InputRecord)
		dropChan  = make(chan int)

		ticker = time.NewTicker(1 * time.Second)

		outRec  InputRecord
		outChan chan InputRecord

		writeID, readID, dropID DbKey
	)

	defer q.db.Close()

	iter := q.db.NewIterator(nil, nil)
	if iter.First() {
		readID = FromBytes(iter.Key()) - 1
	}
	dropID = readID
	if iter.Last() {
		writeID = FromBytes(iter.Key())
	}
	iter.Release()

	q.ReadC = readChan
	q.WriteC = writeChan
	q.DropC = dropChan

	defer ticker.Stop()
	defer close(readChan)
	defer close(q.sync)

	q.sync <- true

	for {
		select {
		case inRec := <-writeChan:
			writeID++
			q.write(writeID, inRec)
		case outChan <- outRec:
			outChan = nil
		case num := <-dropChan:
			next := dropID + DbKey(num)
			if next > readID {
				next = readID
			}
			if err := q.drop(dropID, next); err == nil {
				dropID = next
				log.Debugf("Unqueueq %d records", num)
			} else {
				log.Errorf("Could not unqueue %d records: %s", num, err)
			}
		case <-q.sync:
			log.Debug("Bailing out")
			return
		case <-ticker.C:
			mQueueLen.Update(int64(writeID - dropID))
			mQueuePending.Update(int64(readID - dropID))
		}
		if outChan == nil && readID < writeID {
			readID++
			if err := q.read(readID, &outRec); err == nil {
				outChan = readChan
			} else {
				log.Errorf("Cannot read from queue: %s", err)
			}
		}
	}
}

func (q *Queue) write(id DbKey, rec InputRecord) (err error) {
	buf := queueBytesPool.Get().([]byte)[:4]
	defer queueBytesPool.Put(buf)

	suffix := []byte(rec.Suffix)
	doc := []byte(rec.Document)
	converter.PutUint32(buf[0:4], uint32(len(suffix)))
	buf = append(buf, suffix...)
	buf = append(buf, doc...)

	if err = q.db.Put(id.Bytes(), buf, nil); err == nil {
		mQueueWrittenBytes.Mark(int64(len(buf)))
		mQueueWrittenRecords.Mark(1)
	}

	return
}

func (q *Queue) read(id DbKey, rec *InputRecord) (err error) {
	buf, err := q.db.Get(id.Bytes(), nil)
	if err != nil {
		return err
	}
	mQueueReadBytes.Mark(int64(len(buf)))
	mQueueReadRecords.Mark(1)

	len := converter.Uint32(buf[0:4])
	rec.Suffix = string(buf[4 : 4+len])
	rec.Document = json.RawMessage(buf[4+len:])
	return nil
}

func (q *Queue) drop(from DbKey, to DbKey) (err error) {
	b := &leveldb.Batch{}
	for id := from; id < to; id++ {
		b.Delete(id.Bytes())
	}
	return q.db.Write(b, nil)
}

func makeBytes() interface{} {
	return make([]byte, 0, 1024)
}

type DbKey uint64

func FromBytes(b []byte) DbKey {
	return DbKey(converter.Uint64(b))
}

func (k DbKey) Bytes() []byte {
	b := make([]byte, 8)
	converter.PutUint64(b, uint64(k))
	return b
}

func (k DbKey) String() string {
	return fmt.Sprintf("#%08x", uint64(k))
}
