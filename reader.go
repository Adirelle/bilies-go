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
	"bufio"
	"encoding/json"
	"io"
	"sync"

	"github.com/beeker1121/goque"
	"github.com/rcrowley/go-metrics"
)

type inputRecord struct {
	Suffix   string          `json:"date"`
	Document json.RawMessage `json:"log"`
}

type reader struct {
	reader io.ReadCloser
	queue  *goque.Queue

	running bool
	read    chan bool
	stop    chan bool
	line    []byte
	mu      sync.RWMutex

	mInRecords  metrics.Meter
	mInBytes    metrics.Meter
	mInErrors   metrics.Meter
	mOutRecords metrics.Meter
	mOutBytes   metrics.Meter
	mOutErrors  metrics.Meter
}

func newReader(r io.ReadCloser, q *goque.Queue, m metrics.Registry) service {
	return &reader{
		reader:      r,
		queue:       q,
		mInRecords:  metrics.GetOrRegisterMeter("in.records", m),
		mInBytes:    metrics.GetOrRegisterMeter("in.bytes", m),
		mInErrors:   metrics.GetOrRegisterMeter("in.errors", m),
		mOutRecords: metrics.GetOrRegisterMeter("out.records", m),
		mOutErrors:  metrics.GetOrRegisterMeter("out.errors", m),
	}
}

func (r *reader) Init() {
	r.read = make(chan bool)
	r.stop = make(chan bool)
	r.setRunning(true)
	go func() {
		defer r.reader.Close()
		buf := bufio.NewReader(r.reader)
		var err error
		for range r.read {
			r.line, err = buf.ReadBytes('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Warningf("Cannot read input: %s", err)
			}
		}
		r.Interrupt()
	}()
}

func (r *reader) setRunning(b bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running != b {
		r.running = b
		return true
	}
	return false
}

func (r *reader) Continue() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.running
}

func (r *reader) Interrupt() {
	if r.setRunning(false) {
		close(r.stop)
	}
}

func (r *reader) Iterate() {
	var (
		rec inputRecord
		buf []byte
	)
	select {
	case r.read <- true:
		buf = r.line
		if buf == nil {
			return
		}
	case <-r.stop:
		log.Debug("stop chan closed")
		return
	}
	r.mInRecords.Mark(1)
	r.mInBytes.Mark(int64(len(buf)))
	err := json.Unmarshal(buf, &rec)
	if err != nil {
		log.Warningf("Invalid input, %s: %q", err, buf)
		r.mInErrors.Mark(1)
		return
	}
	if rec.Suffix == "" || rec.Document == nil {
		log.Warningf("Invalid input, %s: %q", err, buf)
		r.mInErrors.Mark(1)
		return
	}
	_, err = r.queue.EnqueueObject(rec)
	if err != nil {
		log.Warningf("Could not enqueue: %s", err)
		r.mOutErrors.Mark(1)
		return
	}
	r.mOutRecords.Mark(1)
}

func (r *reader) Cleanup() {
	close(r.read)
}

func (r *reader) String() string {
	return "reader"
}
