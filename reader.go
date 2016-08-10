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
	"os"

	"github.com/rcrowley/go-metrics"
)

type InputRecord struct {
	Suffix   string          `json:"date"`
	Document json.RawMessage `json:"log"`
}

var (
	mReader        = metrics.NewPrefixedChildRegistry(mRoot, "reader.")
	mInRecords     = metrics.GetOrRegisterMeter("in.records", mReader)
	mInBytes       = metrics.GetOrRegisterMeter("in.bytes", mReader)
	mInErrors      = metrics.GetOrRegisterMeter("in.errors", mReader)
	mQueuedRecords = metrics.GetOrRegisterMeter("out.records", mReader)
	mQueuedBytes   = metrics.GetOrRegisterMeter("out.bytes", mReader)
	mQueuingErrors = metrics.GetOrRegisterMeter("out.errors", mReader)

	reader = os.Stdin

	lines     = make(chan []byte)
	linesReq  = make(chan bool)
	recordsIn = make(chan *InputRecord)
)

func LineReader() {
	buf := bufio.NewReader(reader)
	ever := true
	defer close(lines)
	for ever {
		select {
		case <-done:
			return
		case _, ever = <-linesReq:
			line, err := buf.ReadBytes('\n')
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Errorf("Cannot read input: %s", err)
			}
			lines <- line
		}
	}
}

func RecordParser() {
	defer close(recordsIn)
	defer close(linesReq)
	ever := true
	req := linesReq
	for ever {
		var buf []byte
		select {
		case <-done:
			return
		case buf, ever = <-lines:
			if buf == nil {
				break
			}
			log.Debug("Requesting new line")
			req = linesReq
			mInBytes.Mark(int64(len(buf)))
			var rec InputRecord
			err := json.Unmarshal(buf, &rec)
			if err != nil {
				log.Errorf("Invalid JSON, %s: %q", err, buf)
				mInErrors.Mark(1)
				break
			}
			if rec.Suffix == "" || rec.Document == nil {
				log.Errorf("Malformed record: %q", buf)
				mInErrors.Mark(1)
				break
			}
			mInRecords.Mark(1)
			log.Debug("New record read")
			recordsIn <- &rec
		case req <- true:
			log.Debug("Lines requested")
			req = nil
		}
	}
}

func RecordQueuer() {
	ever := true
	for ever {
		var rec *InputRecord
		select {
		case <-done:
			return
		case rec, ever = <-recordsIn:
			if rec == nil {
				break
			}
			if _, err := queue.EnqueueObject(*rec); err == nil {
				mQueuedRecords.Mark(1)
			} else {
				log.Errorf("Could not enqueue: %s", err)
				mQueuingErrors.Mark(1)
			}
		}
	}
}

func StartReader() {
	StartAndForget("Line reader", LineReader)
	Start("Record parser", RecordParser)
	Start("Record queuer", RecordQueuer)
}
