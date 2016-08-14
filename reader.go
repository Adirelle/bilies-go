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
	"io"
	"os"

	"github.com/rcrowley/go-metrics"
	"github.com/ugorji/go/codec"

	"github.com/Adirelle/bilies-go/data"
)

var (
	mReader    = metrics.NewPrefixedChildRegistry(mRoot, "reader.")
	mInRecords = metrics.GetOrRegisterMeter("in.records", mReader)
	mInBytes   = metrics.GetOrRegisterMeter("in.bytes", mReader)
	mInErrors  = metrics.GetOrRegisterMeter("in.errors", mReader)

	reader = os.Stdin

	lines    = make(chan []byte)
	linesReq = make(chan bool)

	// This is used for the synchronisation with the batcher
	readerDone = make(chan bool)
)

func init() {
	AddBackgroundTask("Line reader", LineReader)
	AddMainTask("Record parser", RecordParser)
}

// LineReader reads lines from input on demand.
func LineReader() {
	buf := bufio.NewReader(reader)
	defer close(lines)
	for range linesReq {
		for {
			line, err := buf.ReadBytes('\n')
			if err == io.EOF {
				log.Notice("End of input reached")
				return
			} else if err != nil {
				log.Errorf("Cannot read input: %s", err)
				continue
			}
			l := len(line)
			mInBytes.Mark(int64(l))
			if l > 1 {
				lines <- line[:l-1]
				break
			}
		}
	}
}

// RecordParser requests Line from LineReader, converts them to InputRecords, and send them to the RecordQueuer.
func RecordParser() {
	dec := codec.NewDecoderBytes(nil, &codec.JsonHandle{})
	defer close(linesReq)
	defer close(readerDone)
	ever := true
	req := linesReq
	for ever {
		var buf []byte
		select {
		case buf, ever = <-lines:
			if buf == nil {
				break
			}
			req = linesReq

			dec.ResetBytes(buf)
			var rec data.Record
			if err := dec.Decode(&rec); err != nil {
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
			queue.WriteC <- rec
		case <-done:
			return
		case req <- true:
			req = nil
		}
	}
}
