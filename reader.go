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
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"unicode/utf8"

	"github.com/landjur/golibrary/uuid"
	"github.com/rcrowley/go-metrics"
	"github.com/spf13/pflag"
	"github.com/ugorji/go/codec"
	"gopkg.in/iconv.v1"

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

	// Charset to try to convert from when given invalid UTF-8.
	inputCharset = "ISO-8859-1"
)

func init() {
	AddBackgroundTask("Line reader", LineReader)
	AddMainTask("Record parser", RecordParser)

	pflag.StringVarP(&inputCharset, "input-charset", "c", inputCharset, "Expected charset for invalid UTF-8 input")
}

// LineReader reads lines from input on demand.
func LineReader() {
	var (
		converter  iconv.Iconv
		err        error
		convBuf    []byte
		convBuffer = bytes.Buffer{}
	)
	if converter, err = iconv.Open(inputCharset, "UTF-8//IGNORE"); err != nil {
		log.Fatalf("Cannot create converter for %s: %s", inputCharset, err)
	}
	buf := bufio.NewReader(reader)
	defer converter.Close()
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
				if !utf8.Valid(line) {
					convBuffer.Reset()
					if _, err := converter.DoWrite(&convBuffer, line, len(line), convBuf); err == nil {
						line = convBuffer.Bytes()
					} else {
						log.Warningf("Could not convert from %s to UTF-8: %s, input: %q", inputCharset, err, line)
					}
				}
				lines <- bytes.TrimRight(line[:l-1], " \t\n\r")
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
			var inRec data.InputRecord
			if err := dec.Decode(&inRec); err != nil {
				log.Errorf("Invalid JSON, %s: %q", err, buf)
				mInErrors.Mark(1)
				break
			}
			if inRec.Suffix == "" || len(inRec.Document) == 0 {
				log.Errorf("Malformed record: %q", buf)
				mInErrors.Mark(1)
				break
			}
			if inRec.ID == "" {
				if uuid, err := uuid.NewTimeBased(); err == nil {
					inRec.ID = base64.RawURLEncoding.EncodeToString(uuid[:])
				} else {
					log.Errorf("Could not generate an UUID: %s", err)
				}
			}
			mInRecords.Mark(1)
			queue.WriteC <- inRec.Record()
		case <-done:
			return
		case req <- true:
			req = nil
		}
	}
}
