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

	metrics "github.com/rcrowley/go-metrics"
	"github.com/spf13/pflag"
)

var (
	indexPrefix = "logs"
	docType     = "log"

	batchSize  = 500
	flushDelay = 1 * time.Second

	mBatcher      = metrics.NewPrefixedChildRegistry(mRoot, "batcher.")
	mBatchRecords = metrics.NewRegisteredMeter("records", mBatcher)
	mBatchBytes   = metrics.NewRegisteredMeter("bytes", mBatcher)
	mBatchErrors  = metrics.NewRegisteredMeter("errors", mBatcher)
	mBatchSize    = metrics.NewRegisteredHistogram("size", mBatcher, metrics.NewUniformSample(1e5))

	batchs = make(chan indexedBuffer)
)

func init() {
	pflag.StringVarP(&indexPrefix, "index", "i", indexPrefix, "Index prefix")
	pflag.StringVarP(&docType, "type", "t", docType, "Document type")
	pflag.IntVarP(&batchSize, "batch-size", "n", batchSize, "Maximum number of events in a batch")
	pflag.DurationVarP(&flushDelay, "flush-delay", "f", flushDelay, "Maximum delay between flushs")

	AddMainTask("Batcher", Batcher)
}

func Batcher() {
	defer close(batchs)

	var (
		buffer      = indexedBuffer{}
		input       = queue.ReadC
		readerState = readerDone

		output  chan<- indexedBuffer
		timeout <-chan time.Time
	)

	for {
		select {
		case rec := <-input:
			_, err := fmt.Fprintf(&buffer, `{"index":{"_index":"log-%s","_type":"log"}}`+"\n%s\n", rec.Suffix, rec.Document)
			if err != nil {
				mBatchErrors.Mark(1)
				log.Errorf("Invalid record: %s", err)
				break
			}
			buffer.Mark()
			if buffer.Count() >= batchSize {
				input = nil
				output = batchs
			}
		case output <- buffer:
			mBatchRecords.Mark(int64(buffer.Count()))
			mBatchSize.Update(int64(buffer.Count()))
			mBatchBytes.Mark(int64(buffer.Len()))
			log.Debugf("Sent batch, %d records, %d bytes", buffer.Count(), buffer.Len())
			input = queue.ReadC
			output = nil
			buffer = indexedBuffer{}
		case <-timeout:
			timeout = nil
			if buffer.Count() > 0 {
				input = nil
				output = batchs
			} else if readerState == nil {
				return
			}
		case <-readerState:
			readerState = nil
		case <-done:
			log.Debug("Batch aborted")
			return
		}
		if input != nil && timeout == nil {
			timeout = time.After(flushDelay)
		}
	}
}
