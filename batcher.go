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
Batchs

Messages are gathered in batchs before being sent to the server.
Batchs are sent when the number of messages reachs a defined value or when the delay since the last sent exceeds a defined value.

The following switches control the generation of bulk messages:

	-i --index=STRING [default: logs]
		Define the prefix of the index name.

	-t --type=STRING [default: log]
		Define the type of messages.

	-n --batch-size=INT [default: 500]
		The maximum number of message to send in a single request.

	-f --flush-delay=DURATION [default: 1s]
		The maximum delay between two requests.
*/
package main

import (
	"fmt"
	"time"

	"github.com/rcrowley/go-metrics"
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

	batchs = make(chan IndexedBuffer)
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
		input       = queue.ReadC
		readerState = readerDone
		buffer      = MakeIndexedBuffer(batchSize)

		output  chan<- IndexedBuffer
		timeout <-chan time.Time
	)

	for {
		select {
		case rec := <-input:
			_, err := fmt.Fprintf(&buffer, `{"index":{"_id":%q, "_index":"%s-%s","_type":"%s"}}`+"\n%s\n", rec.ID, indexPrefix, rec.Suffix, docType, rec.Document)
			if err != nil {
				mBatchErrors.Mark(1)
				logger.Errorf("Could not write record: %s", err)
				break
			}
			buffer.Mark(rec.ID)
			if buffer.Count() >= batchSize {
				input = nil
				output = batchs
			}
		case output <- buffer:
			mBatchRecords.Mark(int64(buffer.Count()))
			mBatchSize.Update(int64(buffer.Count()))
			mBatchBytes.Mark(int64(buffer.Len()))
			logger.Debugf("Sent batch, %d records, %d bytes", buffer.Count(), buffer.Len())
			input = queue.ReadC
			output = nil
			buffer = MakeIndexedBuffer(batchSize)
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
			logger.Debug("Batch aborted")
			return
		}
		if input != nil && timeout == nil {
			timeout = time.After(flushDelay)
		}
	}
}
