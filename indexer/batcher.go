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
package indexer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type Batcher interface {
	Send(Action)
	Stop()

	Results() <-chan ActionResult

	Pending() int
	Received() uint64
	Sent() uint64
	Errors() uint64
}

type batcher struct {
	requester Requester

	actionChannel chan Action
	resultChannel *chan ActionResult
	pending       map[string]Action
	buf           bytes.Buffer
	timer         *time.Timer
	running       bool
	muRun         sync.Mutex
	wgDone        sync.WaitGroup

	flushDelay time.Duration
	flushCount int

	received uint64
	sent     uint64
	errors   uint64
}

func NewBatcher(r Requester, flushDelay time.Duration, flushCount int, bufferSize int) Batcher {
	b := &batcher{
		requester: r,

		actionChannel: make(chan Action),
		pending:       make(map[string]Action, flushCount),
		buf:           bytes.Buffer{},
		running:       true,
		timer:         time.NewTimer(flushDelay),
		flushDelay:    flushDelay,
		flushCount:    flushCount,
	}
	b.buf.Grow(bufferSize)
	go b.run()
	return b
}

func (b *batcher) Stop() {
	b.muRun.Lock()
	if b.running {
		b.running = false
		close(b.actionChannel)
	}
	b.muRun.Unlock()
	b.wgDone.Wait()
}

func (b *batcher) Results() <-chan ActionResult {
	if b.resultChannel == nil {
		c := make(chan ActionResult, b.flushCount)
		b.resultChannel = &c
	}
	return *b.resultChannel
}

func (b *batcher) Send(a Action) {
	b.actionChannel <- a
}

func (b *batcher) Pending() int {
	return len(b.pending)
}

func (b *batcher) Received() uint64 {
	return atomic.LoadUint64(&b.received)
}

func (b *batcher) Sent() uint64 {
	return atomic.LoadUint64(&b.sent)
}

func (b *batcher) Errors() uint64 {
	return atomic.LoadUint64(&b.errors)
}

func (b *batcher) run() {
	b.wgDone.Add(1)
	defer func() {
		b.timer.Stop()
		if b.resultChannel != nil {
			close(*b.resultChannel)
		}
		log.Debugf("Batcher stopped")
		b.wgDone.Done()
	}()

	log.Debugf("Batcher started")
	for b.running {
		b.collectActions()
		if len(b.pending) > 0 {
			b.sendBatch()
		}
	}
}

func (b *batcher) collectActions() {
	b.timer.Reset(b.flushDelay)
	for b.running && len(b.pending) < b.flushCount {
		select {
		case a, open := <-b.actionChannel:
			if a != nil {
				b.pending[a.GetID()] = a
				atomic.AddUint64(&b.received, 1)
			}
			if !open {
				b.running = false
			}
		case <-b.timer.C:
			if len(b.pending) > 0 {
				log.Debugf("Flush timeout")
				return
			}
			b.timer.Reset(b.flushDelay)
		}
	}
	log.Debugf("%d action(s) collected", len(b.pending))
}

type bulkResponse struct {
	Took  int `json:"took"`
	Items []struct {
		Index *struct {
			ID  string   `json:"_id"`
			Err *esError `json:"error"`
		} `json:"index"`
	} `json:"items"`
	Err *esError `json:"error"`
}

type esError struct {
	Reason string   `json:"reason"`
	Cause  *esError `json:"caused_by"`
}

func (e esError) Error() string {
	if e.Cause != nil {
		if msg := e.Cause.Error(); msg != e.Reason {
			return fmt.Sprintf("%s: %s", e.Reason, msg)
		}
	}
	return e.Reason
}

func (b *batcher) sendBatch() {
	defer b.buf.Reset()
	for _, a := range b.pending {
		a.WriteBulkTo(&b.buf)
	}

	body, err := b.requester.Send(bytes.NewReader(b.buf.Bytes()))
	if body != nil {
		defer body.Close()
	}

	if err == nil {
		err = b.parseResponse(body)
	}

	for id := range b.pending {
		b.resolveAction(id, err)
	}
}

func (b *batcher) parseResponse(body io.Reader) error {
	resp := bulkResponse{}

	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		log.Debugf("Could not decode JSON response: %s", err.Error())
		return err
	}

	if resp.Err != nil {
		log.Debugf("Bulk error: %s", resp.Err.Error())
		return resp.Err
	}

	for _, item := range resp.Items {
		if res := item.Index; res != nil && res.Err != nil {
			b.resolveAction(res.ID, res.Err)
		}
	}

	return nil
}

func (b *batcher) resolveAction(id string, err error) {
	a, found := b.pending[id]
	if !found {
		log.Warningf("Unknown action %q", id)
		return
	}
	delete(b.pending, id)
	if err != nil {
		log.Warningf("Action error for %s: %s", a, err.Error())
		atomic.AddUint64(&b.errors, 1)
	} else {
		log.Debugf("Action succeeded, %s", a)
		atomic.AddUint64(&b.sent, 1)
	}
	if b.resultChannel != nil {
		*b.resultChannel <- ActionResult{a, err}
	}
}
