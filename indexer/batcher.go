package indexer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	doneChannel   chan struct{}
	pending       map[string]Action
	buf           bytes.Buffer
	timer         *time.Timer
	running       bool

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
		doneChannel:   make(chan struct{}),
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
	close(b.actionChannel)
	if b.resultChannel != nil {
		close(*b.resultChannel)
	}
	<-b.doneChannel
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
	//log.Println("Worker started")
	defer b.timer.Stop()
	defer close(b.doneChannel)

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
				return
			}
			b.timer.Reset(b.flushDelay)
		}
	}
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
		return err
	}

	if resp.Err != nil {
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
		return
	}
	delete(b.pending, id)
	if err != nil {
		atomic.AddUint64(&b.errors, 1)
	} else {
		atomic.AddUint64(&b.sent, 1)
	}
	if b.resultChannel != nil {
		*b.resultChannel <- ActionResult{a, err}
	}
}
