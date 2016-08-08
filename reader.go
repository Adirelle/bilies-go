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

	"github.com/beeker1121/goque"
)

type inputRecord struct {
	Suffix   string          `json:"date"`
	Document json.RawMessage `json:"log"`
}

type reader struct {
	reader  io.Reader
	queue   *goque.Queue
	scanner *bufio.Scanner
}

func newReader(r io.Reader, q *goque.Queue) service {
	return &reader{reader: r, queue: q}
}

func (r *reader) Init() {
	r.scanner = bufio.NewScanner(r.reader)
}

func (r *reader) Continue() bool {
	return r.scanner.Scan()
}

func (r *reader) Iterate() {
	var rec inputRecord
	buf := r.scanner.Bytes()
	err := json.Unmarshal(buf, &rec)
	if err != nil {
		log.Warningf("Invalid input, %s: %q", err, buf)
		return
	}
	if rec.Suffix == "" || rec.Document == nil {
		log.Warningf("Invalid input, %s: %q", err, buf)
		return
	}
	_, err = r.queue.EnqueueObject(rec)
	if err != nil {
		log.Warningf("Could not enqueue: %s", err)
		return
	}
}

func (r *reader) Cleanup() {
	if err := r.scanner.Err(); err != nil {
		log.Warningf("Scanner error: %s", err)
	}
}

func (r *reader) String() string {
	return "reader"
}
