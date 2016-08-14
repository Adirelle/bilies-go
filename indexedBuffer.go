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
	"bytes"
	"fmt"
)

// IndexedBuffer is a bytes.Buffer that also holds an index and a map to identify records.
type IndexedBuffer struct {
	bytes.Buffer
	index []int
	keys  map[string]int
}

func MakeIndexedBuffer(n int) IndexedBuffer {
	return IndexedBuffer{
		Buffer: *bytes.NewBuffer(make([]byte, 0, n*1024)),
		index:  make([]int, 0, n),
		keys:   make(map[string]int, n),
	}
}

// Mark marks a record in the buffer.
func (b *IndexedBuffer) Mark(key string) {
	b.index = append(b.index, b.Len())
	b.keys[key] = len(b.index)
}

// Count returns the number of marked records.
func (b *IndexedBuffer) Count() int {
	return len(b.index)
}

// PosOf returns the start of the given records.
func (b *IndexedBuffer) PosOf(i int) int {
	if i == 0 {
		return 0
	}
	return b.index[i-1]
}

// GetByKey returns a byte slice containing the record identified by the given key.
func (b *IndexedBuffer) GetByKey(key string) ([]byte, error) {
	if i, found := b.keys[key]; found {
		return b.Bytes()[b.PosOf(i-1):b.PosOf(i)], nil
	}
	return nil, fmt.Errorf("Record not found: %q", key)
}

// Slice returns a byte slice containing the records between i, included, and j, excluded.
func (b *IndexedBuffer) Slice(i int, j int) []byte {
	return b.Bytes()[b.PosOf(i):b.PosOf(j)]
}
