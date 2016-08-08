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

import "bytes"

type indexedBuffer struct {
	bytes.Buffer
	index []int
}

func (b *indexedBuffer) Reset() {
	b.Buffer.Reset()
	b.index = b.index[:0]
}

func (b *indexedBuffer) Mark() {
	b.index = append(b.index, b.Len())
}

func (b *indexedBuffer) Count() int {
	return len(b.index)
}

func (b *indexedBuffer) PosOf(i int) int {
	if i == 0 {
		return 0
	}
	return b.index[i-1]
}

func (b *indexedBuffer) Slice(i int, j int) []byte {
	return b.Bytes()[b.PosOf(i):b.PosOf(j)]
}
