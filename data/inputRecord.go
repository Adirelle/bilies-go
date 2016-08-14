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

package data

import (
	"encoding/json"
	"fmt"
)

// Record defines the expected schema of input.
type InputRecord struct {
	ID       string          `json:"id"`
	Suffix   string          `json:"date"`
	Document json.RawMessage `json:"log"`
}

func (r InputRecord) String() string {
	return fmt.Sprintf("id=%q suffix=%s doc=%s", r.ID, r.Suffix, r.Document)
}

func (r InputRecord) Record() Record {
	return Record{ID: r.ID, Suffix: r.Suffix, Document: string(r.Document)}
}
