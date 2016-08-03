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
	"fmt"
	"io"
)

// A bulk action
type Action interface {
	String() string
	WriteBulkTo(io.Writer) (int, error)
	GetID() string
}

// SimpleAction: a simple action
type SimpleAction struct {
	ID       string
	Index    string
	DocType  string
	Document []byte
}

func (a SimpleAction) WriteBulkTo(w io.Writer) (int, error) {
	return fmt.Fprintf(w, "{\"index\":{\"_index\":%q,\"_type\":%q,\"_id\":%q}}\n%s\n", a.Index, a.DocType, a.ID, a.Document)
}

func (a SimpleAction) GetID() string {
	return a.ID
}

func (a SimpleAction) String() string {
	return fmt.Sprintf("index %s/%s/%s", a.Index, a.DocType, a.ID)
}

type ActionResult struct {
	Action Action
	Err    error
}

func (r ActionResult) Error() string {
	return fmt.Sprint(r.Action, ": ", r.Err)
}
