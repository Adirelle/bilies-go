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
	"bytes"
	"encoding/json"
	"fmt"
)

type ESResponse struct {
	ESStatus
	Took  int              `json:"took"`
	Items []ESItemResponse `json:"items"`
}

func (r ESResponse) String() string {
	buf := bytes.Buffer{}
	fmt.Fprintf(&buf, "took=%d %s [\n", r.Took, r.ESStatus)
	for _, item := range r.Items {
		fmt.Fprintf(&buf, "\t%s\n", item)
	}
	buf.WriteString("]")
	return buf.String()
}

type ESStatus struct {
	Status int      `json:"status"`
	Err    *ESError `json:"error"`
}

func (s ESStatus) String() string {
	buf := bytes.Buffer{}
	if s.Status != 0 {
		fmt.Fprintf(&buf, "status=%d", s.Status)
	}
	if s.Err != nil {
		fmt.Fprintf(&buf, " error=%s", s.Err)
	}
	return buf.String()
}

func (s ESStatus) ToError() error {
	if s.Status < 400 {
		return nil
	}
	return &s
}

func (s ESStatus) Error() string {
	if s.Status < 400 || s.Err == nil {
		return fmt.Sprintf("Status %d", s.Status)
	}
	return fmt.Sprintf("Status %d: %s", s.Status, s.Err)
}

type ESItemResponse struct {
	Create *ESOpStatus `json:"create"`
	Index  *ESOpStatus `json:"index"`
}

func (r ESItemResponse) Status() *ESOpStatus {
	if r.Create == nil {
		return r.Index
	}
	return r.Create
}

func (r ESItemResponse) String() string {
	return r.Status().String()
}

type ESOpStatus struct {
	ESStatus
	ID     string          `json:"_id"`
	Record json.RawMessage `json:"_source"`
}

func (s ESOpStatus) String() string {
	return fmt.Sprintf("id=%q %s record=%q", s.ID, s.ESStatus, s.Record)
}

type ESError struct {
	Reason string   `json:"reason"`
	Cause  *ESError `json:"caused_by"`
}

func (e ESError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s, cause: %s", e.Reason, e.Cause)
	}
	return e.Reason
}

func (e ESError) String() string {
	s := fmt.Sprintf("reason=%q", e.Reason)
	if e.Cause != nil {
		s += e.Cause.String()
	}
	return s
}
