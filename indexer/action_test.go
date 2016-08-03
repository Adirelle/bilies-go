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
	"testing"

	. "gopkg.in/check.v1"
)

func TestAction(t *testing.T) { TestingT(t) }

type ActionTestSuite struct{}

var _ = Suite(&ActionTestSuite{})

func (_ *ActionTestSuite) TestGetID(c *C) {
	a := SimpleAction{ID: "bla"}
	c.Check(a.GetID(), Equals, "bla")
}

func (_ *ActionTestSuite) TestWriteBulkTo(c *C) {
	a := SimpleAction{"myId", "myIndex", "myType", []byte("{data}")}
	buf := bytes.Buffer{}
	_, err := a.WriteBulkTo(&buf)
	c.Assert(err, IsNil)
	c.Assert(buf.String(), Equals, "{\"index\":{\"_index\":\"myIndex\",\"_type\":\"myType\",\"_id\":\"myId\"}}\n{data}\n")
}
