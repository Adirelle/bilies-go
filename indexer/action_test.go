package indexer

import (
	"bytes"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

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
	c.Assert(buf.String(), Equals, "{\"index\":{\"_index\":\"myIndex\",\"_type\":\"myType\",\"id_\":\"myId\"}}\n{data}\n")
}
