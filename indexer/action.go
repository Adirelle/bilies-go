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
	return fmt.Fprintf(w, "{\"index\":{\"_index\":%q,\"_type\":%q,\"id_\":%q}}\n%s\n", a.Index, a.DocType, a.ID, a.Document)
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
