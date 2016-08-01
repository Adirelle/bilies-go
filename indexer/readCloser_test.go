package indexer

import (
	"io"
	"strings"
)

type readCloser struct {
	io.Reader
}

func readerFrom(b string) io.ReadCloser {
	return &readCloser{Reader: strings.NewReader(b)}
}

func (b readCloser) Close() error {
	return nil
}
