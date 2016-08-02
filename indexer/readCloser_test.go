package indexer

import (
	"io"
	"io/ioutil"
	"strings"
)

func readerFrom(b string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(b))
}
