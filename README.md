# bilies-go

Bulk Insert Logs Into ElasticSearch - in Go

[![Build Status](https://travis-ci.org/Adirelle/bilies-go.svg?branch=master)](https://travis-ci.org/Adirelle/bilies-go)
[![GoDoc](https://godoc.org/github.com/Adirelle/bilies-go?status.svg)](https://godoc.org/github.com/Adirelle/bilies-go)

bilibes-go is designed to bulk-insert log entry into ElasticSearch.

It waits for JSON-encoded log entries on standard input, one per line, stores them in a disk-based queue, then send
batchs using the bulk API of ElasticSearch.

## Installation

    go get github.com/Adirelle/bilies-go

## Documentation

Documentation is available on [Godoc](https://godoc.org/github.com/Adirelle/bilies-go).

## License

See the [LICENSE file](LICENSE).
