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

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	flag "github.com/ogier/pflag"
	"github.com/op/go-logging"
	"github.com/wayn3h0/go-uuid"

	"github.com/Adirelle/bilies-go/indexer"
)

var (
	log         = logging.MustGetLogger("github.com/Adirelle/bilies-go")
	eventRegexp = regexp.MustCompile("^(\\d{4}\\.\\d{2}\\.\\d{2}) (\\{.+\\})$")

	indexPrefix = "logs"
	docType     = "log"
)

func main() {
	batcher := setupBatcher()
	installSignalHandler(batcher)
	go runReader(batcher)

	defer log.Infof("Output: queued %d, sent %d, errors %d", batcher.Received(), batcher.Sent(), batcher.Errors())
	for r := range batcher.Results() {
		if r.Err != nil {
			log.Warningf("Output error: %s", r.Error())
		}
	}
}

func setupBatcher() indexer.Batcher {
	var (
		debug   = false
		verbose = false

		hosts      = "localhost"
		protocol   = "http"
		port       = 9200
		username   = ""
		password   = ""
		batchSize  = 500
		flushDelay = 1 * time.Second
	)

	flag.StringVarP(&hosts, "hosts", "h", "localhost", "Comma-separated list of hosts")

	flag.StringVarP(&protocol, "protocol", "P", "http", "Protocol : http | https")
	flag.IntVarP(&port, "port", "p", 9200, "ElasticSearch port")
	flag.StringVarP(&username, "user", "u", "", "Username for authentication")
	flag.StringVarP(&password, "passwd", "w", "", "Password for authentication")

	flag.StringVarP(&indexPrefix, "index", "i", "logs", "Index prefix")
	flag.StringVarP(&docType, "type", "t", "log", "Document type")
	flag.IntVarP(&batchSize, "batch-size", "n", 500, "Maximum number of events in a batch")
	flag.DurationVarP(&flushDelay, "flush-delay", "f", 1*time.Second, "Maximum delay between flushs")

	flag.BoolVarP(&debug, "debug", "d", false, "Enable debug output")
	flag.BoolVarP(&verbose, "verbose", "v", false, "Enable verbose")

	flag.Parse()

	level := logging.WARNING
	if debug {
		level = logging.DEBUG
	} else if verbose {
		level = logging.INFO
	}
	logging.SetBackend(logging.NewLogBackend(os.Stderr, "", 0))
	logging.SetLevel(level, "github.com/Adirelle/bilies-go")
	logging.SetLevel(level, "github.com/Adirelle/bilies-go/indexer")
	logging.SetFormatter(logging.MustStringFormatter("%{time} %{level}: %{message} (%{shortfile})"))

	hostsList := strings.Split(hosts, ",")
	requester := indexer.NewRequester(http.Client{}, hostsList, port, protocol, username, password)
	batcher := indexer.NewBatcher(requester, flushDelay, batchSize, 1024*batchSize)

	return batcher
}

func installSignalHandler(batcher indexer.Batcher) {
	sigChan := make(chan os.Signal)
	go func() {
		sig := <-sigChan
		log.Errorf("Received signal %s", sig)
		batcher.Stop()
	}()
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
}

func runReader(batcher indexer.Batcher) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64000), 64000)
	count := 0
	forwarded := 0
	errors := 0
	defer func() {
		log.Infof("Input: read %d, forwarded %d, errors %d", count, forwarded, errors)
		if err := scanner.Err(); err != nil {
			log.Errorf("Input error: %s", err.Error())
		}
		batcher.Stop()
	}()

	for scanner.Scan() {
		l := scanner.Bytes()
		if len(l) == 0 {
			continue
		}
		count++
		if a, err := parseLine(l); err == nil {
			forwarded++
			batcher.Send(a)
		} else {
			errors++
			log.Warningf("%s: %q", err.Error(), l)
		}
	}
}

func parseLine(line []byte) (indexer.Action, error) {
	matches := eventRegexp.FindSubmatch(line)

	if matches == nil {
		return nil, errors.New("Invalid input")
	}

	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	return indexer.SimpleAction{
		ID:       id.String(),
		Index:    fmt.Sprintf("%s-%s", indexPrefix, matches[1]),
		DocType:  docType,
		Document: matches[2],
	}, nil
}
