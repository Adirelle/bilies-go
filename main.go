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
	"sync"
	"syscall"
	"time"

	flag "github.com/ogier/pflag"
	"github.com/op/go-logging"
	"github.com/wayn3h0/go-uuid"

	"github.com/Adirelle/bilies-go/indexer"
)

var (
	log     = logging.MustGetLogger("github.com/Adirelle/bilies-go")
	debug   = false
	verbose = false
)

type Indexer struct {
	Hosts       string
	Protocol    string
	Port        int
	Username    string
	Password    string
	IndexPrefix string
	DocType     string
	BatchSize   int
	FlushDelay  time.Duration

	actions   chan indexer.Action
	batcher   indexer.Batcher
	waitGroup sync.WaitGroup
	done      chan struct{}
}

var eventRegexp = regexp.MustCompile("^(\\d{4}\\.\\d{2}\\.\\d{2}) (\\{.+\\})$")

func main() {

	logging.SetFormatter(logging.MustStringFormatter("%{time} %{program}[%{pid}] %{module} %{level} %{color}%{message}"))

	i := Indexer{}

	i.readFlags()

	level := logging.WARNING
	if debug {
		level = logging.DEBUG
	} else if verbose {
		level = logging.INFO
	}
	logging.SetLevel(level, "github.com/Adirelle/bilies-go")
	logging.SetLevel(level, "github.com/Adirelle/bilies-go/indexer")

	i.setup()
	i.installSignalHandler()

	i.startReader()
	i.startBatcher()

	i.waitGroup.Wait()
}

func (i *Indexer) readFlags() {
	flag.StringVarP(&i.Hosts, "hosts", "h", "localhost", "Comma-separated list of hosts")

	flag.StringVarP(&i.Protocol, "protocol", "P", "http", "Protocol : http | https")
	flag.IntVarP(&i.Port, "port", "p", 9200, "ElasticSearch port")
	flag.StringVarP(&i.Username, "user", "u", "", "Username for authentication")
	flag.StringVarP(&i.Password, "passwd", "w", "", "Password for authentication")

	flag.StringVarP(&i.IndexPrefix, "index", "i", "logs", "Index prefix")
	flag.StringVarP(&i.DocType, "type", "t", "log", "Document type")
	flag.IntVarP(&i.BatchSize, "batch-size", "n", 500, "Maximum number of events in a batch")
	flag.DurationVarP(&i.FlushDelay, "flush-delay", "f", 1*time.Second, "Maximum delay between flushs")

	flag.BoolVarP(&debug, "debug", "d", false, "Enable debug output")
	flag.BoolVarP(&verbose, "verbose", "v", false, "Enable verbose")

	flag.Parse()
}

func (i *Indexer) setup() {
	hosts := strings.Split(i.Hosts, ",")
	requester := indexer.NewRequester(http.Client{}, hosts, i.Port, i.Protocol, i.Username, i.Password)
	i.batcher = indexer.NewBatcher(requester, i.FlushDelay, i.BatchSize, 1024*i.BatchSize)
	i.actions = make(chan indexer.Action)
	i.done = make(chan struct{})
}

func (i *Indexer) installSignalHandler() {
	sigChan := make(chan os.Signal)
	go func() {
		select {
		case sig := <-sigChan:
			log.Errorf("Received signal %s", sig)
			close(i.done)
		case <-i.done:
		}
	}()
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
}

func (i *Indexer) startReader() {
	go func() {
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
			close(i.actions)
		}()

		for scanner.Scan() {
			l := scanner.Bytes()
			if len(l) == 0 {
				continue
			}
			count++
			if a, err := i.parseLine(l); err == nil {
				forwarded++
				i.actions <- a
			} else {
				errors++
				log.Warningf("%s: %q", err.Error(), l)
			}
		}

		close(i.done)
	}()
}

func (i *Indexer) startBatcher() {
	i.waitGroup.Add(2)

	go func() {
		defer func() {
			i.batcher.Stop()
			log.Infof("Output: queued %d, sent %d, errors %d", i.batcher.Received(), i.batcher.Sent(), i.batcher.Errors())
			i.waitGroup.Done()
		}()

		for {
			select {
			case a, cont := <-i.actions:
				if a != nil {
					i.batcher.Send(a)
				}
				if !cont {
					return
				}
			case <-i.done:
				return
			}
		}
	}()

	go func() {
		defer i.waitGroup.Done()
		c := i.batcher.Results()
		for {
			select {
			case r := <-c:
				if r.Err != nil {
					log.Warningf("Output error: %s", r.Error())
				}
			case <-i.done:
				return
			}
		}
	}()
}

func (i *Indexer) parseLine(line []byte) (indexer.Action, error) {
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
		Index:    fmt.Sprintf("%s-%s", i.IndexPrefix, matches[1]),
		DocType:  i.DocType,
		Document: matches[2],
	}, nil
}
