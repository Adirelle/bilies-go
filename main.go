package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"./indexer"
	uuid "github.com/wayn3h0/go-uuid"
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

	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	i := Indexer{}

	i.readFlags()
	i.setup()
	i.installSignalHandler()

	i.startReader()
	i.startBatcher()

	i.waitGroup.Wait()
}

func (i *Indexer) readFlags() {
	flag.StringVar(&i.Hosts, "hosts", "localhost", "Comma-separated list of hosts")

	flag.StringVar(&i.Protocol, "protocol", "http", "Protocol : http | https")
	flag.IntVar(&i.Port, "port", 9200, "ElasticSearch port")
	flag.StringVar(&i.Username, "user", "", "Username for authentication")
	flag.StringVar(&i.Password, "passwd", "", "Password for authentication")

	flag.StringVar(&i.IndexPrefix, "index", "logs", "Index prefix")
	flag.StringVar(&i.DocType, "type", "log", "Document type")
	flag.IntVar(&i.BatchSize, "batch-size", 500, "Maximum number of events in a batch")
	flag.DurationVar(&i.FlushDelay, "flush-delay", 1*time.Second, "Maximum delay between flushs")

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
			log.Printf("Received signal %s", sig)
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
			log.Printf("Input: read %d, forwarded %d, errors %d", count, forwarded, errors)
			if err := scanner.Err(); err != nil {
				log.Printf("Input error: %s", err.Error())
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
				log.Printf("%s: %q", err, l)
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
			log.Printf("Output: queued %d, sent %d, errors %d", i.batcher.Received(), i.batcher.Sent(), i.batcher.Errors())
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
					log.Printf("Output error: %s", r.Error())
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
