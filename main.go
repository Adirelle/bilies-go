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
}

var eventRegexp = regexp.MustCompile("^(\\d{4}\\.\\d{2}\\.\\d{2}) (\\{.+\\})$")

func main() {

	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	indexer := Indexer{}
	indexer.readFlags()
	indexer.setup()
	indexer.installSignalHandler()

	go indexer.runReader()
	go indexer.runBatcher()

	indexer.waitGroup.Wait()
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
}

func (i *Indexer) installSignalHandler() {
	sigChan := make(chan os.Signal)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %s", sig)
		close(i.actions)
	}()
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
}

func (i *Indexer) runReader() {

	i.waitGroup.Add(1)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64000), 64000)
	defer func() {
		if err := scanner.Err(); err != nil {
			log.Printf("Error on input: %s", err)
		}
		close(i.actions)
		log.Println("Stopped reader")
		i.waitGroup.Done()
	}()

	log.Println("Started reader")

	for scanner.Scan() {
		line := scanner.Bytes()
		if action, err := i.parseLine(line); err == nil {
			i.actions <- action
		} else {
			log.Printf("%s: %q", err, line)
		}
	}
}

func (i *Indexer) runBatcher() {
	i.waitGroup.Add(1)
	defer func() {
		i.batcher.Stop()
		log.Println("Batcher stopped")
		log.Printf("Batcher: received %d, sent %d, errors %d", i.batcher.Received(), i.batcher.Sent(), i.batcher.Errors())
		i.waitGroup.Done()
	}()

	log.Println("Started batcher")
	for a := range i.actions {
		i.batcher.Send(a)
	}
}

func (i *Indexer) parseLine(line []byte) (indexer.Action, error) {
	matches := eventRegexp.FindSubmatch(line)

	if matches == nil {
		return nil, errors.New("Invalid input")
	}

	return indexer.SimpleAction{
		ID:       "",
		Index:    fmt.Sprintf("%s-%s", i.IndexPrefix, matches[1]),
		DocType:  i.DocType,
		Document: matches[2],
	}, nil
}
