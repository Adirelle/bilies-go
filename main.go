
package main

import (
    "flag"
    "time"
    "strings"
    "bufio"
    "os"
    "os/signal"
    "syscall"
    "log"
    "regexp"
    "errors"
    "net/url"

    elastigo "github.com/mattbaird/elastigo/lib"
)

type IndexerSpec struct {
    IndexPrefix string
    DocType string
    MaxConns int
    BatchSize int
    FlushDelay time.Duration
    RetryDelay time.Duration
}

type Event struct {
    Timestamp string
    Record []byte
}

var eventRegexp = regexp.MustCompile("^(\\d{4}\\.\\d{2}\\.\\d{2}) (\\{.+\\})$")

func main() {

    log.SetOutput(os.Stderr)
    log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

    conn := elastigo.NewConn()
    spec := parseFlags(conn)

    events := make(chan Event)

    installSignalHandlers(events)
    readEvents(events)
    runIndexer(conn, spec, events)
}

func parseFlags(conn *elastigo.Conn) IndexerSpec {

    servers := hosts{ []string{ "localhost" } }

    flag.Var(&servers, "hosts", "Comma-separated list of hosts")

    flag.StringVar(&conn.Protocol, "protocol", "http", "Protocol : http | https")
    flag.StringVar(&conn.Port, "port", "9200", "ElasticSearch port")
    flag.StringVar(&conn.Username, "user", "", "Username for authentication")
    flag.StringVar(&conn.Password, "passwd", "", "Password for authentication")
    flag.BoolVar(&conn.Gzip, "compression", false, "Enable compression ?")

    spec := IndexerSpec{}

    flag.StringVar(&spec.IndexPrefix, "index", "logs-", "Index prefix")
    flag.StringVar(&spec.DocType, "type", "log", "Document type")
    flag.IntVar(&spec.MaxConns, "concurrency", 1, "Number of concurrent connections")
    flag.IntVar(&spec.BatchSize, "batch-size", 500, "Maximum number of events in a batch")
    flag.DurationVar(&spec.FlushDelay, "flush-delay", 1 * time.Second, "Maximum delay between flushs")
    flag.DurationVar(&spec.RetryDelay, "retry-delay", 10 * time.Second, "Delay between retries of failed requests")

    flag.Parse()

    conn.Hosts = servers.Names

    return spec
}

func readEvents(events chan Event) {
    go func() {
        scanner := bufio.NewScanner(os.Stdin)

        defer close(events)

        for scanner.Scan() {
            line := scanner.Bytes()
            if event, err := parseLine(line); err == nil {
                events <- *event
            } else {
                log.Printf("%s: %q", err, line)
            }
        }

        if err := scanner.Err(); err != nil {
            log.Printf("Error on input: %s", err)
        }
    }()
}

func parseLine(line []byte) (*Event, error) {

    matches := eventRegexp.FindSubmatch(line)

    if matches == nil {
        return nil, errors.New("Invalid input")
    }

    record := make([]byte, len(matches[2]))
    copy(record, matches[2])

    return &Event{string(matches[1]), record}, nil
}

func runIndexer(conn *elastigo.Conn, spec IndexerSpec, events <-chan Event) {

    indexer := conn.NewBulkIndexerErrors(spec.MaxConns, int(spec.RetryDelay.Seconds()))
    indexer.BulkMaxDocs = spec.BatchSize
    indexer.BufferDelayMax = spec.FlushDelay

    go func() {
        for err := range indexer.ErrorChannel {
            log.Printf("Rejected request, error: %s, request:\n%s", err.Err, err.Buf.Bytes())
        }
    }()

    indexer.Start()

    var num uint64 = 0;

    for event := range events {
        index := spec.IndexPrefix + event.Timestamp
        num = num + 1;
        if err := indexer.Index(index, spec.DocType, "", "", "", nil, event.Record); err != nil {
            log.Printf("Cannot send record, %s: %q", err, event.Record)
        }
    }

    log.Printf("Stopping indexation, %d pending record(s)", indexer.PendingDocuments())
    indexer.Stop()
    log.Printf("%d record(s) sent, %d error(s)", num, indexer.NumErrors())
}

func installSignalHandlers(done chan Event) {
    sigChan := make(chan os.Signal)
    go func() {
        sig := <-sigChan
        log.Printf("Received signal %s", sig)
        close(done)
    }()

    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
}

type hosts struct {
    Names []string
}

func (h *hosts) Set(v string) error {
    h.Names = strings.Split(v, ",")
    return nil
}

func (h *hosts) String() string {
    return strings.Join(h.Names, ",")
}
