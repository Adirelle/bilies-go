
package main

import (
    //"encoding/json"
    "flag"
    "time"
    "strings"
    "bufio"
    "os"
    "os/signal"
    "syscall"

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

func main() {

    conn := elastigo.NewConn()
    spec := parseFlags(conn)

    done := make(chan struct{})

    installSignalHandlers(done)

    indexer := createIndexer(conn, spec, done)
    lines := readLines(done)

    for line := range lines {
        index := spec.IndexPrefix + string(line[:10])
        record := line[11:]
        if err := indexer.Index(index, spec.DocType, "", "", "", nil, record); err != nil {
            println("Document error:", err)
        }
    }
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

func readLines(done chan struct{}) <-chan []byte {
    lines := make(chan []byte)

    go func() {
        defer close(lines)

        scanner := bufio.NewScanner(os.Stdin)

        for scanner.Scan() {
            line := scanner.Bytes()
            if len(line) < 13 {
                println("Invalid input:", string(line))
                continue
            }
            lines <- line
        }

        if err := scanner.Err(); err != nil {
            println("Error on input:", err.Error())
        }
    }()

    go func() {
        <- done
        println("Closing STDIN")
        os.Stdin.Close()
    }()

    return lines
}

func createIndexer(conn *elastigo.Conn, spec IndexerSpec, done <-chan struct{}) *elastigo.BulkIndexer {

    indexer := conn.NewBulkIndexerErrors(spec.MaxConns, int(spec.RetryDelay.Seconds()))
    indexer.BulkMaxDocs = spec.BatchSize
    indexer.BufferDelayMax = spec.FlushDelay

    go func() {
        for err := range indexer.ErrorChannel {
            println("Failed request,", err.Err.Error(), ":\n", string(err.Buf.Bytes()))
        }
    }()

    go func() {
        <- done
        println("Stopping indexation,", indexer.PendingDocuments(), "pending document(s)")
        indexer.Stop()
        println(indexer.NumErrors(), "error(s) during indexation")
    }()

    indexer.Start()

    return indexer
}

func installSignalHandlers(done chan struct{}) {
    sigChan := make(chan os.Signal)
    go func() {
        select {
            case <- done:
                return
            case sig := <-sigChan:
                println("Received signal:", sig.String())
                close(done)
        }
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
