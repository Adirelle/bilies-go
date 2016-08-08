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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/beeker1121/goque"
	flag "github.com/ogier/pflag"
	"github.com/op/go-logging"
)

var (
	log = logging.MustGetLogger("github.com/Adirelle/bilies-go")
)

type config struct {
	indexPrefix string
	docType     string
	batchSize   int
	flushDelay  time.Duration
	hosts       string
	protocol    string
	port        int
	username    string
	password    string
	queueDir    string
	logLevel    logging.Level
	debug       bool
	logFile     string
	pidFile     string
}

func main() {
	cfg := config{logLevel: logging.WARNING}

	parseFlags(&cfg)

	if cfg.pidFile != "" {
		setupPidFile(cfg.pidFile)
		defer os.Remove(cfg.pidFile)
	}

	logFile, logBackend := setupLogging(cfg.logFile, cfg.logLevel, cfg.debug)
	defer logFile.Close()

	queue, err := goque.OpenQueue(cfg.queueDir)
	if err != nil {
		log.Panicf("Cannot open queue %q: %s", cfg.queueDir, err)
	}
	defer queue.Close()

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	spv := newMultiSupervisor()

	go func() {
		sig := <-sigChan
		if sig != nil {
			log.Errorf("Received signal %s", sig)
			spv.Interrupt()
		}
	}()

	rspv := spv.Add(newReader(os.Stdin, queue))
	spv.Add(newBatcher(queue, cfg.flushDelay, cfg.batchSize, rspv))

	spv.Start()
	spv.Wait()
	close(sigChan)
}

func parseFlags(c *config) {
	var (
		verbose = false
	)

	pwd, _ := os.Getwd()
	defaultQueueDir := filepath.Join(pwd, ".queue")

	flag.StringVarP(&c.hosts, "hosts", "h", "localhost", "Comma-separated list of hosts")

	flag.StringVarP(&c.protocol, "protocol", "P", "http", "Protocol : http | https")
	flag.IntVarP(&c.port, "port", "p", 9200, "ElasticSearch port")
	flag.StringVarP(&c.username, "user", "u", "", "Username for authentication")
	flag.StringVarP(&c.password, "passwd", "w", "", "Password for authentication")

	flag.StringVarP(&c.indexPrefix, "index", "i", "logs", "Index prefix")
	flag.StringVarP(&c.docType, "type", "t", "log", "Document type")
	flag.IntVarP(&c.batchSize, "batch-size", "n", 500, "Maximum number of events in a batch")
	flag.DurationVarP(&c.flushDelay, "flush-delay", "f", 1*time.Second, "Maximum delay between flushs")

	flag.BoolVarP(&c.debug, "debug", "d", false, "Enable debug output")
	flag.BoolVarP(&verbose, "verbose", "v", false, "Enable verbose")

	flag.StringVar(&c.logFile, "log-file", "", "Write the logs into the file")
	flag.StringVar(&c.pidFile, "pid-file", "", "Write the PID into that file")

	flag.StringVarP(&c.queueDir, "quque", "q", defaultQueueDir, "Queue directory")

	flag.Parse()

	if c.debug {
		c.logLevel = logging.DEBUG
	} else if verbose {
		c.logLevel = logging.INFO
	}
}

func setupPidFile(path string) {
	if pidFile, err := os.Create(path); err == nil {
		defer pidFile.Close()
		if _, err = fmt.Fprintf(pidFile, "%d", os.Getpid()); err != nil {
			log.Panicf("Could not write PID in %q: %s", path, err)
		}
	} else {
		log.Panicf("Could not open PID file %q: %s", path, err)
	}
}

func setupLogging(path string, level logging.Level, debug bool) (logFile *os.File, logBackend *logging.LogBackend) {
	var err error
	if path != "" {
		if logFile, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_SYNC, 0640); err != nil {
			log.Panicf("Cannot open logfile %q: %s", path, err)
		}
	} else {
		logFile = os.Stderr
	}
	logBackend = logging.NewLogBackend(logFile, "", 0)
	logging.SetBackend(logBackend)
	logging.SetLevel(level, log.Module)
	logFormat := "%{time} %{level}: %{message}"
	if debug {
		logFormat = "%{time} %{level}: %{message} (%{shortfile})"
	}
	logging.SetFormatter(logging.MustStringFormatter(logFormat))
	return
}
