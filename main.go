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
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/beeker1121/goque"
	"github.com/spf13/pflag"
)

var (
	startGroup = sync.WaitGroup{}
	mainGroup  = sync.WaitGroup{}
	endGroup   = sync.WaitGroup{}
	done       = make(chan bool)

	hosts    = "localhost"
	protocol = "http"
	port     = 9200
	username string
	password string

	pidFile string

	queueDir string
	queue    *goque.Queue
)

func init() {
	pflag.StringVar(&pidFile, "pid-file", pidFile, "Write the PID into that file")

	if pwd, err := os.Getwd(); err == nil {
		queueDir = filepath.Join(pwd, ".queue")
	}
	pflag.StringVarP(&queueDir, "queue-dir", "q", queueDir, "Queue directory")
}

func main() {
	pflag.Parse()

	StartLogging()
	defer StopLogging()
	log.Noticef("===== bilies-go starting, PID %d =====", os.Getpid())

	if pidFile != "" {
		SetupPidFile()
		defer os.Remove(pidFile)
	}

	var err error
	queue, err = goque.OpenQueue(queueDir)
	if err != nil {
		log.Panicf("Cannot open queue %q: %s", queueDir, err)
	}
	defer queue.Close()

	StartAndForget("Signal handler", SignalHandler)

	StartMetrics()
	StartReader()
	StartBatcher()

	startGroup.Wait()
	log.Notice("Start complete")

	mainGroup.Wait()

	Shutdown()
}

func SetupPidFile() {
	if f, err := os.Create(pidFile); err == nil {
		defer f.Close()
		if _, err = fmt.Fprintf(f, "%d", os.Getpid()); err != nil {
			log.Panicf("Could not write PID in %q: %s", pidFile, err)
		}
	} else {
		log.Panicf("Could not open PID file %q: %s", pidFile, err)
	}
}

func Start(name string, f func()) {
	startGroup.Add(1)
	endGroup.Add(1)
	go func() {
		defer log.Debugf("%s ended", name)
		defer endGroup.Done()
		log.Debugf("%s started", name)
		startGroup.Done()
		f()
	}()
}

func StartMain(name string, f func()) {
	startGroup.Add(1)
	mainGroup.Add(1)
	endGroup.Add(1)
	go func() {
		defer log.Debugf("%s ended", name)
		defer mainGroup.Done()
		defer endGroup.Done()
		log.Debugf("%s started", name)
		startGroup.Done()
		f()
	}()
}

func StartAndForget(name string, f func()) {
	startGroup.Add(1)
	go func() {
		defer log.Debugf("%s ended", name)
		log.Debugf("%s started", name)
		startGroup.Done()
		f()
	}()
}

func SignalHandler() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case sig := <-sigChan:
			log.Errorf("Received signal: %s", sig)
			Shutdown()
		case <-done:
			return
		}
	}
}

func Shutdown() {
	select {
	case <-done:
		return // Already closed
	default:
		log.Notice("Shutting down")
		close(done)
	}

	ok := make(chan bool)
	go func() {
		endGroup.Wait()
		close(ok)
	}()

	select {
	case <-ok:
		log.Notice("Shutdown complete")
	case <-time.After(2 * time.Second):
		log.Fatal("Forceful shutdown")
	}
}

const (
	// BackoffBaseDelay is the minimum duration of backoff
	BackoffBaseDelay = float64(500 * time.Millisecond)
	// BackoffMaxDelay is the maximum duration of backoff
	BackoffMaxDelay = 2 * time.Minute
	// BackoffFactor is the duration multiplier
	BackoffFactor = 2.0
)

// BackoffDelay calculates a backoff delay, given a number of consecutive failures.
func BackoffDelay(n int) time.Duration {
	d := time.Duration(BackoffBaseDelay * math.Pow(BackoffFactor, float64(n-1)))
	if d > BackoffMaxDelay {
		return BackoffMaxDelay
	}
	return d
}
