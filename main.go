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

/*

PID file

	--pid-file=STRING
		Write the PID into  the specified file.
*/
package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"
)

var (
	tasks      []func()
	startGroup = sync.WaitGroup{}
	mainGroup  = sync.WaitGroup{}
	endGroup   = sync.WaitGroup{}
	done       = make(chan bool)

	queue    *Queue
	queueDir string

	pidFile string
)

func init() {
	if pwd, err := os.Getwd(); err == nil {
		queueDir = filepath.Join(pwd, ".queue")
	}

	pflag.StringVarP(&queueDir, "queue-dir", "q", queueDir, "Queue directory")
	pflag.StringVar(&pidFile, "pid-file", pidFile, "Write the PID into that file")

	AddBackgroundTask("Signal handler", SignalHandler)
}

func main() {
	pflag.Parse()

	SetupLogging()
	logger.Noticef("===== bilies-go starting, PID %d =====", os.Getpid())

	var err error
	queue, err = OpenQueue(queueDir)
	if err != nil {
		logger.Fatalf("Cannot open the message queue in %q: %s", queueDir, err)
	}
	defer queue.Close()

	if pidFile != "" {
		SetupPidFile()
		defer os.Remove(pidFile)
	}

	StartTasks()

	mainGroup.Wait()

	Shutdown()
}

// SetupPidFile creates the PID file and writes the current PID into it
func SetupPidFile() {
	if f, err := os.Create(pidFile); err == nil {
		defer f.Close()
		if _, err = fmt.Fprintf(f, "%d", os.Getpid()); err != nil {
			logger.Panicf("Could not write PID in %q: %s", pidFile, err)
		}
	} else {
		logger.Panicf("Could not open PID file %q: %s", pidFile, err)
	}
}

// Start starts f as a goroutine, and registers it in the starting and ending groups.
func Start(name string, f func()) {
	startGroup.Add(1)
	endGroup.Add(1)
	go func() {
		defer logger.Debugf("%s ended", name)
		defer endGroup.Done()
		logger.Debugf("%s started", name)
		startGroup.Done()
		f()
	}()
}

func StartTasks() {
	for _, task := range tasks {
		go task()
	}
	startGroup.Wait()
}

// StartMain starts f as a goroutine, and registers it in the starting, main and ending groups.
func addTask(name string, f func(), main bool, wait bool) {
	startGroup.Add(1)
	if main {
		mainGroup.Add(1)
	}
	if wait {
		endGroup.Add(1)
	}
	tasks = append(tasks, func() {
		defer logger.Debugf("%s ended", name)
		if main {
			defer mainGroup.Done()
		}
		if wait {
			defer endGroup.Done()
		}
		logger.Debugf("%s started", name)
		startGroup.Done()
		f()
	})
}

func AddMainTask(name string, f func()) {
	addTask(name, f, true, true)
}

func AddTask(name string, f func()) {
	addTask(name, f, false, true)
}

func AddBackgroundTask(name string, f func()) {
	addTask(name, f, false, false)
}

// SignalHandler initiates a shutdown when SIGINT or SIGTERM is received.
func SignalHandler() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case sig := <-sigChan:
			logger.Errorf("Received signal: %s", sig)
			Shutdown()
		case <-done:
			return
		}
	}
}

// Shutdown initiates a shutdown and waits for all goroutines to finish.
func Shutdown() {
	select {
	case <-done:
		return // Already closed
	default:
		logger.Notice("Shutting down")
		close(done)
	}

	ok := make(chan bool)
	go func() {
		endGroup.Wait()
		close(ok)
	}()

	select {
	case <-ok:
		logger.Notice("Shutdown complete")
	case <-time.After(2 * time.Second):
		logger.Critical("Forceful shutdown")
		StopLogging()
		os.Exit(1)
	}
}
