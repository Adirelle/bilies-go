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
	tasks      []func()
	startGroup = sync.WaitGroup{}
	mainGroup  = sync.WaitGroup{}
	endGroup   = sync.WaitGroup{}
	done       = make(chan bool)

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

	AddBackgroundTask("Signal handler", SignalHandler)
}

func main() {
	pflag.Parse()

	SetupLogging()
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

	StartTasks()

	mainGroup.Wait()

	Shutdown()
}

// SetupPidFile creates the PID file and writes the current PID into it
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

// Start starts f as a goroutine, and registers it in the starting and ending groups.
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
		defer log.Debugf("%s ended", name)
		if main {
			defer mainGroup.Done()
		}
		if wait {
			defer endGroup.Done()
		}
		log.Debugf("%s started", name)
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
			log.Errorf("Received signal: %s", sig)
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
		log.Critical("Forceful shutdown")
		StopLogging()
		os.Exit(1)
	}
}
