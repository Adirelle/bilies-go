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
	"strings"
	"sync"

	logging "github.com/op/go-logging"
	"github.com/spf13/pflag"
)

var (
	log       = logging.MustGetLogger("github.com/Adirelle/bilies-go")
	logWriter LoggerWriter
	logFile   string
	logChan   = make(chan []byte, 5)
	logDest   = os.Stderr

	logBufferPool = sync.Pool{New: NewLogBuffer}

	debug bool
)

func init() {
	pflag.BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
	pflag.BoolP("verbose", "v", false, "Enable verbose logging")
	pflag.StringVar(&logFile, "log-file", "", "Write the logs into the file")
}

// StartLogging setups logging and starts the asynchronous logger.
func StartLogging() {
	logging.SetBackend(logging.NewLogBackend(AsyncWriter{}, "", 0))
	StartAndForget("Async logger", AsyncLogger)

	logFormat := "%{time} %{level}: %{message}"
	if debug {
		logFormat = "%{time} %{level}: %{message} (%{shortfile})"
	}
	logging.SetFormatter(logging.MustStringFormatter(logFormat))

	logLevel := logging.NOTICE
	if debug {
		logLevel = logging.DEBUG
	} else if verbose, err := pflag.CommandLine.GetBool("verbose"); err == nil && verbose {
		logLevel = logging.INFO
	}
	logging.SetLevel(logLevel, log.Module)

	if logFile != "" {
		var err error
		if logDest, err = os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_SYNC, 0640); err != nil {
			logDest = os.Stderr
			log.Panicf("Cannot open logfile %q: %s", logFile, err)
		}
	}

	log.Noticef("Log settings: file=%s, level=%s", logDest.Name(), logging.GetLevel(log.Module))
}

// StopLogging stops the asynchronous logger.
func StopLogging() {
	close(logChan)
}

// NewLogBuffer allocates a new buffer for the buffer pool.
func NewLogBuffer() interface{} {
	return make([]byte, 1024)
}

// AsyncLogger writes logs from
func AsyncLogger() {
	defer logDest.Close()
	for buf := range logChan {
		logDest.Write(buf)
		logBufferPool.Put(buf)
	}
}

// AsyncWriter is an empty struct that implements io.Writer
type AsyncWriter struct{}

// Write gets a buffer from the buffer pool, copy the input buffer into it and send it to the asynchronous logger.
func (w AsyncWriter) Write(buf []byte) (int, error) {
	l := len(buf)
	logBuf := logBufferPool.Get().([]byte)
	if cap(logBuf) < l {
		logBuf = make([]byte, l)
	} else {
		logBuf = logBuf[:l]
	}
	copy(logBuf, buf)
	logChan <- logBuf
	return l, nil
}

// LoggerWriter is an empty struct that implements io.Writer
type LoggerWriter struct{}

// Write splits the incoming data in lines and pass them to the logger
func (w LoggerWriter) Write(p []byte) (int, error) {
	for _, s := range strings.Split(string(p), "\n") {
		s2 := strings.TrimRight(s, " \n")
		if s2 != "" {
			log.Info(s2)
		}
	}
	return len(p), nil
}
