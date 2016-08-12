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
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	logging "github.com/op/go-logging"
	"github.com/spf13/pflag"
)

var (
	log         = logging.MustGetLogger("github.com/Adirelle/bilies-go")
	logWriter   = NewLoggerWriter(log)
	logFile     string
	asyncWriter AsyncWriter

	logBufferPool sync.Pool

	debug bool
)

func init() {
	pflag.BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
	pflag.BoolP("verbose", "v", false, "Enable verbose logging")
	pflag.StringVar(&logFile, "log-file", "", "Write the logs into the file")
}

// StartLogging setups logging and starts the asynchronous logger.
func SetupLogging() {

	logFormat := "%{time:2006-01-02T15:04:05.000} %{level:.3s}: %{message}"
	if debug {
		logFormat += " (%{shortfile} %{shortfunc})"
	}
	logging.SetFormatter(logging.MustStringFormatter(logFormat))

	logDest := os.Stderr
	if logFile != "" {
		if f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_SYNC, 0640); err == nil {
			logDest = f
		} else {
			log.Fatalf("Cannot open logfile %q: %s", logFile, err)
		}
	}

	stderrBackend := logging.AddModuleLevel(logging.NewLogBackend(os.Stderr, "", 0))
	logging.SetBackend(logging.NewLogBackend(NewAsyncWriter(logDest), "", 0), stderrBackend)

	logLevel := logging.NOTICE
	if debug {
		logLevel = logging.DEBUG
	} else if verbose, err := pflag.CommandLine.GetBool("verbose"); err == nil && verbose {
		logLevel = logging.INFO
	}
	logging.SetLevel(logLevel, "")
	stderrBackend.SetLevel(logging.CRITICAL, "")

	log.Noticef("Log settings: file=%s, level=%s, debug=%t", logDest.Name(), logging.GetLevel(""), debug)
}

// StopLogging stops the asynchronous logger.
func StopLogging() {
	asyncWriter.Close()
}

// AsyncWriter is an empty struct that implements io.Writer
type AsyncWriter struct {
	underlying io.Writer
	input      chan []byte
	done       sync.WaitGroup
}

// NewAsyncWriter creates a new asynchronous writter for the specified writer.
func NewAsyncWriter(w io.Writer) io.WriteCloser {
	aw := AsyncWriter{w, make(chan []byte, 5), sync.WaitGroup{}}
	go aw.process()
	return &aw
}

func (w *AsyncWriter) process() {
	if c, ok := w.underlying.(io.Closer); ok {
		defer c.Close()
	}
	defer w.done.Done()
	w.done.Add(1)
	for buf := range w.input {
		if _, err := w.underlying.Write(buf); err != nil {
			fmt.Fprintf(os.Stderr, "Cannot write to file: %s\n", err)
		}
		logBufferPool.Put(buf[:0])
	}
}

// Write sends a copy of the buffer to the goroutines.
func (w *AsyncWriter) Write(buf []byte) (int, error) {
	l := len(buf)
	var logBuf []byte
	if pooled := logBufferPool.Get(); pooled != nil {
		logBuf = append(pooled.([]byte), buf...)
	} else {
		logBuf = make([]byte, l)
		copy(logBuf, buf)
	}
	w.input <- logBuf
	return l, nil
}

// Close stops the processing goroutines by closing the channel and waits for its completion.
func (w *AsyncWriter) Close() (err error) {
	if w.input != nil {
		close(w.input)
		w.done.Wait()
		w.input = nil
	}
	return
}

// LoggerWriter is an empty struct that implements io.Writer
type LoggerWriter struct {
	logger *logging.Logger
	buffer []byte
}

// NewLoggerWriter creates a Writer for the given logger.
func NewLoggerWriter(logger *logging.Logger) *LoggerWriter {
	return &LoggerWriter{logger: logger}
}

// Write splits the incoming data in lines and pass them to the logger
func (w *LoggerWriter) Write(data []byte) (n int, err error) {
	n = len(data)
	if !w.logger.IsEnabledFor(logging.INFO) {
		return
	}
	lines := bytes.Split(append(w.buffer, data...), []byte("\n"))
	last := len(lines) - 1
	for i := 0; i < last; i++ {
		w.logger.Info(trimRightSpaces(lines[i]))
	}
	w.buffer = lines[last]
	return
}

func (w *LoggerWriter) Close() {
	w.logger.Info(trimRightSpaces(w.buffer))
}

func trimRightSpaces(buf []byte) string {
	return string(bytes.TrimRight(buf, "\r\n\t "))
}
