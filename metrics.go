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
	"syscall"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

var mRoot = metrics.NewPrefixedRegistry("bilies.")

// StartMetrics simply starts MetricPoller.
func StartMetrics() {
	Start("Metric poller", MetricPoller)
}

// DumpMetrics writes a snapshot of all metrics into the log.
func DumpMetrics() {
	metrics.WriteOnce(mRoot, logWriter)
}

// MetricPoller polls the queue length at regular intervals and dumps the metrics when receiving SIGUSR1
func MetricPoller() {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	if debug {
		defer DumpMetrics()
	}

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGUSR1)
	defer close(sigChan)

	s := metrics.NewUniformSample(1e5)
	metrics.GetOrRegisterHistogram("queue.length", mRoot, s)

	for {
		select {
		case <-t.C:
			s.Update(int64(queue.Length()))
		case <-sigChan:
			DumpMetrics()
		case <-done:
			return
		}
	}
}
