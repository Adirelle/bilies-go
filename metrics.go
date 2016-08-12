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

	metrics "github.com/rcrowley/go-metrics"
)

var mRoot = metrics.NewPrefixedRegistry("bilies.")

func init() {
	AddTask("Metric Handler", MetricDumper)
}

// MetricDumper dumps the metrics when receiving SIGUSR1 and on exit (in debug mode)
func MetricDumper() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGUSR1)
	defer close(sigChan)
	if debug {
		defer metrics.WriteOnce(mRoot, os.Stderr)
	}

	for {
		select {
		case <-sigChan:
			metrics.WriteOnce(mRoot, logWriter)
		case <-done:
			return
		}
	}
}
