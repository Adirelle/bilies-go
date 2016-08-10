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
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

var mRoot = metrics.NewPrefixedRegistry("bilies.")

func MetricPoller() {
	t := time.NewTicker(1 * time.Second)
	if debug {
		defer DumpMetrics()
	}
	defer t.Stop()

	s := metrics.NewUniformSample(1e5)
	metrics.GetOrRegisterHistogram("queue.length", mRoot, s)
	for {
		select {
		case <-t.C:
			s.Update(int64(queue.Length()))
		case <-done:
			return
		}
	}
}

func DumpMetrics() {
	metrics.WriteOnce(mRoot, logWriter)
}

func StartMetrics() {
	StartAndForget("Metric poller", MetricPoller)
}
