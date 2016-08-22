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
Metrics

bilies-go collects several metrics. They can be written to the log by sending an USR1 signal to the process.
*/
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	metrics "github.com/rcrowley/go-metrics"
)

var (
	mRoot = metrics.NewRegistry()
)

func init() {
	AddTask("Metric Handler", MetricDumper)
}

// MetricDumper dumps the metrics when receiving SIGUSR1 and on exit (in debug mode)
func MetricDumper() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGUSR1)
	defer close(sigChan)
	if debug {
		defer DumpMetrics(mRoot, os.Stderr)
	}

	var buf bytes.Buffer
	for {
		select {
		case <-sigChan:
			DumpMetrics(mRoot, &buf)
			for s := bufio.NewScanner(&buf); s.Scan(); {
				log.Notice(s.Text())
			}
			buf.Reset()
		case <-done:
			return
		}
	}
}

func DumpMetrics(r metrics.Registry, w io.Writer) {
	var buf bytes.Buffer
	r.Each(func(n string, m interface{}) {
		fmt.Fprintf(&buf, "%s:", n)
		if hm, ok := m.(HealthMetric); ok {
			fmt.Fprintf(&buf, " error=%s", hm.Error())
		}
		if gm, ok := m.(GaugeMetric); ok {
			fmt.Fprintf(&buf, " value=%d", gm.Value())
		}
		if cm, ok := m.(CounterMetric); ok {
			fmt.Fprintf(&buf, " count=%d", cm.Count())
		}
		if rm, ok := m.(RateMetric); ok {
			fmt.Fprintf(&buf, " rate1=%g rate5=%g rate15=%g rateMean=%g", rm.Rate1(), rm.Rate5(), rm.Rate15(), rm.RateMean())
		}
		if sm, ok := m.(StatMetric); ok {
			fmt.Fprintf(&buf, " sum=%d min=%d max=%d mean=%g var=%g stddev=%g", sm.Sum(), sm.Min(), sm.Max(), sm.Mean(), sm.Variance(), sm.StdDev())
		}
		if pm, ok := m.(PercentileMetric); ok {
			v := pm.Percentiles([]float64{0.50, 0.75, 0.95, 0.99})
			fmt.Fprintf(&buf, " median=%g 75%%=%g 95%%=%g 99%%=%g", v[0], v[1], v[2], v[3])
		}
		buf.Write([]byte("\n"))
		buf.WriteTo(w)
		buf.Reset()
	})
}

type CounterMetric interface {
	Count() int64
}

type GaugeMetric interface {
	Value() int64
}

type StatMetric interface {
	Max() int64
	Mean() float64
	Min() int64
	StdDev() float64
	Sum() int64
	Variance() float64
}

type PercentileMetric interface {
	Percentile(float64) float64
	Percentiles([]float64) []float64
}

type RateMetric interface {
	Rate1() float64
	Rate5() float64
	Rate15() float64
	RateMean() float64
}

type HealthMetric interface {
	Error() error
}
