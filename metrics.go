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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
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

	for {
		select {
		case <-sigChan:
			/*
				logWriter := NewFuncWriter(log.Notice)
				defer logWriter.Close()
				DumpMetrics(mRoot, logWriter)
			*/
			DumpMetrics(mRoot, os.Stderr)
		case <-done:
			return
		}
	}
}

func DumpMetrics(r metrics.Registry, w io.Writer) {
	root := make(map[string]interface{})

	r.Each(func(n string, m interface{}) {
		d := make(map[string]interface{})
		if hm, ok := m.(HealthMetric); ok {
			d["error"] = hm.Error()
		}
		if gm, ok := m.(GaugeMetric); ok {
			d["value"] = gm.Value()
		}
		if cm, ok := m.(CounterMetric); ok {
			d["count"] = cm.Count()
		}
		if rm, ok := m.(RateMetric); ok {
			d["rate"] = map[string]float64{"1": rm.Rate1(), "5": rm.Rate5(), "15": rm.Rate15(), "mean": rm.RateMean()}
		}
		if sm, ok := m.(StatMetric); ok {
			d["sum"] = sm.Sum()
			d["min"] = sm.Min()
			d["max"] = sm.Max()
			d["mean"] = sm.Mean()
			d["variance"] = sm.Variance()
			d["stddev"] = sm.StdDev()
		}
		if pm, ok := m.(PercentileMetric); ok {
			v := pm.Percentiles([]float64{0.50, 0.75, 0.95, 0.99})
			d["percentiles"] = map[string]float64{"0.5": v[0], "0.75": v[1], "0.95": v[2], "0.99": v[3]}
		}
		if len(d) == 0 {
			return
		}
		root = putHiearchicalData(root, n, d)
	})

	fmt.Fprintf(w, "%+v", root)
	if repr, err := json.MarshalIndent(root, " ", "  "); err == nil {
		w.Write(repr)
	} else {
		w.Write([]byte(err.Error()))
	}
}

func putHiearchicalData(root map[string]interface{}, key string, value interface{}) map[string]interface{} {
	return doPutHiearchicalData(root, strings.Split(key, "."), value)
}

func doPutHiearchicalData(dict map[string]interface{}, keys []string, value interface{}) map[string]interface{} {
	if len(keys) == 1 {
		dict[keys[0]] = value
		return dict
	}
	key := keys[0]
	tail := keys[1:]
	if next, exists := dict[key]; exists {
		sub := next.(map[string]interface{})
		dict[key] = doPutHiearchicalData(sub, tail, value)
	} else {
		dict[key] = doPutHiearchicalData(make(map[string]interface{}), tail, value)
	}
	return dict
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
