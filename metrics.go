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
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

var (
	mRoot = metrics.NewRegistry()

	_ = GetOrRegisterUptime("uptime", mRoot)

	baseFormatter = BaseMetricFormatter{}
	byteFormatter = ScaledMetricFormatter{BaseMetricFormatter{"b"}, []string{"", "Ki", "Mi", "Gi"}}
	timeFormatter = ScaledMetricFormatter{BaseMetricFormatter{"s"}, []string{"n", "µ", "m", ""}}
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
	var (
		buf    bytes.Buffer
		names  []string
		values = make(map[string]string)
	)

	r.Each(func(n string, m interface{}) {
		var f MetricFormatter = baseFormatter
		if strings.HasSuffix(n, ".bytes") || n == "requests.size" {
			f = byteFormatter
		} else if strings.HasSuffix(n, ".time") || n == "uptime" {
			f = timeFormatter
		}

		spl, isSampled := m.(SampledMetric)
		if isSampled {
			s := spl.Sample()
			fmt.Fprintf(&buf, "\n\tsample (inputs, actual size): %d, %d", s.Count(), s.Size())
		}

		if cm, ok := m.(CounterMetric); !isSampled && ok {
			fmt.Fprintf(&buf, "\n\tcount: %s", f.FormatInt(cm.Count()))
		}
		if hm, ok := m.(HealthMetric); ok {
			fmt.Fprintf(&buf, "\n\terror: %s", hm.Error())
		}
		if gm, ok := m.(GaugeMetric); ok {
			fmt.Fprintf(&buf, "\n\tvalue: %s", f.FormatInt(gm.Value()))
		}
		if rm, ok := m.(RateMetric); ok && rm.Count() > 0 {
			rf := f
			if _, countEvents := m.(metrics.Timer); countEvents {
				rf = baseFormatter
			}
			fmt.Fprintf(&buf,
				"\n\trates (1m, 5m, 15m, overall): %s/s, %s/s, %s/s, %s/s",
				rf.FormatFloat(rm.Rate1()),
				rf.FormatFloat(rm.Rate5()),
				rf.FormatFloat(rm.Rate15()),
				rf.FormatFloat(rm.RateMean()))
		}
		if sm, ok := m.(StatMetric); ok && sm.Count() > 0 {
			fmt.Fprintf(&buf,
				"\n\tstats: sum=%s min=%s max=%s mean=%s stddev=%s",
				f.FormatInt(sm.Sum()),
				f.FormatInt(sm.Min()),
				f.FormatInt(sm.Max()),
				f.FormatFloat(sm.Mean()),
				f.FormatFloat(sm.StdDev()))
		}
		if pm, ok := m.(PercentileMetric); ok && pm.Count() > 0 {
			v := pm.Percentiles([]float64{0.50, 0.75, 0.95, 0.99})
			fmt.Fprintf(&buf,
				"\n\tpercentiles (50%%, 75%%, 95%%, 99%%): %s, %s, %s, %s",
				f.FormatFloat(v[0]),
				f.FormatFloat(v[1]),
				f.FormatFloat(v[2]),
				f.FormatFloat(v[3]))
		}
		names = append(names, n)
		values[n] = buf.String()
		buf.Reset()
	})

	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(w, "%s:%s\n", n, values[n])
	}
}

type Uptime struct {
	started time.Time
}

func NewUptime() metrics.Gauge {
	if metrics.UseNilMetrics {
		return metrics.NilGauge{}
	}
	return Uptime{time.Now()}
}

func GetOrRegisterUptime(name string, r metrics.Registry) metrics.Gauge {
	if nil == r {
		r = metrics.DefaultRegistry
	}
	return r.GetOrRegister(name, NewUptime).(metrics.Gauge)
}

func (u Uptime) Snapshot() metrics.Gauge {
	return metrics.GaugeSnapshot(u.Value())
}

func (u Uptime) Update(int64) {
	panic("Trying to update Uptime")
}

func (u Uptime) Value() int64 {
	return int64(time.Now().Sub(u.started))
}

// Formatter for metric values
type MetricFormatter interface {
	FormatFloat(float64) string
	FormatInt(int64) string
}

// BaseMetricFormatter is a formatter with an unit
type BaseMetricFormatter struct {
	Unit string
}

func (f BaseMetricFormatter) FormatFloat(v float64) string {
	return fmt.Sprintf("%.3f%s", v, f.Unit)
}

func (f BaseMetricFormatter) FormatInt(v int64) string {
	return fmt.Sprintf("%d%s", v, f.Unit)
}

// ScaledMetricFormatter is a formatter with an unit and scale prefixes
type ScaledMetricFormatter struct {
	BaseMetricFormatter
	Powers []string
}

func (f ScaledMetricFormatter) FormatFloat(v float64) string {
	i := 0
	for v > 10000.0 && i < len(f.Powers)-1 {
		v /= 1000.0
		i++
	}
	return fmt.Sprintf("%.3f%s%s", v, f.Powers[i], f.Unit)
}

func (f ScaledMetricFormatter) FormatInt(v int64) string {
	i := 0
	for v > 10000 && i < len(f.Powers)-1 {
		v /= 1000
		i++
	}
	return fmt.Sprintf("%d%s%s", v, f.Powers[i], f.Unit)
}

// Metric type signatures

type CounterMetric interface {
	Count() int64
}

type GaugeMetric interface {
	Value() int64
}

type StatMetric interface {
	CounterMetric
	Max() int64
	Mean() float64
	Min() int64
	StdDev() float64
	Sum() int64
	Variance() float64
}

type PercentileMetric interface {
	CounterMetric
	Percentile(float64) float64
	Percentiles([]float64) []float64
}

type RateMetric interface {
	CounterMetric
	Rate1() float64
	Rate5() float64
	Rate15() float64
	RateMean() float64
}

type HealthMetric interface {
	Error() error
}

type SampledMetric interface {
	Sample() metrics.Sample
}
