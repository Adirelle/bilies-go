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

Backend pool

Several hosts can be passed on the command line to create a backend pool.

When a network error occurs while tryng to reach a backend, it is temporarily
removed from the pool, using a delay which exponentially increases on consecutive
errors. This delay starts at 500ms and is capped at 2 minutes.

*/
package main

import (
	"fmt"
	"math"
	"time"
)

var (
	backoffBaseDelay = float64(500 * time.Millisecond)
	backoffMaxDelay  = 2 * time.Minute
	backoffFactor    = 2.0
)

// BackendURL is a string containing the URL of a backend.
type BackendURL struct {
	url      string
	failures int
	pool     *BackendURLPool
}

// BackendURLPool manages a collection of backend URLs.
type BackendURLPool struct {
	urls chan *BackendURL
}

// NewBackendURLPool creates a new backend pool for the given hosts.
func NewBackendURLPool(hosts []string, protocol string, port int) BackendURLPool {
	b := BackendURLPool{make(chan *BackendURL, len(hosts))}
	for _, host := range hosts {
		url := fmt.Sprintf("%s://%s:%d/_bulk", protocol, host, port)
		b.urls <- &BackendURL{url, 0, &b}
	}
	return b
}

// Get fetchs a backend from the pool.
func (p *BackendURLPool) Get() <-chan *BackendURL {
	return p.urls
}

// String returns the Backend URL as a string
func (u *BackendURL) String() string {
	return u.url
}

// Release releases the backend to the pool.
func (u *BackendURL) Release(hasFailed bool) {
	if hasFailed {
		u.releaseWithBackoff()
	} else {
		u.releaseDirectly()
	}
}

// Release releases the backend to the pool.
func (u *BackendURL) releaseDirectly() {
	if u.failures > 0 {
		u.failures = 0
		logger.Infof("%q is working again", u)
	}
	u.pool.urls <- u
	logger.Debugf("%q released", u)
}

// Release releases the backend to the pool.
func (u *BackendURL) releaseWithBackoff() {
	u.failures++
	duration := BackoffDelay(u.failures)
	logger.Noticef("%d consecutive error(s) with %q, ignoring for %s", u.failures, u, duration)
	time.AfterFunc(duration, func() {
		u.pool.urls <- u
		logger.Debugf("%q is available again", u)
	})
}

// BackoffDelay calculates a backoff delay, given a number of consecutive failures.
func BackoffDelay(n int) time.Duration {
	d := time.Duration(backoffBaseDelay * math.Pow(backoffFactor, float64(n-1)))
	if d > backoffMaxDelay {
		return backoffMaxDelay
	}
	return d
}
