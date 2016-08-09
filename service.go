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
	"runtime/debug"
	"sync"
)

type service interface {
	Init()
	Continue() bool
	Iterate()
	Cleanup()
	String() string
}

type interruptableService interface {
	Interrupt()
}

type supervisor interface {
	Start()
	Wait()
	IsRunning() bool
	Interrupt()
}

type baseSupervisor struct {
	svc         service
	running     bool
	interrupted bool
	syncChan    chan bool
	mu          sync.RWMutex
}

func newSupervisor(svc service) supervisor {
	return &baseSupervisor{svc: svc, syncChan: make(chan bool)}
}

func (s *baseSupervisor) Start() {
	log.Infof("Starting %s", s.svc)
	go s.run()
	<-s.syncChan
}

func (s *baseSupervisor) Wait() {
	log.Infof("Waiting end of %s", s.svc)
	<-s.syncChan
}

func (s *baseSupervisor) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running && !s.interrupted
}

func (s *baseSupervisor) Interrupt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.interrupted {
		log.Infof("Interrupting %s", s.svc)
		s.interrupted = true
		if svc, ok := s.svc.(interruptableService); ok {
			svc.Interrupt()
		}
	}
}

func (s *baseSupervisor) run() {
	s.svc.Init()

	defer func() {
		s.svc.Cleanup()
		log.Infof("%s stopped", s.svc)
		s.syncChan <- false
	}()

	log.Infof("%s started", s.svc)
	s.setRunning(true)
	s.syncChan <- true

	for !s.isInterrupted() && s.svc.Continue() {
		s.iterate()
	}

	s.setRunning(false)
}

func (s *baseSupervisor) iterate() {
	defer func() {
		if err := recover(); err != nil {
			s.recover(err, debug.Stack())
		}
	}()
	s.svc.Iterate()
}

func (s *baseSupervisor) recover(err interface{}, stack []byte) {
	log.Errorf("%s panicked: %v, stack trace:", s.svc, err)
	for _, l := range bytes.Split(stack, []byte("\n"))[7:] {
		log.Error(string(l))
	}
}

func (s *baseSupervisor) setRunning(b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = b
}

func (s *baseSupervisor) isInterrupted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.interrupted
}

type multiSupervisor struct {
	spv []supervisor
}

func newMultiSupervisor() *multiSupervisor {
	return &multiSupervisor{}
}

func (m *multiSupervisor) Add(svc service) supervisor {
	spv := newSupervisor(svc)
	m.spv = append(m.spv, spv)
	return spv
}

func (m *multiSupervisor) each(f func(supervisor)) {
	for _, spv := range m.spv {
		f(spv)
	}
}

func (m *multiSupervisor) any(f func(supervisor) bool) bool {
	for _, spv := range m.spv {
		if f(spv) {
			return true
		}
	}
	return false
}

func (m *multiSupervisor) Start() {
	log.Debug("Starting services")
	m.each(func(s supervisor) { s.Start() })
	log.Debug("Services started")
}

func (m *multiSupervisor) Wait() {
	m.each(func(s supervisor) { s.Wait() })
}

func (m *multiSupervisor) Interrupt() {
	log.Debug("Interrupting services")
	m.each(func(s supervisor) { s.Interrupt() })
}

func (m *multiSupervisor) IsRunning() bool {
	return m.any(func(s supervisor) bool { return s.IsRunning() })
}
