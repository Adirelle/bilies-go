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
package indexer

import (
	"testing"

	"github.com/op/go-logging"
	"gopkg.in/check.v1"
)

func TestAll(t *testing.T) {
	logging.SetFormatter(logging.MustStringFormatter("%{shortfile} %{level} %{message}"))
	check.TestingT(t)
}

type baseSuite struct{}

func (_ *baseSuite) SetUpTest(c *check.C) {
	logging.SetBackend(testLogger{C: c})
}

type testLogger struct {
	*check.C
}

func (l testLogger) Log(level logging.Level, calldepth int, rec *logging.Record) error {
	l.C.Log(rec.Formatted(calldepth + 2))
	return nil
}
