// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

// writeTrace, writeStep and creditTrace are the write trace's
// (trace_write_on.go); no size here.
type (
	writeTrace  struct{}
	writeStep   struct{}
	creditTrace struct{}
)

func traceWriteUp(*Session)                            {}
func (*Session) writeStepUp() writeStep                { return writeStep{} }
func (*Session) writeSet(bool, bool)                   {}
func (*Session) writeFire()                            {}
func (*Session) traceTake(writeStep)                   {}
func (*Session) writeNewCh()                           {}
func (s *Session) writePast(writeStep) bool            { return s.wd.past.Load() }
func (*sessionCredit) traceCheck(writeStep, int, bool) {}
func (writeStep) traceWake()                           {}
func (*sessionCredit) traceGrant(int)                  {}
