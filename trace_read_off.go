// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

// readTrace is a Session's read trace (trace_read_on.go); no size here.
type readTrace struct{}

func traceReadUp(*Session)                         {}
func (*Session) traceRead(string, bool, int, bool) {}
func (*Session) traceReadEnd(error)                {}
func (s *Session) setDLPast(v bool)                { s.dlPast.Store(v) }
