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
func (*Session) traceDrain()                       {}

// pumpedRead is Read's look at s.pump, under rmu.
func (s *Session) pumpedRead() bool { return s.pump.Load() != nil }

// setPump is Pump's: the copy claim taken alone, then s.pump set.
func (s *Session) setPump(p *pump) bool {
	if !s.copiers.CompareAndSwap(0, -1) {
		return false
	}
	s.pump.Store(p)
	return true
}

// claimWriteTo and unclaimCopy are WriteTo's take and release of the
// copy claim.
func (s *Session) claimWriteTo() bool { return s.tryClaimWriteTo() }
func (s *Session) unclaimCopy()       { s.copiers.Add(-1) }
