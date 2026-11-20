// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

// The relayed close trace is trace_relayed_on.go's.
func traceRelayed(string, *Session, *Session, *Session, *Session) {}
func traceRelayedOff()                                            {}
func traceRC(*Mux, string, string, string, ...string)             {}
func traceRCEnv(string)                                           {}
func traceRCFrame(*Mux, rxFrame)                                  {}
func traceRCClose(*Session)                                       {}
func traceRCRet(*Session, error)                                  {}
