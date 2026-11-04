// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

type poolTraceState struct{}

func tracePool(*Pool)                                   {}
func tracePoolOpen(*Pool) int                           { return 0 }
func tracePoolEv(*Pool, string, string, int, *Mux, int) {}

func (p *Pool) traceEv(string, int, *Mux, int) {}
