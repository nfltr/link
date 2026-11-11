// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package pathswitch

import (
	"encoding/json"
	"os"
	"sync"
)

// Trace mode (go test -tags linktrace, LINKTRACE=<dir>): a session's two
// Conns write one JSON line per step of spec/DirectPath.tla they take to
// one file (tracePair), under the step's lock (c.mu) and one process-wide
// lock, so the file's order is an order the two ends could have taken;
// spec/TraceDirectPath.tla checks it per direction (make spec-traces).
// A step on k bytes is k lines (the model's frames are bytes).
//
//	Init      n: the window W (the first line)
//	Write     by: the writing end, per byte handed to it
//	Send      by, e: the stream, per byte sent; SendEnd: the END
//	Resume    the client took the server's RESUME on e; n: its count
//	TakeAck   by, e, n: the count
//	Recv      by, e, per byte delivered; RecvEnd: the END
//	Ack       by, e, n: the count sent
//	Open      e, x: the path: the client opened stream e (Move)
//	Attach    e, n: the client's count in the OPEN, x: "abort" if the OPEN carried the reset
//	Abort     by: the end resetting the session
//	TakeAbort by, e: the end taking the peer's ABORT
//
// The test's paths write the network's steps (traceEnv): Offer (x: the
// holder), Verify, Refuse, Die (x: the path), Revive.
type traceState = *traceFile

type traceFile struct{ f *os.File }

var traceMu sync.Mutex

// tracePair starts the trace of the session a (client) and b (server) to
// <LINKTRACE>/<name>.ndjson; nothing without LINKTRACE.
func tracePair(name string, a, b *Conn) {
	dir := os.Getenv("LINKTRACE")
	if dir == "" {
		return
	}
	f, err := os.Create(dir + "/" + name + ".ndjson")
	if err != nil {
		panic(err)
	}
	t := &traceFile{f: f}
	t.write(traceLine{Ev: "Init", N: a.w})
	for _, c := range []*Conn{a, b} {
		c.mu.Lock()
		c.tr = t
		c.mu.Unlock()
	}
}

type traceLine struct {
	Ev string `json:"ev"`
	By string `json:"by"`
	E  uint64 `json:"e"`
	N  uint64 `json:"n"`
	X  string `json:"x"`
}

func (t *traceFile) write(l traceLine) {
	b, _ := json.Marshal(l)
	traceMu.Lock()
	defer traceMu.Unlock()
	_, _ = t.f.Write(append(b, '\n'))
}

func (c *Conn) trace(ev, by string, e, n uint64, x string) {
	if c.tr != nil {
		c.tr.write(traceLine{ev, by, e, n, x})
	}
}

func (c *Conn) traceN(ev, by string, e, k uint64) {
	for range k {
		c.trace(ev, by, e, 0, "")
	}
}

// traceEnv writes a network step of the session's trace.
func traceEnv(c *Conn, ev, x string) { c.trace(ev, "", 0, 0, x) }
