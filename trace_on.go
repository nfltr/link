// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Trace mode (go test -tags linktrace, LINKTRACE=<dir>) writes one JSON
// line per step of spec/Link.tla that a Conn takes, to
// <dir>/<pair>.ndjson, where pair is the dialer's local address: the two
// sides of one connection write the same file. Each event is written at
// the step's state change, under the lock that changed it, and under one
// process-wide lock, so a file's order is an order the two sides could
// have taken; spec/Trace.tla checks it against the model (make
// spec-traces). LINKTRACE_FILE instead puts every connection in that one
// file: for a process with one connection whose sides see different
// addresses (a proxy between them).
const traceOn = true

type traceState = *traceConn

type traceConn struct {
	side string // "d" (dialer) or "l"
	path string
}

// traceEvent is one line. Fields an event does not use are 0 or "".
//
//	Up        n: this side's window, credit: the peer's, held: 1 with FeatureChain
//	Open, Accept, GoAway, Leave, ConnClose, Down
//	Acquire   n: taken, credit: the session's credit after
//	SendData  n: bytes (one data frame)
//	Close     n: close code (3: an ABORT)
//	Read      n: bytes read, held: Credit.held after, grant: the window update decided (0 none)
//	Window    n: the update
//	Ping      n: 1 when its time is past goAwayAt
//	Pong      n: 1 when it replaced a pong still buffered
//	Recv      t: the frame (open, data, win, accept, close, goaway, ping, pong),
//	          n: its size, update or code; credit, held: after (credit -1: the session is unknown)
type traceEvent struct {
	ev, sid, t      string
	n, credit, held int
	grant           int
}

var (
	traceMu    sync.Mutex
	traceFiles = map[string]*os.File{}
)

// traceUp starts c's trace when LINKTRACE names a directory.
func traceUp(c *Conn) {
	path, dir := os.Getenv("LINKTRACE_FILE"), os.Getenv("LINKTRACE")
	if path == "" && dir == "" {
		return
	}
	side := "l"
	if c.dialer {
		side = "d"
	}
	if path == "" {
		pair := c.nc.RemoteAddr().String()
		if c.dialer {
			pair = c.nc.LocalAddr().String()
		}
		name := strings.NewReplacer(":", "_", "/", "_", "[", "", "]", "").Replace(pair)
		path = filepath.Join(dir, name+".ndjson")
	}
	c.credit.tr = &traceConn{side: side, path: path}
	chain := 0
	if c.chain() {
		chain = 1
	}
	traceEv(c.credit.tr, traceEvent{ev: "Up", n: c.credit.window, credit: c.credit.peerWindow, held: chain})
}

func traceEv(t *traceConn, e traceEvent) {
	if t == nil {
		return
	}
	b, err := json.Marshal(map[string]any{
		"ev": e.ev, "side": t.side, "sid": hex.EncodeToString([]byte(e.sid)), "t": e.t,
		"n": e.n, "credit": e.credit, "held": e.held, "grant": e.grant,
	})
	if err != nil {
		panic(err)
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	f := traceFiles[t.path]
	if f == nil {
		if f, err = os.OpenFile(t.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err != nil { // #nosec G304 -- the tester's LINKTRACE
			panic(err)
		}
		traceFiles[t.path] = f
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}
