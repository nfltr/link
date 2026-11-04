// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// A Pool's trace (LINKTRACE=<dir>): one JSON line per step of
// spec/Pool.tla the Pool takes, to <dir>/pool-<n>.ndjson, written under
// p.mu at the step's state change (spec/TracePool.tla checks it, make
// spec-traces). o numbers the Pool's Opens, c its connections (from 1, in
// the order they were dialled):
//
//	Pool      n: Min, c: Max
//	Start     an Open begins (o)
//	Pick      t: closed | place (c) | dial | full | wait; grow: a background growth dial (unmodelled)
//	Place     n: 1 when OpenChain on c opened the session, 0 when it sends the Open back to pick
//	OpenErr   Open returned another error (unmodelled)
//	End       the session Open o placed on c ended (its Done)
//	MaintainWait, MaintainDial: maintain's check waits on p.changed or reserves a dial
//	Dial      a dial ended: c the new connection (n: the sessions it takes), 0 when it failed
//	PeerGoAway the Pool heard the peer's GOAWAY on c
//	Down      c closed (Serve returned), Remove: it left the Pool
//	DrainStart n: the replacements it dials (the first reserved with it)
//	GoAway    Drain sent GOAWAY on its old connections or its replacements
//	CloseStart, CloseEnd
type poolTraceState = *poolTrace

type poolTrace struct {
	path  string
	opens atomic.Int32
	ids   map[*Mux]int // under p.mu
}

var tracePools atomic.Int32

func tracePool(p *Pool) {
	dir := os.Getenv("LINKTRACE")
	if dir == "" {
		return
	}
	p.tr = &poolTrace{path: filepath.Join(dir, fmt.Sprintf("pool-%d.ndjson", tracePools.Add(1))), ids: map[*Mux]int{}}
	tracePoolEv(p, "Pool", "", 0, nil, p.cfg.Min)
}

// tracePoolOpen numbers an Open (0 when p is not traced).
func tracePoolOpen(p *Pool) int {
	if p.tr == nil {
		return 0
	}
	return int(p.tr.opens.Add(1))
}

// tracePoolEv writes one step; m, when not nil, is numbered on first sight.
func tracePoolEv(p *Pool, ev, t string, o int, m *Mux, n int) {
	tr := p.tr
	if tr == nil {
		return
	}
	c := 0
	if m != nil {
		if c = tr.ids[m]; c == 0 {
			c = len(tr.ids) + 1
			tr.ids[m] = c
		}
	}
	if ev == "Pool" {
		c = p.cfg.Max
	}
	traceWrite(tr.path, map[string]any{"ev": ev, "t": t, "o": o, "c": c, "n": n})
}

// traceEv writes one step under p.mu.
func (p *Pool) traceEv(ev string, o int, m *Mux, n int) {
	p.mu.Lock()
	tracePoolEv(p, ev, "", o, m, n)
	p.mu.Unlock()
}
