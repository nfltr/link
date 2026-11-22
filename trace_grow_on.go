// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package link

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// traceFixedWindows: a recorder for a model of fixed windows runs (any
// LINKTRACE* variable but LINKTRACE_GROW: Trace.tla checks Link.tla's,
// TraceCreditWaiters.tla a grant of what the waiters took, ...), so this
// side announces no FeatureGrow (Config.announce).
var traceFixedWindows = sync.OnceValue(func() bool {
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "LINKTRACE") && !strings.HasPrefix(kv, "LINKTRACE_GROW=") {
			return true
		}
	}
	return false
})

// Growth trace (go test -tags linktrace, LINKTRACE_GROW=<dir>): one JSON
// line per step of spec/CreditGrow.tla that a Conn's Credit takes, to
// <dir>/<pair>.grow-<r>.ndjson, one file per receiving side r of a
// connection (pair as tracePair): its sessions' data toward r and the
// window updates back, both sides writing it, each line under the Credit's
// lock that changed the state and the process-wide trace lock; the frame a
// step sends goes out after its line, the frame a step reads came before
// it. spec/TraceCreditGrow.tla checks it (make spec-traces). Fields a line
// does not use are 0 or "":
//
//	Up       by r: n Config.Window, max WindowMax, g WindowBudget, grow 1 with FeatureGrow
//	Open     either side: the session's credit starts (the sender's credit cr)
//	Send     the sender buffers a data frame of n bytes
//	Block    the sender's BLOCKED (cr its credit)
//	Data     r charges a data frame of n bytes (known 0: a session it forgot), held after
//	Blocked  r reads a BLOCKED: g the growth (0 none), win and grown after
//	Read     r's reader took n bytes: grant the window update decided (0 none), held after
//	Win      the sender reads a window update of n bytes (known 0: forgotten), cr after
//	Drop     either side: the session's credit ends
//
// The sender's credit cr is the model's cred: what it may still buffer,
// a Write's acquired bytes not yet in a frame counted in (Credit's n +
// pend - growSess.buf).
type growTrace struct {
	side       string
	recv, send string // the files of the sessions this side receives, and sends
}

// growSess is a session's bytes logged as Send that Credit.sent has not
// yet taken off pend (under Credit.mu).
type growSess struct{ buf int }

type growLine struct {
	Ev    string `json:"ev"`
	Side  string `json:"side"`
	Sid   string `json:"sid"`
	N     int    `json:"n"`
	Cr    int    `json:"cr"`
	Held  int    `json:"held"`
	G     int    `json:"g"`
	Win   int    `json:"win"`
	Grown int    `json:"grown"`
	Max   int    `json:"max"`
	Grow  int    `json:"grow"`
	Known int    `json:"known"`
	Grant int    `json:"grant"`
}

// traceGrowUp starts c's growth trace when LINKTRACE_GROW names a
// directory; after c.credit.grows is set.
func traceGrowUp(c *Conn) {
	dir := os.Getenv("LINKTRACE_GROW")
	if dir == "" {
		return
	}
	side, peer := "l", "d"
	if c.dialer {
		side, peer = "d", "l"
	}
	base := filepath.Join(dir, tracePair(c.nc.LocalAddr(), c.nc.RemoteAddr(), c.dialer)+".grow-")
	cr := c.credit
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.gt = growTrace{side: side, recv: base + side + ".ndjson", send: base + peer + ".ndjson"}
	grow := 0
	if cr.grows {
		grow = 1
	}
	cr.growLog(true, growLine{Ev: "Up", N: cr.window, Max: cr.winMax, G: cr.budget, Grow: grow})
}

// growLog writes l to the receiving (recv) or sending side's file, under c.mu.
func (c *Credit) growLog(recv bool, l growLine) {
	if c.gt.side == "" {
		return
	}
	l.Side, l.Sid = c.gt.side, hex.EncodeToString([]byte(l.Sid))
	path := c.gt.send
	if recv {
		path = c.gt.recv
	}
	traceWrite(path, map[string]any{
		"ev": l.Ev, "side": l.Side, "sid": l.Sid, "n": l.N, "cr": l.Cr, "held": l.Held, "g": l.G,
		"win": l.Win, "grown": l.Grown, "max": l.Max, "grow": l.Grow, "known": l.Known, "grant": l.Grant,
	})
}

func (s *sessionCredit) growCred() int { return s.n + s.pend - s.gs.buf }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (c *Credit) traceGrowOpen(sid string, s *sessionCredit) {
	c.growLog(true, growLine{Ev: "Open", Sid: sid})
	c.growLog(false, growLine{Ev: "Open", Sid: sid, Cr: s.growCred()})
}

func (c *Credit) traceGrowDrop(sid string) {
	c.growLog(true, growLine{Ev: "Drop", Sid: sid})
	c.growLog(false, growLine{Ev: "Drop", Sid: sid})
}

func (c *Credit) traceGrowSend(sid string, s *sessionCredit, n int) {
	s.gs.buf += n
	c.growLog(false, growLine{Ev: "Send", Sid: sid, N: n})
}

func (s *sessionCredit) traceGrowSent(n int) { s.gs.buf -= n }

func (c *Credit) traceGrowBlock(sid string, s *sessionCredit) {
	c.growLog(false, growLine{Ev: "Block", Sid: sid, Cr: s.growCred()})
}

func (c *Credit) traceGrowData(sid string, s *sessionCredit, n int) {
	l := growLine{Ev: "Data", Sid: sid, N: n, Known: b2i(s != nil)}
	if s != nil {
		l.Held = s.held
	}
	c.growLog(true, l)
}

func (c *Credit) traceGrowBlocked(sid string, s *sessionCredit, g int) {
	l := growLine{Ev: "Blocked", Sid: sid, G: g, Known: b2i(s != nil), Grown: c.grown}
	if s != nil {
		l.Win = s.win
	}
	c.growLog(true, l)
}

func (c *Credit) traceGrowRead(sid string, s *sessionCredit, n, grant int) {
	c.growLog(true, growLine{Ev: "Read", Sid: sid, N: n, Held: s.held, Grant: grant})
}

func (c *Credit) traceGrowWin(sid string, s *sessionCredit, n int) {
	l := growLine{Ev: "Win", Sid: sid, N: n, Known: b2i(s != nil)}
	if s != nil {
		l.Cr = s.growCred()
	}
	c.growLog(false, l)
}
