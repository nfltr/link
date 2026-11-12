// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

var errKilled = errors.New("memstream: path died")

// pipe is one direction of an in-memory stream: FIFO bytes, a FIN, a
// break (the path died or the stream was reset), a stall (bytes held).
type pipe struct {
	mu      sync.Mutex
	cond    *sync.Cond
	buf     []byte
	fin     bool
	broken  bool
	stalled bool
}

func newPipe() *pipe { p := &pipe{}; p.cond = sync.NewCond(&p.mu); return p }

func (p *pipe) read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.broken && (p.stalled || len(p.buf) == 0) && !(p.fin && len(p.buf) == 0) {
		p.cond.Wait()
	}
	if p.broken {
		return 0, errKilled
	}
	if len(p.buf) == 0 {
		return 0, io.EOF
	}
	n := copy(b, p.buf)
	p.buf = p.buf[n:]
	return n, nil
}

func (p *pipe) write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.broken || p.fin {
		return 0, errKilled
	}
	p.buf = append(p.buf, b...)
	p.cond.Broadcast()
	return len(b), nil
}

func (p *pipe) set(f func(p *pipe)) { p.mu.Lock(); f(p); p.cond.Broadcast(); p.mu.Unlock() }

// memStream is one end of an in-memory Stream.
type memStream struct{ r, w *pipe }

func memPair() (*memStream, *memStream) {
	a, b := newPipe(), newPipe()
	return &memStream{r: a, w: b}, &memStream{r: b, w: a}
}

func (s *memStream) Read(b []byte) (int, error)  { return s.r.read(b) }
func (s *memStream) Write(b []byte) (int, error) { return s.w.write(b) }
func (s *memStream) CloseWrite() error           { s.w.set(func(p *pipe) { p.fin = true }); return nil }
func (s *memStream) Reset() error                { s.kill(); return nil }
func (s *memStream) kill() {
	for _, p := range []*pipe{s.r, s.w} {
		p.set(func(p *pipe) { p.broken, p.buf = true, nil })
	}
}
func (s *memStream) stall(on bool) {
	for _, p := range []*pipe{s.r, s.w} {
		p.set(func(p *pipe) { p.stalled = on })
	}
}
func (s *memStream) dead() bool { s.r.mu.Lock(); defer s.r.mu.Unlock(); return s.r.broken }

// harness is a session over in-memory paths, "relay" and "direct", that
// the test kills, stalls and revives (the network of spec/DirectPath.tla).
type harness struct {
	t    *testing.T
	mu   sync.Mutex // the network's: a kill against a Move or an Attach
	c, s *Conn
	str  []hstream
	up   map[string]bool
	auto bool // the client falls back on its own (OnStreamEnd)
	// refuse: the server's acceptor knows no session (Refuse)
	refuse bool
	// forget: the server's acceptor forgets the session once it is Done
	// and resets a stream that names it, as a stranger's (relay's
	// client/agent directServer)
	forget bool
}

type hstream struct {
	cl, sv *memStream
	path   string
}

func newHarness(t *testing.T, w int, auto bool) *harness {
	h := &harness{t: t, up: map[string]bool{"relay": true}, auto: auto}
	cl, sv := memPair()
	h.str = []hstream{{cl, sv, "relay"}}
	cfg := Config{Window: w, Detach: 5 * time.Second}
	h.s = Server(sv, "relay", cfg)
	cfg.OnStreamEnd = h.onStreamEnd
	h.c = Client(cl, "relay", cfg)
	tracePair(t.Name(), h.c, h.s)
	t.Cleanup(func() {
		var none traceState // the trace ends with the test, before its cleanup
		for _, c := range []*Conn{h.c, h.s} {
			c.mu.Lock()
			c.tr = none
			c.mu.Unlock()
		}
		h.c.Abort()
		h.s.Abort()
		h.c.Close()
		h.s.Close()
	})
	return h
}

func token(e uint64) []byte { return []byte{byte(e)} }

// onStreamEnd: the client's stream ended: a working path, the relay
// revived if it must (spec Fallback).
func (h *harness) onStreamEnd(uint64, error) {
	if !h.auto {
		return
	}
	h.mu.Lock()
	p := "direct"
	if !h.up["direct"] {
		p = "relay"
		if !h.up["relay"] {
			h.up["relay"] = true
			traceEnv(h.c, "Revive", "")
		}
	}
	h.mu.Unlock()
	h.move(p)
}

// move opens the session on a new stream of path p (spec Open) and lets
// the server's acceptor take it.
func (h *harness) move(p string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.up[p] {
		return 0
	}
	cl, sv := memPair()
	e, err := h.c.Move(cl, p, token)
	if err != nil {
		return 0
	}
	h.str = append(h.str, hstream{cl, sv, p})
	go h.accept(sv, p)
	return e
}

func (h *harness) accept(sv *memStream, p string) {
	o, err := ReadOpen(sv)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if sv.dead() {
		return
	}
	if h.refuse {
		Refuse(o)
		return
	}
	if h.forget && h.s.isDone() {
		_ = sv.Reset()
		return
	}
	if !bytes.Equal(o.Token, token(o.Epoch)) {
		h.t.Errorf("OPEN %d: token %x", o.Epoch, o.Token)
	}
	_ = h.s.Attach(o, p)
}

// directUp: a candidate of the peer's, verified (spec Offer, Verify).
func (h *harness) directUp() {
	h.mu.Lock()
	defer h.mu.Unlock()
	traceEnv(h.c, "Offer", "peer")
	traceEnv(h.c, "Verify", "")
	h.up["direct"] = true
}

// punchFails: a candidate that never answers, or answers as someone
// else (spec Offer, Refuse): the path stays down.
func (h *harness) punchFails() {
	h.mu.Lock()
	defer h.mu.Unlock()
	traceEnv(h.c, "Offer", "peer")
	traceEnv(h.c, "Refuse", "")
}

// die kills every stream of path p at both ends at once: what is in
// flight on it is lost (spec Die).
func (h *harness) die(p string) { h.kill(p, "Die", p) }

// rebind: the agent's address changes; its direct connection dies as a
// QUIC server's does not migrate (spec Rebind).
func (h *harness) rebind() { h.kill("direct", "Rebind", "") }

// repunch: the client's punch at once after the change, on a fresh
// exchange: the agent's new address, verified (spec RePunch, Verify).
func (h *harness) repunch() {
	h.mu.Lock()
	defer h.mu.Unlock()
	traceEnv(h.c, "RePunch", "")
	traceEnv(h.c, "Verify", "")
	h.up["direct"] = true
}

func (h *harness) kill(p, ev, x string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.c.mu.Lock()
	h.s.mu.Lock()
	for _, st := range h.str {
		if st.path != p {
			continue
		}
		for _, c := range []*Conn{h.c, h.s} {
			for _, ps := range append([]*pstream{c.cur}, c.old...) {
				if ps.s == Stream(st.cl) || ps.s == Stream(st.sv) {
					ps.dead = true
				}
			}
		}
		st.cl.kill()
	}
	h.up[p] = false
	traceEnv(h.c, ev, x)
	h.s.mu.Unlock()
	h.c.mu.Unlock()
}

// stall holds every byte on path p's live streams until die.
func (h *harness) stall(p string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, st := range h.str {
		if st.path == p {
			st.cl.stall(true)
		}
	}
}

// retained: the bytes c keeps for its peer (spec BufferBound).
func retained(c *Conn) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.acked > c.hi {
		return 0
	}
	return c.hi - c.acked
}

func payload(n int, seed uint64) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(seed, seed))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// transfer writes a to c and b to s concurrently, ends both directions,
// and returns what each end read (until EOF) and the read errors. step,
// if set, runs once per chunk the client wrote (the test's network).
func (h *harness) transfer(a, b []byte, chunk int, step func(i int)) (gotS, gotC []byte, errS, errC error) {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		gotS, errS = io.ReadAll(h.s)
	}()
	go func() {
		defer wg.Done()
		gotC, errC = io.ReadAll(h.c)
	}()
	go func() {
		defer wg.Done()
		if _, err := h.s.Write(b); err == nil {
			_ = h.s.CloseWrite()
		}
	}()
	for i := 0; i*chunk < len(a); i++ {
		if _, err := h.c.Write(a[i*chunk : min(len(a), (i+1)*chunk)]); err != nil {
			break
		}
		if step != nil {
			step(i)
		}
	}
	_ = h.c.CloseWrite()
	wg.Wait()
	return
}
