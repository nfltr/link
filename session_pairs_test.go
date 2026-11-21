// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// Every pair of Session calls the docs let run at once (Session, net.Conn)
// that no other test races, raced round after round (lane SESSIONAPI; the
// table in spec/README.md "Session API pairs"): each byte handed out once
// and in order, every call returns, the end once. The pairs a test of its
// own races: Read/Read (TestSessionConcurrentReads), Read/WriteTo and
// Read/Pump (TestSessionReadBesideWriteTo), Read/SetReadDeadline
// (TestSessionReadDeadlineClearedNeverEOF), Write/CloseWrite
// (TestWriteRacingCloseWriteSendsNothingAfterFIN), Write/SetWriteDeadline
// (TestSessionWriteWaitingForCreditSeesFirstDeadline), CloseWriteSync/Reset
// (TestEndAckCloseWriteRacingResetEndsTheWait). make test-under-load
// PKGS='-run TestSessionAPIPairs .' runs it starved.
func TestSessionAPIPairs(t *testing.T) {
	rounds := 100
	switch {
	case traceOn:
		rounds = 2
	case testing.Short():
		rounds = 10
	}
	for _, pair := range [][2]string{
		{"read", "read"}, {"read", "writeto"}, {"writeto", "writeto"},
		{"read", "close"}, {"read", "reset"}, {"writeto", "close"}, {"writeto", "reset"},
		{"write", "write"}, {"write", "closewrite"}, {"write", "close"}, {"write", "reset"},
		{"write", "closewritesync"},
		{"closewrite", "closewrite"}, {"closewrite", "close"}, {"closewrite", "reset"},
		{"closewritesync", "close"}, {"closewritesync", "closewrite"},
		{"close", "close"}, {"close", "reset"}, {"reset", "reset"},
	} {
		t.Run(pair[0]+"/"+pair[1], func(t *testing.T) {
			open := sessionOpener(t)
			for range rounds {
				s, p := open(t)
				racePair(t, s, p, pair)
			}
		})
	}
}

// sessionOpener opens sessions on one connection: the opener's end, then
// the accepted one.
func sessionOpener(tb testing.TB) func(t testing.TB) (*Session, *Session) {
	got := make(chan *Session, 1)
	dm, _ := v2muxPair(tb, func(_ []byte, s *Session) { got <- s }, v2cfg, v2cfg)
	return func(t testing.TB) (*Session, *Session) {
		t.Helper()
		s, err := dm.Open(context.Background(), []byte("h"))
		if err != nil {
			t.Fatal(err)
		}
		select {
		case p := <-got:
			return s, p
		case <-time.After(5 * time.Second):
			t.Fatal("no session accepted")
		}
		return nil, nil
	}
}

// pairByte is byte i (0..126) of writer w (0, 1): each writer's bytes in
// order, apart from the other's.
func pairByte(w, i int) byte { return byte(w<<7 | i) }

const pairBytes = 127

// pairOp is one call of a pair, on s: its bytes read (readers) or the
// bytes Write said it sent (writers).
type pairOp struct {
	got  []byte
	sent int
	err  error
}

func runPairOp(s *Session, kind string, w int, op *pairOp) {
	switch kind {
	case "read":
		b := make([]byte, 1+rand.IntN(5))
		for {
			n, err := s.Read(b)
			op.got = append(op.got, b[:n]...)
			if err != nil {
				op.err = err
				return
			}
		}
	case "writeto":
		var c collector
		_, op.err = s.WriteTo(&c)
		op.got = c.b
	case "write":
		for i := 0; i < pairBytes; {
			k := min(1+rand.IntN(5), pairBytes-i)
			b := make([]byte, k)
			for j := range b {
				b[j] = pairByte(w, i+j)
			}
			n, err := s.Write(b)
			op.sent += n
			if i += k; err != nil {
				op.err = err
				return
			}
		}
	case "closewrite":
		op.err = s.CloseWrite()
	case "closewritesync":
		op.err = s.CloseWriteSync()
	case "close":
		op.err = s.Close()
	case "reset":
		op.err = s.Reset()
	}
}

// collector is a WriteTo's writer.
type collector struct{ b []byte }

func (c *collector) Write(p []byte) (int, error) { c.b = append(c.b, p...); return len(p), nil }

func isReader(k string) bool { return k == "read" || k == "writeto" }

// racePair runs pair at once on s, p the peer: p writes bytes 1..255 then
// ends its writes for readers, or reads everything s sends.
func racePair(t *testing.T, s, p *Session, pair [2]string) {
	t.Helper()
	readers := isReader(pair[0])
	ending := !isReader(pair[1]) // a reader beside an end: the peer never ends
	peerDone := make(chan []byte, 1)
	if readers {
		go func() {
			for i := 1; i < 256; {
				k := min(1+rand.IntN(7), 256-i)
				b := make([]byte, k)
				for j := range b {
					b[j] = byte(i + j)
				}
				if _, err := p.Write(b); err != nil {
					break // ended here
				}
				i += k
			}
			if !ending {
				_ = p.CloseWrite()
			}
			peerDone <- nil
		}()
	} else {
		go func() { b, _ := io.ReadAll(p); peerDone <- b }()
	}
	var ops [2]pairOp
	var wg sync.WaitGroup
	for i, kind := range pair {
		wg.Go(func() { runPairOp(s, kind, i, &ops[i]) })
	}
	if !waitFor(&wg, 10*time.Second) {
		t.Fatalf("%v: a call did not return", pair)
	}
	if pair == [2]string{"write", "write"} {
		_ = s.CloseWrite()
	}
	var peer []byte
	select {
	case peer = <-peerDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("%v: the peer did not end", pair)
	}
	if readers {
		checkReaders(t, pair, ops[:], ending)
	} else {
		checkWriters(t, pair, ops[:], peer)
	}
	_ = s.Close()
	_ = p.Close()
	select {
	case <-s.Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("%v: the session never ended", pair)
	}
	if n, err := s.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatalf("%v: Read after the end: (%d, %v)", pair, n, err)
	}
}

// checkReaders: each byte to one reader at most once, in order; all of
// them, with a clean end, once the peer ended.
func checkReaders(t *testing.T, pair [2]string, ops []pairOp, ending bool) {
	t.Helper()
	var seen [256]int
	total := 0
	for i, op := range ops {
		if !isReader(pair[i]) {
			continue
		}
		for j, b := range op.got {
			if seen[b]++; b == 0 || seen[b] > 1 || (j > 0 && b <= op.got[j-1]) {
				t.Fatalf("%v: reader %d got %v: a byte twice or out of order", pair, i, op.got)
			}
		}
		total += len(op.got)
		if !ending && op.err != nil && op.err != io.EOF {
			t.Fatalf("%v: reader %d ended %v", pair, i, op.err)
		}
	}
	if !ending && total != 255 {
		t.Fatalf("%v: readers got %d bytes, want 255", pair, total)
	}
}

// checkWriters: each writer's bytes reach the peer in order, once; when
// nothing reset the session, every byte a Write said it sent (a Write
// racing CloseWrite once sent data after the FIN and returned nil).
func checkWriters(t *testing.T, pair [2]string, ops []pairOp, peer []byte) {
	t.Helper()
	reset := pair[0] == "reset" || pair[1] == "reset"
	var next [2]int
	for _, b := range peer {
		w, i := int(b>>7), int(b&127)
		if pair[w] != "write" || i != next[w] {
			t.Fatalf("%v: the peer read %v: a byte twice, out of order or never written", pair, peer)
		}
		next[w]++
	}
	for w, op := range ops {
		if pair[w] != "write" {
			continue
		}
		if next[w] > op.sent || (!reset && next[w] != op.sent) {
			t.Fatalf("%v: writer %d said it sent %d bytes (%v), the peer read %d", pair, w, op.sent, op.err, next[w])
		}
	}
}

// waitFor waits for wg, up to d.
func waitFor(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// A CloseWriteSync racing Close returns at once: Close ended the ENDACK
// waits before closeWrite registered its own, which waited out the idle
// bound (30 s) for an ENDACK the closed session never gets (found by
// TestSessionAPIPairs under -race, lane SESSIONAPI; as CloseWrite racing
// Reset, spec/RelayedClose.tla close-after-reset-waits).
func TestCloseWriteSyncRacingCloseReturns(t *testing.T) {
	open := sessionOpener(t)
	rounds := 300
	if testing.Short() {
		rounds = 30
	}
	for i := range rounds {
		s, p := open(t)
		go func() { _, _ = io.Copy(io.Discard, p) }()
		res := make(chan error, 1)
		go func() { res <- s.CloseWriteSync() }()
		_ = s.Close()
		select {
		case err := <-res:
			if err != nil && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("round %d: CloseWriteSync beside Close: %v; want nil or io.ErrClosedPipe", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: CloseWriteSync beside Close did not return", i)
		}
		_ = p.Close()
	}
}
