// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlinktest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// The chain cases: a path origin -> hop -> far end over two Pairs, the hop
// serving peerlink.Forward on the first Pair's B with a Route that opens
// on the second Pair's A. Where the sessions answer opens
// (peerlink.ChainSession with Answers), the typed outcomes are checked:
// the far end's answer at the origin, codes, which hop; otherwise (an
// implementation without answers) the plain contract: bytes, io.EOF only
// at a clean end.

// ChainPath is a path of two Pairs joined by a Forward hop.
type ChainPath struct {
	In, Out Pair // origin -> hop, hop -> far end
}

// NewChainPath makes a path from two Pairs of newPair: the hop serves
// Forward(cfg) on In.B and opens on Out.A; the far end (Out.B) serves far.
// The test's cleanup drains all four Links.
func NewChainPath(t *testing.T, newPair func(*testing.T) Pair, cfg link.ForwardConfig, far func(peer string, header []byte, s peerlink.Session)) ChainPath {
	t.Helper()
	out := pair(t, newPair, far)
	route := func(ctx context.Context, _ string, header []byte, _ peerlink.Session) (peerlink.Session, error) {
		return out.A.Open(ctx, out.BName, header)
	}
	in := pair(t, newPair, peerlink.Forward(route, cfg))
	return ChainPath{In: in, Out: out}
}

// open opens a session from the origin.
func (c ChainPath) open(t *testing.T, header string) peerlink.Session {
	t.Helper()
	return open(t, c.In, header)
}

// answers is s's ChainSession when its connection answers opens.
func answers(s peerlink.Session) peerlink.ChainSession {
	if cs, ok := s.(peerlink.ChainSession); ok && cs.Answers() {
		return cs
	}
	return nil
}

// abortOf returns err's *link.Abort or fails.
func abortOf(t *testing.T, what string, err error) *link.Abort {
	t.Helper()
	var a *link.Abort
	if !errors.As(err, &a) {
		t.Fatalf("%s: %v (%T), want a *link.Abort", what, err, err)
	}
	return a
}

// farAccept answers yes, then echoes.
func farAccept(_ string, _ []byte, s peerlink.Session) {
	if cs, ok := s.(peerlink.ChainSession); ok {
		_ = cs.Accept()
	}
	echo("", nil, s)
}

var chainCases = []struct {
	name string
	run  func(*testing.T, func(*testing.T) Pair)
}{
	{"ChainAccept", testChainAccept},
	{"ChainRefuse", testChainRefuse},
	{"ChainAbortFar", testChainAbortFar},
	{"ChainAbortOrigin", testChainAbortOrigin},
	{"ChainHalfClose", testChainHalfClose},
	{"ChainDrainMiddle", testChainDrainMiddle},
}

// ChainAccept: the origin's Answered returns once the far end accepted,
// with the path's length (1: one hop beyond), and the echo is intact.
func testChainAccept(t *testing.T, newPair func(*testing.T) Pair) {
	c := NewChainPath(t, newPair, link.ForwardConfig{}, farAccept)
	s := c.open(t, "echo")
	if cs := answers(s); cs != nil {
		if err := cs.Answered(context.Background()); err != nil || cs.PathLen() != 1 {
			t.Fatalf("Answered %v, PathLen %d; want nil, 1", err, cs.PathLen())
		}
	}
	if err := exchange(s, 4<<20); err != nil {
		t.Fatal(err)
	}
}

// ChainRefuse: the far end's refusal reaches the origin with its code,
// message and hop (1); without answers, as an error that is not io.EOF.
func testChainRefuse(t *testing.T, newPair func(*testing.T) Pair) {
	c := NewChainPath(t, newPair, link.ForwardConfig{}, func(_ string, _ []byte, s peerlink.Session) {
		if cs, ok := s.(peerlink.ChainSession); ok {
			_ = cs.Refuse(link.CodeApp+1, "no such target")
			return
		}
		_ = s.Reset()
	})
	s := c.open(t, "x")
	cs := answers(s)
	if cs == nil {
		cutErr(t, "origin", readErr(s))
		return
	}
	a := abortOf(t, "Answered", cs.Answered(context.Background()))
	if !a.Refused || a.Code != link.CodeApp+1 || a.Hops != 1 || a.Msg != "no such target" {
		t.Fatalf("refusal %+v", a)
	}
	if !errors.Is(readErr(s), link.ErrOpenRefused) {
		t.Fatal("read after a refusal: not ErrOpenRefused")
	}
}

// ChainAbortFar: the far end's reset with a code, after bytes, reaches the
// origin (after those bytes, or some of them: a QUIC reset discards what
// was not delivered).
func testChainAbortFar(t *testing.T, newPair func(*testing.T) Pair) {
	c := NewChainPath(t, newPair, link.ForwardConfig{}, func(_ string, _ []byte, s peerlink.Session) {
		_, _ = s.Write([]byte("partial"))
		if cs, ok := s.(peerlink.ChainSession); ok && cs.Answers() {
			_ = cs.Abort(link.CodeApp+2, "target died")
			return
		}
		_ = s.Reset()
	})
	s := c.open(t, "x")
	b, err := io.ReadAll(s)
	if !strings.HasPrefix("partial", string(b)) {
		t.Fatalf("read %q before the abort", b)
	}
	cutErr(t, "origin", err)
	if answers(s) != nil {
		if a := abortOf(t, "read", err); a.Refused || a.Code != link.CodeApp+2 || a.Hops != 1 || (a.Msg != "" && a.Msg != "target died") { // a reset may carry no message (QUIC)
			t.Fatalf("abort %+v", a)
		}
	}
}

// ChainAbortOrigin: the origin's reset with a code reaches the far end.
func testChainAbortOrigin(t *testing.T, newPair func(*testing.T) Pair) {
	got := make(chan error, 1)
	c := NewChainPath(t, newPair, link.ForwardConfig{}, func(_ string, _ []byte, s peerlink.Session) {
		_, _ = s.Write([]byte("ready")) // answers (accepted) too
		got <- readErr(s)
		_ = s.Close()
	})
	s := c.open(t, "x")
	if _, err := io.ReadFull(s, make([]byte, 5)); err != nil { // the path is up
		t.Fatal(err)
	}
	cs := answers(s)
	if cs != nil {
		_ = cs.Abort(link.CodeApp+3, "origin gave up")
	} else {
		_ = s.Reset()
	}
	err := recvWithin(t, got, 1, time.Now(), Within, "far end's read")[0]
	cutErr(t, "far end", err)
	if cs != nil {
		if a := abortOf(t, "far end's read", err); a.Code != link.CodeApp+3 || a.Hops != 1 {
			t.Fatalf("abort %+v", a)
		}
	}
}

// ChainHalfClose: each direction ends on its own through the hop, either
// side first.
func testChainHalfClose(t *testing.T, newPair func(*testing.T) Pair) {
	t.Run("origin first", func(t *testing.T) {
		c := NewChainPath(t, newPair, link.ForwardConfig{}, func(_ string, _ []byte, s peerlink.Session) {
			defer s.Close()
			b, err := io.ReadAll(s)
			if err == nil {
				_, _ = s.Write(append([]byte("after "), b...))
			}
			_ = s.CloseWrite()
			_, _ = io.Copy(io.Discard, s)
		})
		s := c.open(t, "x")
		_, _ = s.Write([]byte("close"))
		closeWrite(t, s)
		b, err := io.ReadAll(s)
		if err != nil || string(b) != "after close" {
			t.Fatalf("read %q, %v", b, err)
		}
	})
	t.Run("far end first", func(t *testing.T) {
		late := make(chan string, 1)
		c := NewChainPath(t, newPair, link.ForwardConfig{}, func(_ string, _ []byte, s peerlink.Session) {
			defer s.Close()
			_, _ = s.Write([]byte("bye"))
			_ = s.CloseWrite()
			b, _ := io.ReadAll(s)
			late <- string(b)
		})
		s := c.open(t, "x")
		b, err := io.ReadAll(s)
		if err != nil || string(b) != "bye" {
			t.Fatalf("read %q, %v", b, err)
		}
		_, _ = s.Write([]byte("still here"))
		closeWrite(t, s)
		if got := recvWithin(t, late, 1, time.Now(), Within, "far end's read")[0]; got != "still here" {
			t.Fatalf("far end read %q after its half close", got)
		}
	})
}

// ChainDrainMiddle: the hop drains its Link toward the far end: the
// session joined through it finishes intact, and a new open through it
// is refused (CodeDraining, from the hop, where sessions answer).
func testChainDrainMiddle(t *testing.T, newPair func(*testing.T) Pair) {
	c := NewChainPath(t, newPair, link.ForwardConfig{}, farAccept)
	s := c.open(t, "echo")
	half(t, s)
	drained := drain(c.Out.A, Within)
	deadline := time.Now().Add(Within)
	for {
		n := c.open(t, "echo")
		var err error
		if cs := answers(n); cs != nil {
			err = cs.Answered(context.Background())
		} else {
			err = exchange(n, 1)
		}
		if err != nil {
			if cs := answers(n); cs != nil {
				if a := abortOf(t, "open through a draining hop", err); !a.Refused || a.Code != link.CodeDraining || a.Hops != 0 {
					t.Fatalf("refusal %+v", a)
				}
			}
			break
		}
		_ = n.Close()
		if time.Now().After(deadline) {
			t.Fatalf("opens through the hop still accepted %v after its Drain began", Within)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := exchange(s, 1<<20); err != nil {
		t.Fatalf("the joined session across the drain: %v", err)
	}
	_ = s.Close()
	if err := recvWithin(t, drained, 1, time.Now(), Within, "Drain returned")[0]; err != nil {
		t.Errorf("Drain: %v, want nil", err)
	}
}
