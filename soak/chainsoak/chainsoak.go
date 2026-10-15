// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package chainsoak is the chain soak's roles (SOAK.md, "Chains"): a far
// end, a forwarding hop and an origin's echo, used in process by the
// soak's chain-3hop config and across machines (pods) by cmd/linkhop.
package chainsoak

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// Header of a session whose far reader never reads (InFlight).
const Stuck = "stuck"

// Echo is a far end: it accepts and echoes; a Stuck session it accepts
// and never reads, until the session ends.
func Echo(_ string, h []byte, s peerlink.Session) {
	if cs, ok := s.(peerlink.ChainSession); ok {
		_ = cs.Accept()
	}
	if string(h) == Stuck {
		select { // read nothing until the opener ends it
		case <-done(s):
		case <-time.After(10 * time.Minute):
		}
		_ = s.Close()
		return
	}
	_, _ = io.Copy(s, s)
	_ = s.CloseWrite()
	_, _ = io.Copy(io.Discard, s)
	_ = s.Close()
}

// done is s's Done (link and quiclink sessions), else nil (never).
func done(s peerlink.Session) <-chan struct{} {
	if d, ok := s.(interface{ Done() <-chan struct{} }); ok {
		return d.Done()
	}
	return nil
}

// Hop is a forwarding hop: every session it takes goes on to next on l
// (peerlink.Forward with cfg).
func Hop(l peerlink.Link, next string, cfg link.ForwardConfig) func(string, []byte, peerlink.Session) {
	return peerlink.Forward(func(ctx context.Context, _ string, h []byte, _ peerlink.Session) (peerlink.Session, error) {
		return l.Open(ctx, next, h)
	}, cfg)
}

// EchoOnce opens a session to peer on l, waits for the far end's answer
// (when the session answers), sends n random bytes and checks by SHA-256
// that the same came back: intact false is an integrity failure; err is
// why the echo failed (a *link.Abort says where and why).
func EchoOnce(ctx context.Context, l peerlink.Link, peer string, n int) (intact bool, err error) {
	s, err := l.Open(ctx, peer, []byte("echo"))
	if err != nil {
		return true, fmt.Errorf("open: %w", err)
	}
	defer s.Close()
	if cs, ok := s.(peerlink.ChainSession); ok && cs.Answers() {
		if err := cs.Answered(ctx); err != nil {
			return true, fmt.Errorf("answer: %w", err)
		}
	}
	payload := make([]byte, n)
	_, _ = rand.Read(payload)
	want := sha256.Sum256(payload)
	werr := make(chan error, 1)
	go func() {
		_, err := s.Write(payload)
		if err == nil {
			err = s.CloseWrite()
		}
		werr <- err
	}()
	h := sha256.New()
	got, rerr := io.Copy(h, s)
	if err := <-werr; err != nil {
		return true, fmt.Errorf("write: %w", err)
	}
	if rerr != nil {
		return true, fmt.Errorf("read after %d of %d bytes: %w", got, n, rerr)
	}
	return got == int64(n) && bytes.Equal(h.Sum(nil), want[:]), nil
}

// InFlight opens a Stuck session to peer on l and writes into it until
// its writes stall for settle: what the path holds for a stalled far
// reader (at most the sum of its links' windows plus 256 KiB per hop).
// The session is closed when it returns.
func InFlight(ctx context.Context, l peerlink.Link, peer string, settle time.Duration) (int64, error) {
	s, err := l.Open(ctx, peer, []byte(Stuck))
	if err != nil {
		return 0, err
	}
	defer s.Close()
	sent := make(chan int, 64)
	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, err := s.Write(buf)
			sent <- n
			if err != nil {
				close(sent)
				return
			}
		}
	}()
	var total int64
	t := time.NewTimer(settle)
	defer t.Stop()
	for {
		select {
		case n, ok := <-sent:
			if !ok {
				return total, fmt.Errorf("the stuck session ended after %d bytes", total)
			}
			total += int64(n)
			t.Reset(settle)
		case <-t.C:
			return total, nil
		case <-ctx.Done():
			return total, ctx.Err()
		}
	}
}
