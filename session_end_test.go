// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// openEcho opens a session on a version-2 pair whose listener hands each
// accepted session to got.
func openPair(t *testing.T, dialer, listener Config) (*Session, *Session) {
	t.Helper()
	got := make(chan *Session, 1)
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) { got <- s }, dialer, listener)
	s, err := dm.Open([]byte("h"))
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

// A reset session reads the peer's bytes, then ErrReset, never a clean end.
func TestSessionReset(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	if _, err := p.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	_ = p.Reset()
	b, err := io.ReadAll(s)
	if !errors.Is(err, ErrReset) || string(b) != "tail" {
		t.Fatalf("read %q, %v; want the tail, then ErrReset", b, err)
	}
	if _, err := s.WriteTo(io.Discard); !errors.Is(err, ErrReset) {
		t.Fatalf("WriteTo after a reset: %v", err)
	}
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after a reset")
	}
}

// A clean close still reads io.EOF; Done closes once both sides closed.
func TestSessionCleanEnd(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	_, _ = p.Write([]byte("x"))
	_ = p.CloseWrite()
	b, err := io.ReadAll(s)
	if err != nil || string(b) != "x" {
		t.Fatalf("read %q, %v", b, err)
	}
	select {
	case <-s.Done():
		t.Fatal("Done closed while this side still sends")
	default:
	}
	_ = s.CloseWrite()
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after both half-closes")
	}
}

// WriteBuffers sends every buffer in order, past the peer's window.
func TestSessionWriteBuffers(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	want := bytes.Repeat([]byte("0123456789abcdef"), 3*testWindow/16+5)
	bufs := [][]byte{want[:7], want[7 : len(want)/2], want[len(want)/2:]}
	go func() {
		if n, err := p.WriteBuffers(bufs); err != nil || n != int64(len(want)) {
			t.Errorf("WriteBuffers: %d, %v", n, err)
		}
		_ = p.CloseWrite()
	}()
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("read %d bytes, %v; want %d intact", len(got), err, len(want))
	}
}

// An open the peer refuses reads a reset, never a clean end.
func TestRefusedOpenReadsReset(t *testing.T) {
	dm, _ := v2muxPair(t, nil, v2cfg, v2cfg)
	s, err := openV2(dm, "h")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(s); !errors.Is(err, ErrReset) {
		t.Fatalf("read: %v; want ErrReset", err)
	}
}

// A peer of link v0.2 (no FeatureReset) rejects CLOSE code 2: it gets a
// plain close.
func TestSessionResetToAnOldPeer(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	p.m.c.peerFeatures = 0 // as bound with a v0.2 dialer
	_ = p.Reset()
	if b, err := io.ReadAll(s); err != nil || len(b) != 0 {
		t.Fatalf("read %q, %v; want a clean end", b, err)
	}
	if s.m.c.Err() != nil {
		t.Fatalf("connection failed: %v", s.m.c.Err())
	}
}

// A session cut with its connection reads an error, never io.EOF.
func TestSessionCutWithItsConnection(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	_ = p.m.Close()
	if _, err := io.ReadAll(s); err == nil {
		t.Fatal("a session cut with its connection read a clean end")
	}
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed")
	}
}

func TestSessionReadDeadline(t *testing.T) {
	s, p := openPair(t, v2cfg, v2cfg)
	_ = s.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	start := time.Now()
	if _, err := s.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("deadline took %v", d)
	}
	// A past deadline ends a pending read at once (net/http's abort).
	errc := make(chan error, 1)
	_ = s.SetReadDeadline(time.Time{})
	go func() { _, err := s.Read(make([]byte, 1)); errc <- err }()
	time.Sleep(20 * time.Millisecond)
	_ = s.SetReadDeadline(time.Unix(1, 0))
	if err := <-errc; !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("pending Read: %v", err)
	}
	// Cleared, reads work again and nothing was lost.
	_ = s.SetReadDeadline(time.Time{})
	_, _ = p.Write([]byte("ok"))
	b := make([]byte, 2)
	if _, err := io.ReadFull(s, b); err != nil || string(b) != "ok" {
		t.Fatalf("read %q, %v", b, err)
	}
}

// A listener of v0.2 recorded a hello with no feature bits: it still binds,
// and the peer's features read 0.
func TestServerTakesAHelloWithoutFeatures(t *testing.T) {
	a, b := tcpPair(t)
	old := appendHelloV2(nil, helloV2{maxVersion: 2, window: testWindow, maxSessions: 8,
		liveness: LivenessDefault, method: MethodToken, auth: []byte(v2Token)})
	go func() { _, _ = a.Write(old) }()
	c, _, err := Server(context.Background(), b, Config{Window: testWindow, Auth: tokenAuth(v2Token)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	if c.peerFeatures != 0 {
		t.Fatalf("features %x", c.peerFeatures)
	}
	rep, err := readReplyV2(a)
	if err != nil || rep.features != features {
		t.Fatalf("reply %+v, %v", rep, err)
	}
	if !bytes.HasPrefix(old, []byte(Magic)) {
		t.Fatal("hello")
	}
}

// A session cut with its connection reads ErrConnClosed saying why and
// what to do, never io.EOF (a clean end); a write past it returns
// io.ErrClosedPipe wrapping the same error.
func TestSessionConnClosed(t *testing.T) {
	for _, c := range []struct {
		name, why string
		closer    func(dm, lm *Mux) error
	}{
		{"the peer closed it", "the peer closed it", func(_, lm *Mux) error { return lm.Close() }},
		{"this side closed it", "", func(dm, _ *Mux) error { return dm.Close() }},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := make(chan *Session, 1)
			dm, lm := v2muxPair(t, func(_ []byte, s *Session) { got <- s }, v2cfg, v2cfg)
			s, err := openV2(dm, "h")
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-got:
			case <-time.After(5 * time.Second):
				t.Fatal("no session accepted")
			}
			_ = c.closer(dm, lm)
			_, err = io.ReadAll(s)
			if !errors.Is(err, ErrConnClosed) || errors.Is(err, io.EOF) || !strings.Contains(err.Error(), c.why) || !strings.Contains(err.Error(), "open a new session") {
				t.Fatalf("read: %v; want ErrConnClosed saying %q and to open a new session", err, c.why)
			}
			<-s.Done()
			if _, err := s.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) || !errors.Is(err, ErrConnClosed) {
				t.Fatalf("write: %v; want io.ErrClosedPipe wrapping ErrConnClosed", err)
			}
		})
	}
}

// A session's deadline timers stop when it ends, and none arms after: a
// pending one held the session (its queue, its Mux) until it fired.
func TestSessionEndStopsDeadlineTimers(t *testing.T) {
	s, _ := openPair(t, v2cfg, v2cfg)
	armed := func() bool {
		s.dlMu.Lock()
		r := s.dlTimer != nil
		s.dlMu.Unlock()
		s.wdMu.Lock()
		w := s.wdTimer != nil
		s.wdMu.Unlock()
		return r || w
	}
	_ = s.SetDeadline(time.Now().Add(time.Hour))
	_ = s.Reset()
	if armed() {
		t.Fatal("a deadline timer outlived the session")
	}
	_ = s.SetDeadline(time.Now().Add(time.Hour))
	if armed() {
		t.Fatal("a deadline set after the session ended armed a timer")
	}
}
