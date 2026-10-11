// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// opening returns Mux.Open's argument for a session with header.
func opening(header string) func(sid string) ([]byte, error) {
	return func(string) ([]byte, error) { return []byte(header), nil }
}

// header is an open's header. allow:wrapper
func header(open []byte) string { return string(open) }

// muxPair returns an opener Mux and the accepting Mux's peer, serving
// sessions with accept.
func muxPair(t *testing.T, accept func([]byte, *Session)) *Mux {
	t.Helper()
	ca, cb := v2pair(t, Config{Window: testWindow, Auth: testToken}, Config{Window: testWindow, Auth: testToken})
	client, server := NewMux(ca), NewMux(cb)
	go func() { _ = client.Serve(nil) }()
	go func() { _ = server.Serve(accept) }()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client
}

// echoAccept echoes every session except those whose agent is "nope",
// which it refuses.
func echoAccept(open []byte, s *Session) {
	defer s.Close() //nolint:errcheck
	if header(open) == "nope" {
		return
	}
	_, _ = io.Copy(s, s)
}

func TestMuxSessions(t *testing.T) {
	client := muxPair(t, echoAccept)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 3*testWindow/16) // past a window: credit must return
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			s, err := client.Open(opening("echo"))
			if err != nil {
				t.Error(err)
				return
			}
			msg := append([]byte(fmt.Sprintf("session %d:", i)), payload...)
			go func() { _, _ = s.Write(msg) }()
			got := make([]byte, len(msg))
			if _, err := io.ReadFull(s, got); err != nil || !bytes.Equal(got, msg) {
				t.Errorf("session %d: echo %d bytes, %v", i, len(got), err)
			}
			_ = s.Close()
		})
	}
	wg.Wait()

	// A refused open is a close: the opener reads io.EOF, its writes fail.
	s, err := client.Open(opening("nope"))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("refused session read %d, %v; want io.EOF", n, err)
	}
	if _, err := s.Write([]byte("x")); err == nil {
		t.Fatal("write on a refused session succeeded")
	}
	waitSessions(t, client, 0)
}

func waitSessions(t *testing.T, m *Mux, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; {
		m.mu.Lock()
		got := len(m.sess)
		m.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// A session whose reader stopped takes its window and no more; another
// session on the connection keeps flowing, and the first resumes once read.
func TestMuxSlowSessionIsolated(t *testing.T) {
	release := make(chan struct{})
	client := muxPair(t, func(open []byte, s *Session) {
		if header(open) == "slow" {
			<-release
		}
		echoAccept(open, s)
	})
	// A hang fails in 30s, not at the package timeout: closing the mux ends
	// every blocked read and write below.
	defer time.AfterFunc(30*time.Second, func() {
		t.Error("hung: closing the mux")
		_ = client.Close()
	}).Stop()
	slow, err := client.Open(opening("slow"))
	if err != nil {
		t.Fatal(err)
	}
	wrote := make(chan error, 1)
	big := bytes.Repeat([]byte("s"), 3*testWindow)
	go func() { _, err := slow.Write(big); wrote <- err }()

	fast, err := client.Open(opening("fast"))
	if err != nil {
		t.Fatal(err)
	}
	msg := bytes.Repeat([]byte("f"), 2*testWindow)
	go func() { _, _ = fast.Write(msg) }()
	if _, err := io.ReadFull(fast, make([]byte, len(msg))); err != nil {
		t.Fatalf("a session behind an unread one: %v", err)
	}
	select {
	case err := <-wrote:
		t.Fatalf("wrote 3 windows to an unread session: %v", err)
	default:
	}
	close(release)
	if _, err := io.ReadFull(slow, make([]byte, len(big))); err != nil {
		t.Fatalf("the slow session once read: %v", err)
	}
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
}

// A Write of several windows on a connection with no other traffic: the
// writer must not wait for credit while its own chunks sit unsent in the
// write buffer, since the peer grants credit only for bytes it received.
// Deterministic: the only frames client->server are this Write's, so a
// writer that waits with buffered chunks hangs every run.
func TestMuxWriteFlushesBeforeWaitingForCredit(t *testing.T) {
	a, b := net.Pipe()
	client, server := NewMux(newTestConn(a, MinWindow, MinWindow)), NewMux(newTestConn(b, MinWindow, MinWindow))
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	got := make(chan int64, 1)
	go func() { _ = client.Serve(nil) }()
	go func() {
		_ = server.Serve(func(_ []byte, s *Session) {
			n, _ := s.WriteTo(io.Discard)
			got <- n
		})
	}()
	s, err := client.Open(opening("sink"))
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 8*MinWindow)
	wrote := make(chan error, 1)
	go func() { _, err := s.Write(big); wrote <- err }()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Write of 8 windows waits for credit with its chunks unsent")
	}
	_ = s.Close()
	if n := <-got; n != int64(len(big)) {
		t.Fatalf("peer read %d bytes, want %d", n, len(big))
	}
}

func TestMuxMaxSessions(t *testing.T) {
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	var accepted atomic.Int64
	client := muxPair(t, func(_ []byte, s *Session) {
		accepted.Add(1)
		<-hold
		_ = s.Close()
	})
	var first *Session
	for i := range MaxSessions {
		s, err := client.Open(opening("a"))
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		if first == nil {
			first = s
		}
	}
	if _, err := client.Open(opening("a")); !errors.Is(err, ErrFull) {
		t.Fatalf("session past MaxSessions: %v, want ErrFull", err)
	}
	_ = first.Close()
	if _, err := client.Open(opening("a")); err != nil {
		t.Fatalf("after one closed: %v", err)
	}
	// An open past the accepting side's limit: TestMuxV2MaxSessions.
}

// countConn counts writes and slows reads, so writes queue up.
type countConn struct {
	net.Conn
	writes atomic.Int64
	slow   bool
}

func (c *countConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

func (c *countConn) Read(b []byte) (int, error) {
	if c.slow {
		time.Sleep(20 * time.Microsecond)
	}
	return c.Conn.Read(b)
}

// Concurrent senders: every frame arrives intact and in order per
// session, and frames sent while a write is in flight share the next one.
func TestConnCombinesWrites(t *testing.T) {
	a, b := net.Pipe()
	ca, cb := &countConn{Conn: a}, &countConn{Conn: b, slow: true}
	sender, receiver := newTestConn(ca, testWindow, testWindow), newTestConn(cb, testWindow, testWindow)
	t.Cleanup(func() { _ = sender.Close(); _ = receiver.Close() })
	const sessions, frames = 16, 200
	var wg sync.WaitGroup
	for s := range sessions {
		wg.Go(func() {
			for i := range frames {
				if err := sender.SendData(fmt.Sprint(s), []byte(fmt.Sprintf("%d.%d;", s, i))); err != nil {
					t.Error(err)
					return
				}
				if err := sender.Flush(); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	got := map[string]*bytes.Buffer{}
	for range sessions * frames {
		f, err := receiver.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if got[f.SID] == nil {
			got[f.SID] = new(bytes.Buffer)
		}
		got[f.SID].Write(f.Data)
	}
	wg.Wait()
	for s := range sessions {
		var want bytes.Buffer
		for i := range frames {
			fmt.Fprintf(&want, "%d.%d;", s, i)
		}
		if g := got[fmt.Sprint(s)]; g == nil || g.String() != want.String() {
			t.Fatalf("session %d out of order or incomplete", s)
		}
	}
	if w := ca.writes.Load(); w >= sessions*frames {
		t.Fatalf("%d writes for %d frames: none combined", w, sessions*frames)
	}
}
