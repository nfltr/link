// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	quic "github.com/nfltr/quic-go"
)

// heldConn's reads wait while held before they return what they read:
// quic-go's receive path in a process that does not run (the packets wait
// in the socket).
type heldConn struct {
	net.PacketConn
	mu   sync.Mutex
	held chan struct{} // non-nil: closed when reads may go on
}

func (c *heldConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	c.mu.Lock()
	h := c.held
	c.mu.Unlock()
	if h != nil {
		<-h
	}
	return n, addr, err
}

// TestStarvedProcessKeepsALivePeer: a process that did not run for longer
// than its liveness allows (link's spec/README.md "Starvation", finding
// S1): its packets wait in the socket, its liveness check runs late and
// before quic-go read them. The connection must live on. Before the fix
// the late check closed it (CodeDeadPeer).
func TestStarvedProcessKeepsALivePeer(t *testing.T) {
	t.Setenv("QUIC_GO_DISABLE_RECEIVE_BUFFER_WARNING", "true") // heldConn is not a *net.UDPConn
	l := link.LivenessFast
	pc := listenUDP(t)
	hc := &heldConn{PacketConn: listenUDP(t)}
	tr := &quic.Transport{Conn: hc}
	t.Cleanup(func() { _ = tr.Close() })
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: l})
	a := mustNew(t, quiclink.Config{Liveness: l,
		Dial: func(ctx context.Context, _ string, qc *quic.Config) (*quic.Conn, error) {
			return tr.Dial(ctx, pc.LocalAddr(), clientTLS("a", "b"), qc)
		}})
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { _, _ = io.Copy(s, s); _ = s.Close() })
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	echo := func() error {
		s, err := a.Open(ctx, "b", nil)
		if err != nil {
			return err
		}
		defer s.Close()
		if _, err := s.Write([]byte("x")); err != nil {
			return err
		}
		_, err = io.ReadFull(s, make([]byte, 1))
		return err
	}
	if err := echo(); err != nil {
		t.Fatal(err)
	}
	ended := make(chan struct{})
	s, err := a.Open(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	go func() { _, _ = io.Copy(io.Discard, s); close(ended) }()

	held := make(chan struct{})
	hc.mu.Lock()
	hc.held = held
	hc.mu.Unlock()
	starved := time.Now()
	ran := make(chan struct{})
	var once sync.Once
	quiclink.SetWatchTickHook(func(peer string) {
		if peer != "b" {
			return
		}
		once.Do(func() {
			// a's check does not run until well past Interval + Timeout/2;
			// quic-go reads again just after this tick.
			time.Sleep(time.Until(starved.Add(l.Interval + l.Timeout*5/8)))
			time.AfterFunc(50*time.Millisecond, func() { close(held) })
			close(ran)
		})
	})
	t.Cleanup(func() { quiclink.SetWatchTickHook(nil) })
	<-ran
	select {
	case <-ended:
		t.Fatal("a live peer taken for dead after the process ran again")
	case <-time.After(l.Interval + l.Timeout):
	}
	if err := echo(); err != nil {
		t.Fatalf("a new session after the starvation: %v", err)
	}
}
