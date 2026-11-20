// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// slowConn delays each write: two FINs written at once cross on the link.
type slowConn struct{ net.Conn }

func (s slowConn) Write(p []byte) (int, error) {
	time.Sleep(50 * time.Millisecond)
	return s.Conn.Write(p)
}

// TestJoinCrossingHalfClosesEnd (spec/HopHold.tla, lane ENDACKJOIN): two
// joints on one relayed path (the agent's, target <-> session; the
// relay's, session <-> visitor), both TCP apps half-closing. Each app
// reads the other's bytes and a clean io.EOF, and both joins return nil.
// Crossing FINs held each joint's end until the other's ENDACK, which
// that one held in turn (mutant hold-until-ack): both apps were reset at
// the 30 s idle bound (ErrEndAckIdle).
func TestJoinCrossingHalfClosesEnd(t *testing.T) {
	for _, tc := range []struct {
		name     string
		crossing bool
	}{{"crossing", true}, {"sequential", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ln, rn := tcpPair(t)
			visitor, vRelay := tcpPair(t)
			target, tAgent := tcpPair(t)
			rJoin := make(chan error, 1)
			go func() {
				c, _, err := link.Server(ctx, slowConn{ln}, link.Config{Auth: link.NoAuth, Hop: true})
				if err != nil {
					rJoin <- err
					return
				}
				_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) { rJoin <- peerlink.Join(vRelay, s) })
			}()
			gc, err := link.Client(ctx, slowConn{rn}, link.Config{Hop: true})
			if err != nil {
				t.Fatal(err)
			}
			gm := link.NewMux(gc)
			go func() { _ = gm.Serve(nil) }()
			t.Cleanup(func() { _ = gc.Close() })
			s, err := gm.Open(ctx, []byte("h"))
			if err != nil {
				t.Fatal(err)
			}
			gJoin := make(chan error, 1)
			go func() { gJoin <- peerlink.Join(tAgent, s) }()

			// both joins carry bytes before the FINs
			roundTrip(t, target, visitor, "up")
			roundTrip(t, visitor, target, "down")

			start := time.Now()
			readEnd := func(c *net.TCPConn) chan error {
				ch := make(chan error, 1)
				go func() {
					b, err := io.ReadAll(c)
					if err == nil && len(b) != 0 {
						err = io.ErrUnexpectedEOF
					}
					ch <- err
				}()
				return ch
			}
			if err := target.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			vEnd := readEnd(visitor)
			if !tc.crossing {
				if err := wait(t, vEnd, "visitor's EOF"); err != nil {
					t.Fatalf("visitor: %v", err)
				}
			}
			if err := visitor.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			tEnd := readEnd(target)
			if tc.crossing {
				if err := wait(t, vEnd, "visitor's EOF"); err != nil {
					t.Fatalf("visitor: %v after %v", err, time.Since(start))
				}
			}
			if err := wait(t, tEnd, "target's EOF"); err != nil {
				t.Fatalf("target: %v after %v", err, time.Since(start))
			}
			for name, ch := range map[string]chan error{"agent's join": gJoin, "relay's join": rJoin} {
				if err := wait(t, ch, name); err != nil {
					t.Fatalf("%s: %v after %v", name, err, time.Since(start))
				}
			}
			t.Logf("both ends in %v", time.Since(start))
		})
	}
}

// roundTrip writes msg on from and reads it at to.
func roundTrip(t *testing.T, from, to net.Conn, msg string) {
	t.Helper()
	if _, err := from.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, len(msg))
	if _, err := io.ReadFull(to, b); err != nil || string(b) != msg {
		t.Fatalf("read %q, %v; want %q", b, err, msg)
	}
}

// wait is ch's result, bounded only against a hang.
func wait(t *testing.T, ch chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(90 * time.Second):
		t.Fatalf("%s: hang", what)
		return nil
	}
}
