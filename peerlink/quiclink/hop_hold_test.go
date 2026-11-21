// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/internal/linkhook"
	"github.com/nfltr/link/peerlink"
)

// readGate, once armed, holds the first bytes its conn reads until open
// closes (got closes when they came): its Mux reads nothing the far end
// sent meanwhile (peerlink's hop_hold_test.go readGate).
type readGate struct {
	net.Conn
	armed     atomic.Bool
	got, open chan struct{}
	once      sync.Once
}

func (g *readGate) Read(p []byte) (int, error) {
	n, err := g.Conn.Read(p)
	if n > 0 && g.armed.Load() {
		g.once.Do(func() { close(g.got) })
		<-g.open
	}
	return n, err
}

// writeSignal, once armed, closes wrote at its conn's next write.
type writeSignal struct {
	net.Conn
	armed atomic.Bool
	wrote chan struct{}
	once  sync.Once
}

func (w *writeSignal) Write(p []byte) (int, error) {
	n, err := w.Conn.Write(p)
	if w.armed.Load() {
		w.once.Do(func() { close(w.wrote) })
	}
	return n, err
}

// TestJoinsHoldOntoASequentialQuicApp (spec/HopHold.tla HopHoldSeq.cfg,
// spec/TraceHopHold.tla with SeqApp {"R"}, mutant hold-in-receipt; lane
// LINKGATE): two joints on one path, G (a TCP target and a link session)
// and R (the link session's peer and a QUIC session whose app reads only
// once its own CloseWriteSync returned). The app's FIN goes first; G's Mux
// reads nothing until R's Mux answered the target's FIN, so R holds the
// target's end while it syncs the app's. Run inside
// the sync's receipt callback (the code before lane HOLDCB), that held
// end waited for the app's read, which waited for the ENDACK the sync
// could not pass on: both waited out the idle bound. Under -tags
// linktrace with LINKTRACE_HOPHOLD the two joints and the app's read are
// recorded (make spec-traces, hopholdquic): the two-joint trace with a
// QUIC far end, which peerlink's own tests cannot build (quiclink's module
// requires link's).
func TestJoinsHoldOntoASequentialQuicApp(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	for _, name := range []string{"1", "2", "3"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ln, rn := tcpConns(t)
			gate := &readGate{Conn: rn, got: make(chan struct{}), open: make(chan struct{})}
			rw := &writeSignal{Conn: ln, wrote: make(chan struct{})}
			target, tAgent := tcpConns(t)
			app, in := openSent(t, a, acc, "down")
			t.Cleanup(traceHopHold(t.Name(), tAgent, in, app)) // -tags linktrace: spec/TraceHopHold.tla
			rJoin := make(chan error, 1)
			go func() {
				c, _, err := link.Server(ctx, rw, link.Config{Auth: link.NoAuth, Hop: true})
				if err != nil {
					rJoin <- err
					return
				}
				_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) { rJoin <- peerlink.Join(in, s) })
			}()
			gc, err := link.Client(ctx, gate, link.Config{Hop: true})
			if err != nil {
				t.Fatal(err)
			}
			gm := link.NewMux(gc)
			go func() { _ = gm.Serve(nil) }()
			t.Cleanup(func() { _ = gc.Close() })
			gs, err := gm.Open(ctx, []byte("h"))
			if err != nil {
				t.Fatal(err)
			}
			gJoin := make(chan error, 1)
			go func() { gJoin <- peerlink.Join(tAgent, gs) }()

			// bytes both ways first: the gate's first bytes are R's CLOSE
			roundTrip(t, target, app, "up")
			b := make([]byte, 4)
			if _, err := io.ReadFull(target, b); err != nil || string(b) != "down" {
				t.Fatalf("the target read %q, %v, want \"down\"", b, err)
			}
			tEnd := make(chan error, 1)
			go func() {
				b, err := io.ReadAll(target)
				if err == nil && len(b) != 0 {
					err = io.ErrUnexpectedEOF
				}
				tEnd <- err
			}()
			start := time.Now()
			gate.armed.Store(true)
			appSync := closeSync(app) // the app reads only once this returned
			select {
			case <-gate.got: // R's CLOSE (the app's end) reached G: R syncs it
			case <-time.After(10 * time.Second):
				t.Fatal("R passed no CLOSE on")
			}
			rw.armed.Store(true)
			if err := target.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-rw.wrote: // R's Mux answered G's CLOSE (the target's end) with the receipt
			case <-time.After(10 * time.Second):
				t.Fatal("R's Mux answered no CLOSE")
			}
			close(gate.open)
			if err := wait(t, appSync, "the app's CloseWriteSync"); err != nil {
				t.Fatalf("the app's CloseWriteSync: %v after %v, want nil", err, time.Since(start))
			}
			if d := time.Since(start); d > 10*time.Second {
				t.Fatalf("the app's CloseWriteSync took %v: R's held end waited for the app's read", d)
			}
			if b, err := io.ReadAll(app); err != nil || len(b) != 0 {
				t.Fatalf("the app read %q, %v, want the target's clean end", b, err)
			}
			if err := wait(t, tEnd, "the target's read"); err != nil {
				t.Fatalf("the target: %v", err)
			}
			for name, ch := range map[string]chan error{"G's join": gJoin, "R's join": rJoin} {
				if err := wait(t, ch, name); err != nil {
					t.Fatalf("%s: %v after %v", name, err, time.Since(start))
				}
			}
			_ = app.Close()
			t.Logf("both ends in %v", time.Since(start))
		})
	}
}

// traceHopHold is peerlink's two-joint recorder (-tags linktrace) for
// this module's tests: g, r the joints' app conns, far R's far end apps.
func traceHopHold(test string, g, r net.Conn, far ...net.Conn) func() {
	if linkhook.TraceHopHold == nil {
		return func() {}
	}
	return linkhook.TraceHopHold(test, g, r, far...)
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
