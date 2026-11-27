// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"context"
	"io"
	"net"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/internal/linkhook"
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
// the 30 s idle bound (ErrEndAckIdle). The crossing case also guards
// mutant held-end-no-ack: a held end whose session ended meanwhile,
// passed onto a TCP conn without ackEnd (joint.ended's Done branch), left
// the agent's join waiting for the idle bound (lane ENDACKCODE: with that
// ackEnd removed, crossing fails at 30.4 s, the agent's join reset).
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
			t.Cleanup(peerlink.TraceHopHold(t.Name(), tAgent, vRelay)) // -tags linktrace: spec/TraceHopHold.tla
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

// TestHopHoldTraceEventsMapped: the events the hop recorder writes
// (trace_on.go's list, each written by a traceJoin call of chain.go, and
// traceHopRead's AppRead, a far end app's read) are
// spec/TraceHopHold.tla's steps, and Lost, which is none (nothing fails in
// HopHold.tla: a trace with it is rejected).
func TestHopHoldTraceEventsMapped(t *testing.T) {
	read := func(f string) string {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	list := regexp.MustCompile(`case ("[A-Za-z]+"(?:, "[A-Za-z]+")*):\n\t\tif t\.hop`).FindStringSubmatch(read("trace_on.go"))
	if list == nil {
		t.Fatal("trace_on.go: the hop recorder's list of events not found")
	}
	var code []string
	on := read("trace_on.go")
	for _, m := range regexp.MustCompile(`"ev": "([A-Za-z]+)"`).FindAllStringSubmatch(on, -1) {
		code = append(code, m[1]) // traceHopRead's
	}
	chain := read("chain.go")
	for _, e := range strings.Split(list[1], ", ") {
		e = strings.Trim(e, `"`)
		if !strings.Contains(chain, `traceJoin(j, "`+e+`"`) {
			t.Errorf("the hop recorder lists %q, which no traceJoin call of chain.go writes", e)
		}
		if e != "Lost" {
			code = append(code, e)
		}
	}
	var spec []string
	for _, m := range regexp.MustCompile(`E\.ev = "([A-Za-z]+)"`).FindAllStringSubmatch(read("../spec/TraceHopHold.tla"), -1) {
		spec = append(spec, m[1])
	}
	slices.Sort(code)
	slices.Sort(spec)
	if !slices.Equal(code, spec) {
		t.Errorf("the hop recorder writes %v; spec/TraceHopHold.tla maps %v", code, spec)
	}
}

// readGate, once armed, holds the first bytes its conn reads until open
// closes (got closes when they came): the far end's Mux reads nothing
// the joint sent meanwhile.
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

// A joint between two link sessions (a hop's: Config.Hop; one of them
// wrapped, as relay-core's metered agent session is, so peerlink's own
// joint runs) whose far ends half-close crossing: B first, its FIN passed
// onto A's session, whose far end reads it only once A's FIN reached the
// joint, held while the joint syncs. Both far ends' CloseWriteSync
// return nil: the held end, run at A's receipt, closed B's session whole
// before the joint passed on the ENDACK of B's own end, and B's peer
// read that close as a reset (spec/HopHold.tla CloseAfterAck, mutant
// close-at-end; lane PIPERESET: nfltr-pipe's relayed listener exited 6).
// The far ends read to their end, as a program does: each acknowledges
// the other's end only then (lane ENDACK).
func TestHopClosesASessionOnlyOnceItsEndIsAcknowledged(t *testing.T) {
	ctx := context.Background()
	joined := make(chan *link.Session, 2)
	hop := func(c net.Conn) {
		lc, _, err := link.Server(ctx, c, link.Config{Auth: link.NoAuth, Hop: true})
		if err != nil {
			_ = c.Close()
			return
		}
		_ = link.NewMux(lc).Serve(func(_ []byte, s *link.Session) { joined <- s })
	}
	far := func(c net.Conn) *link.Session {
		lc, err := link.Client(ctx, c, link.Config{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = lc.Close() })
		m := link.NewMux(lc)
		go func() { _ = m.Serve(nil) }()
		s, err := m.Open(ctx, []byte("h"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	an, aj := tcpPair(t)
	bn, bj := tcpPair(t)
	go hop(aj)
	go hop(bj)
	gate := &readGate{Conn: an, got: make(chan struct{}), open: make(chan struct{})}
	a := far(gate)
	sa := <-joined
	b := far(bn)
	sb := <-joined
	join := make(chan error, 1)
	go func() { join <- peerlink.Join(sa, wrapped{sb}) }()
	roundTrip(t, a, b, "up")
	roundTrip(t, b, a, "down")

	for _, s := range []*link.Session{a, b} { // the far programs read to the end: their ENDACKs go then (lane ENDACK)
		go func() { _, _ = io.Copy(io.Discard, s) }()
	}
	gate.armed.Store(true)
	bEnd := make(chan error, 1)
	go func() { bEnd <- b.CloseWriteSync() }()
	<-gate.got // the joint passed B's FIN onto A's session and syncs
	aEnd := make(chan error, 1)
	go func() { aEnd <- a.CloseWriteSync() }()
	time.Sleep(100 * time.Millisecond) // A's FIN reaches the joint, held (makes the race certain)
	close(gate.open)
	for name, ch := range map[string]chan error{"A's CloseWriteSync": aEnd, "B's CloseWriteSync": bEnd, "the join": join} {
		if err := wait(t, ch, name); err != nil {
			t.Fatalf("%s: %v, want nil (every byte and end was taken)", name, err)
		}
	}
}

// wrapped is a link session behind another type, as relay-core's
// metered conn: peerlink's own joint, not link's.
type wrapped struct{ *link.Session }

// syncTCP is a TCP app conn that syncs its end (a conn passing
// CloseWriteSync on, as tlsend's): Join passes an end onto it with
// CloseWriteSync, not CloseWrite.
type syncTCP struct{ *net.TCPConn }

// wrappedSession is a link session behind another type (relay-core's
// metered conn, a budgeted leg): every method passed on, and Unwrap
// (Join's doc; lane LINKWRAP). Without Unwrap Join took it for a plain
// conn: its end onto a syncing conn passed unheld, its connection's end
// unwatched.
type wrappedSession struct{ *link.Session }

func (w wrappedSession) Unwrap() net.Conn { return w.Session }

// sessionWrappers are the ways a joint sees a link session: itself, and
// behind a wrapper that unwraps to it.
var sessionWrappers = []struct {
	name string
	wrap func(*link.Session) net.Conn
}{
	{"session", func(s *link.Session) net.Conn { return s }},
	{"wrapped", func(s *link.Session) net.Conn { return wrappedSession{s} }},
}

func (s syncTCP) CloseWriteSync() error { return s.CloseWrite() }

// jointGate sees the joints' steps (internal/linkhook.Joint): the first
// sync of app's end waits in synced until open closes; held closes at a
// joint's hold, unheld at a sync of any other conn's end but via's
// (G's: the target's).
type jointGate struct {
	synced, open, held, unheld chan struct{}
	once                       sync.Once
}

func (g *jointGate) release() { g.once.Do(func() { close(g.open) }) }

func gateJoints(t *testing.T, app, via net.Conn) *jointGate {
	g := &jointGate{synced: make(chan struct{}), open: make(chan struct{}), held: make(chan struct{}), unheld: make(chan struct{})}
	var first, h, u sync.Once
	f := func(ev string, src net.Conn) {
		switch {
		case ev == "held":
			h.Do(func() { close(g.held) })
		case src == app:
			first.Do(func() { close(g.synced); <-g.open })
		case src != via:
			u.Do(func() { close(g.unheld) })
		}
	}
	linkhook.Joint.Store(&f)
	t.Cleanup(func() {
		linkhook.Joint.Store(nil)
		g.release()
	})
	return g
}

// TestJoinHoldsAnEndOntoASyncingApp (spec/HopHold.tla HopHoldConc.cfg,
// mutant sync-no-hold; lane LINKHOLD2): two joints, G (a TCP target and a
// link session) and R (that session's peer and an app conn that syncs its
// end too). The app half-closes first and reads on; R's sync of that end
// waits before its CloseWrite (jointGate), so R's session has not ended
// whole when the target's FIN reaches R. R must hold that end while it
// syncs the app's: the code before passed it with a sync of its own, and
// the app read the peer's EOF with its own FIN still at R.
func TestJoinHoldsAnEndOntoASyncingApp(t *testing.T) {
	for _, w := range sessionWrappers {
		t.Run(w.name, func(t *testing.T) { testJoinHoldsAnEndOntoASyncingApp(t, w.wrap) })
	}
}

func testJoinHoldsAnEndOntoASyncingApp(t *testing.T, wrap func(*link.Session) net.Conn) {
	ctx := context.Background()
	gl, rl := tcpPair(t)
	target, tAgent := tcpPair(t)
	app, rApp := tcpPair(t)
	appSide := syncTCP{rApp}
	rJoin := make(chan error, 1)
	go func() {
		c, _, err := link.Server(ctx, rl, link.Config{Auth: link.NoAuth, Hop: true})
		if err != nil {
			rJoin <- err
			return
		}
		_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) { rJoin <- peerlink.Join(appSide, wrap(s)) })
	}()
	gc, err := link.Client(ctx, gl, link.Config{Hop: true})
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
	roundTrip(t, target, app, "up")
	roundTrip(t, app, target, "down")
	appEnd, tEnd := readEnd(app), readEnd(target)

	g := gateJoints(t, appSide, tAgent)
	if err := app.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	<-g.synced // R syncs the app's end: its CloseWrite waits
	if err := target.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-appEnd:
		t.Fatalf("the app's reads ended (%v) while its own FIN was still at R: R passed the target's end unheld", err)
	case <-g.unheld:
		t.Fatal("R passed the target's end onto the app with a sync of its own, unheld, while the app's end syncs")
	case <-g.held:
	case <-time.After(10 * time.Second):
		t.Fatal("R neither held nor passed the target's end")
	}
	g.release()
	for name, ch := range map[string]chan error{"the app's read": appEnd, "the target's read": tEnd, "G's join": gJoin, "R's join": rJoin} {
		if err := wait(t, ch, name); err != nil {
			t.Fatalf("%s: %v, want nil", name, err)
		}
	}
}

// readEnd reads c to its end: nil at a clean end with nothing more read.
func readEnd(c net.Conn) chan error {
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
