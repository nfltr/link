// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The spec review's findings F1-F5, F7 and F8 (spec/README.md "Review
// findings"): each test fails on the code before its fix. None waits a
// fixed time: each waits for the state it needs (a sender parked, a pong
// buffered), bounded only against a hang.

// gatedMuxPair returns the dialer's Mux, whose writes stop while g's gate
// is shut, its listener's, and g.
func gatedMuxPair(t *testing.T, accept func([]byte, *Session), dcfg, lcfg Config) (*Mux, *Mux, *gateConn) {
	t.Helper()
	a, b := tcpPair(t)
	g := &gateConn{Conn: a}
	lcc := make(chan *Conn, 1)
	go func() {
		c, _, _ := Server(context.Background(), b, lcfg)
		lcc <- c
	}()
	cc, err := Client(context.Background(), g, dcfg)
	if err != nil {
		t.Fatal(err)
	}
	lc := <-lcc
	if lc == nil {
		t.Fatal("no listener")
	}
	t.Cleanup(func() { _ = cc.Close(); _ = lc.Close() })
	dm, lm := NewMux(cc), NewMux(lc)
	go func() { _ = dm.Serve(nil) }()
	go func() { _ = lm.Serve(accept) }()
	return dm, lm, g
}

// waitConn waits until cond holds for c, read under wmu.
func waitConn(t *testing.T, c *Conn, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); runtime.Gosched() {
		c.wmu.Lock()
		ok := cond()
		c.wmu.Unlock()
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// stuckWriter fills c's write buffer behind a stuck network: s's frames
// are in the buffer and a write of b's waits for room with a ticket. It
// returns when b's write is parked, and a channel closed when b's Write
// returned (once the gate opens and the peer reads).
func stuckWriter(t *testing.T, g *gateConn, s, b *Session) <-chan struct{} {
	t.Helper()
	c := s.m.c
	_ = s.SetWriteDeadline(time.Now().Add(time.Minute)) // its flushes go to a goroutine
	if _, err := s.Write([]byte("x")); err != nil {     // taken by a write that blocks
		t.Fatal(err)
	}
	waitConn(t, c, "a write in flight", func() bool { return c.writing && c.wbuf == nil })
	if _, err := s.Write([]byte("y")); err != nil { // in the next buffer
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _ = b.Write(make([]byte, 2*writeBuf)); close(done) }()
	waitConn(t, c, "a sender waiting for room", func() bool { return len(c.waiters) == 1 })
	return done
}

// F1: a Write parked for its turn (frameLocked's turns) while CloseWrite
// buffers the session's CLOSE sends nothing after it: it returns
// io.ErrClosedPipe, and the peer reads exactly the bytes before the FIN.
// The code before checked only before the wait: Write returned 1, nil
// and the peer dropped the byte.
func TestWriteParkedForItsTurnSendsNothingAfterFIN(t *testing.T) {
	read := make(chan []byte, 1)
	dm, _, g := gatedMuxPair(t, func(h []byte, s *Session) {
		b, _ := io.ReadAll(s)
		if string(h) == "a" {
			read <- b
		}
		_ = s.Close()
	}, v2cfg, v2cfg)
	sa, err := dm.Open([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	sb, err := dm.Open([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	c := dm.c
	g.mu.Lock() // the network takes nothing from here
	bdone := stuckWriter(t, g, sa, sb)
	wrote := make(chan error, 1)
	go func() {
		_, err := sa.Write([]byte("z"))
		wrote <- err
	}()
	waitConn(t, c, "the Write parked for its turn", func() bool { return len(c.turns) == 1 && len(c.waiters) == 2 })
	go func() { _ = sa.CloseWrite() }()
	waitConn(t, c, "the CLOSE parked with a ticket", func() bool { return len(c.waiters) == 3 })
	g.mu.Unlock()
	if err := <-wrote; !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write racing CloseWrite: %v, want io.ErrClosedPipe", err)
	}
	if b := <-read; string(b) != "xy" {
		t.Fatalf("peer read %q, want \"xy\"", b)
	}
	<-bdone // the trace ends with the sessions, not under them
	_ = sb.Close()
}

// F2: an OPEN waiting for its turn when the peer's GOAWAY and drain ping
// arrive is not sent after the pong (which never waits): Open returns
// ErrGoAway. The code before checked only before the wait: the peer read
// the pong (drained), then the OPEN, and refused it as a broken protocol.
func TestOpenParkedPastPeerGoAwayIsNotSent(t *testing.T) {
	dm, lm, g := gatedMuxPair(t, func(_ []byte, s *Session) { _, _ = io.Copy(io.Discard, s); _ = s.Close() }, v2cfg, v2cfg)
	sa, err := dm.Open([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	sb, err := dm.Open([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	c := dm.c
	g.mu.Lock()
	bdone := stuckWriter(t, g, sa, sb)
	opened := make(chan error, 1)
	go func() {
		_, err := dm.Open([]byte("late"))
		opened <- err
	}()
	waitConn(t, c, "the OPEN parked with a ticket", func() bool { return len(c.waiters) == 2 })
	if err := lm.goAway(time.Time{}); err != nil {
		t.Fatal(err)
	}
	waitConn(t, c, "the drain ping's pong buffered", func() bool { return c.pongEnd > 0 })
	g.mu.Unlock()
	if err := <-opened; !errors.Is(err, ErrGoAway) {
		t.Fatalf("Open parked past the peer's GOAWAY: %v, want ErrGoAway", err)
	}
	// The drain ends the connection once the sessions ended (a clean
	// trace: no close under the leaving side).
	<-bdone
	_, _ = sa.Close(), sb.Close()
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the drain did not end the connection")
	}
}

// F3: an Abort at the peer's max sessions racing an Open keeps the
// connection: the session counts until its ABORT is buffered, so the
// OPEN goes after it. The code before forgot the session before it took
// wmu: the OPEN went first, past the peer's max, and the peer ended the
// connection (ErrFull).
func TestAbortAtMaxSessionsRacingOpenKeepsTheConnection(t *testing.T) {
	lcfg := v2cfg
	lcfg.MaxSessions = 1
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) { _, _ = io.Copy(s, s); _ = s.Close() }, v2cfg, lcfg)
	s, err := dm.Open([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan error, 1)
	var sb *Session
	testHookAbortForgot = func() {
		go func() {
			var err error
			sb, err = dm.Open([]byte("b"))
			opened <- err
		}()
		// The Open either buffered its OPEN (the code before) or waits for wmu.
		waitStack(t, "link.(*Conn).sendOpen", "sync.(*Mutex).Lock", opened)
	}
	t.Cleanup(func() { testHookAbortForgot = nil })
	_ = s.Abort(1, "gone")
	testHookAbortForgot = nil
	if err := <-opened; err != nil {
		t.Fatalf("Open after the Abort: %v", err)
	}
	if _, err := sb.Write([]byte("echo")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4)
	if _, err := io.ReadFull(sb, b); err != nil || string(b) != "echo" {
		t.Fatalf("echo on the second session: %q, %v (the connection lives)", b, err)
	}
}

// waitStack waits until a goroutine's stack holds both fn and in, or done
// has a value (put back).
func waitStack(t *testing.T, fn, in string, done chan error) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		select {
		case err := <-done:
			done <- err
			return
		default:
		}
		for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if strings.Contains(g, fn) && strings.Contains(g, in) {
				return
			}
		}
	}
	t.Fatalf("no goroutine in %s", fn)
}

// F4: a Mux takes as many sessions as the connection allows each
// direction (Config.MaxSessions), not its own 1024 for both.
func TestMuxHonoursConfigMaxSessions(t *testing.T) {
	const n = MaxSessions + 100
	cfg := v2cfg
	cfg.MaxSessions = n
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	var accepted atomic.Int64
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) {
		accepted.Add(1)
		<-hold
		_ = s.Close()
	}, cfg, cfg)
	for i := range n {
		if _, err := dm.Open([]byte("a")); err != nil {
			t.Fatalf("session %d of %d: %v", i, n, err)
		}
	}
	if _, err := dm.Open([]byte("a")); !errors.Is(err, ErrFull) {
		t.Fatalf("session past Config.MaxSessions: %v, want ErrFull", err)
	}
	for end := time.Now().Add(10 * time.Second); accepted.Load() < n && time.Now().Before(end); time.Sleep(time.Millisecond) {
	}
	if a := accepted.Load(); a != n {
		t.Fatalf("peer accepted %d sessions, want %d", a, n)
	}
}

// F5: an open of an id that is open is refused, by its sender and, from
// a peer that sends one anyway, as a protocol error (WIRE.md section 5).
// The code before restarted the session: the old one's bytes in flight
// were charged to the new one.
func TestOpenOfAnOpenIDIsRefused(t *testing.T) {
	c, l := v2pair(t, v2cfg, v2cfg)
	if err := c.SendOpen("1", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.SendOpen("1", nil); !errors.Is(err, errSIDInUse) {
		t.Fatalf("second SendOpen of an open id: %v, want errSIDInUse", err)
	}
	c.wmu.Lock() // a peer that sends one anyway
	err := c.frameLocked(typeOpen, "1", nil, nil)
	c.wmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Flush()
	if f, err := l.Recv(); err != nil || !f.Open {
		t.Fatalf("first open: %+v, %v", f, err)
	}
	if f, err := l.Recv(); !errors.Is(err, ErrFrame) {
		t.Fatalf("open of an open id: %+v, %v; want ErrFrame", f, err)
	}
}

// F7: the receive loop goes on reading while a refusal it owes cannot be
// written (the network takes nothing from this side): refusals go out
// from a goroutine. The code before wrote the refusal inline, and the
// receive loop read nothing more until the network took it.
func TestReceiveLoopReadsWhileARefusalWaits(t *testing.T) {
	dm, lm, g := gatedMuxPair(t, nil, v2cfg, v2cfg) // the dialer refuses every open
	_ = lm
	c := dm.c
	g.mu.Lock()
	t.Cleanup(g.mu.Unlock)
	if _, err := lm.Open([]byte("refused")); err != nil {
		t.Fatal(err)
	}
	s, err := lm.Open([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	// The second OPEN and its data reach the dialer only if its receive
	// loop did not wait on the first refusal's write.
	for end := time.Now().Add(10 * time.Second); c.Stats().SessionsOpened < 2; time.Sleep(time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("the receive loop stopped at a refusal it could not write")
		}
	}
}

// F9 (lane LINKF7, the yamux comparison): CloseWrite reports a FIN it
// could not write, as Write reports its bytes: the connection's writes
// failed, so the peer never reads this side's io.EOF. The code before
// returned nil (Mux.sendClose dropped the error).
func TestCloseWriteReportsAFailedFIN(t *testing.T) {
	dm, _, g := gatedMuxPair(t, func(_ []byte, s *Session) { <-s.Done() }, v2cfg, v2cfg)
	s, err := dm.Open([]byte("fin"))
	if err != nil {
		t.Fatal(err)
	}
	g.broken.Store(true)
	if err := s.CloseWrite(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("CloseWrite with the connection's writes failing: %v, want io.ErrClosedPipe", err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatalf("a second CloseWrite: %v, want nil (the write side is closed)", err)
	}
}

// F8: a peer's window update past the peer's window, counting the credit
// of a Write between its acquire and its frame, is ErrCredit.
func TestGrantCountsCreditInAWrite(t *testing.T) {
	cr := NewCredit(testWindow, testWindow, func(string, int) {})
	cr.open("1", byUs)
	if ok, _ := cr.acquire("1", MaxChunk, nil, nil, true); !ok {
		t.Fatal("no credit")
	}
	if err := cr.Grant("1", MaxChunk); !errors.Is(err, ErrCredit) {
		t.Fatalf("a grant of credit a Write holds: %v, want ErrCredit", err)
	}
	cr.refund("1", MaxChunk)
	if err := cr.Grant("1", 1); !errors.Is(err, ErrCredit) {
		t.Fatalf("a grant past the window after the refund: %v, want ErrCredit", err)
	}
}

// F5 kept Credit.Open's order: a Conn caller opens a session's credit, then
// sends its OPEN (relay-core's data connections). That id is not in use:
// SendOpen takes the caller's credit over, bytes acquired meanwhile still
// counted. An id the link opened stays refused, even after the caller's
// Open of it.
func TestSendOpenAfterCreditOpen(t *testing.T) {
	c, l := v2pair(t, v2cfg, v2cfg)
	c.Credit().Open("1")
	if !c.Credit().Acquire("1", c.Credit().PeerWindow()) {
		t.Fatal("Acquire of the caller's credit")
	}
	if err := c.SendOpen("1", nil); err != nil {
		t.Fatalf("SendOpen after the caller's Credit.Open: %v", err)
	}
	if n := c.credit.sessions["1"].n; n != 0 {
		t.Fatalf("credit after SendOpen = %d, want 0: the acquired window still counts", n)
	}
	_ = c.Flush()
	if f, err := l.Recv(); err != nil || !f.Open {
		t.Fatalf("open: %+v, %v", f, err)
	}
	c.Credit().Open("1")
	if err := c.SendOpen("1", nil); !errors.Is(err, errSIDInUse) {
		t.Fatalf("SendOpen of a link-opened id after Credit.Open: %v, want errSIDInUse", err)
	}
}

// LINKDROPSPLIT: a CloseWrite that ends both halves (the peer's FIN read
// first) forgets the session's credit before its CLOSE can reach the
// peer: the peer drops the session on that CLOSE and may open another at
// once, which a receiver still counting the old one refused with ErrFull
// (the connection's end). The code before forgot it in Mux.drop, after the
// CLOSE (spec/Link.tla CloseWriteSend, mutant drop-forgets-late).
func TestCloseWriteEndingBothHalvesFreesItsSessionAtOnce(t *testing.T) {
	lcfg := v2cfg
	lcfg.MaxSessions = 1
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) { _, _ = io.Copy(s, s); _ = s.CloseWrite() }, v2cfg, lcfg)
	sa, err := dm.Open([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sa.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	var once atomic.Bool
	h := func() {
		if once.Swap(true) {
			return
		}
		func() {
			select {
			case <-sa.Done(): // the dialer read the listener's CLOSE
			case <-time.After(10 * time.Second):
				got <- errors.New("session a never ended at the dialer")
				return
			}
			sb, err := dm.Open([]byte("b"))
			if err == nil {
				_, err = sb.Write([]byte("echo"))
			}
			if err == nil {
				err = sb.CloseWrite()
			}
			if err == nil {
				var b []byte
				if b, err = io.ReadAll(sb); err == nil && string(b) != "echo" {
					err = fmt.Errorf("echo %q", b)
				}
			}
			got <- err
		}()
	}
	testHookCloseWriteSent.Store(&h)
	t.Cleanup(func() { testHookCloseWriteSent.Store(nil) })
	if _, err := io.ReadAll(sa); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("a second session after the first ended: %v (the connection lives)", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no second session")
	}
}
