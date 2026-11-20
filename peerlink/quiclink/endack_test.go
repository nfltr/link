// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
)

// endAckPair is a dialling a and a listening b (b's TLS: tlsB, nil for
// serverTLS's: offering ALPNEndAck first), a offering protos; idle is
// a's CloseWriteSync idle bound (0: 30 s). b's accepted sessions arrive
// on the channel.
func endAckPair(t *testing.T, idle time.Duration, tlsB *tls.Config, protos ...string) (a, b *quiclink.Node, acc chan peerlink.Session) {
	t.Helper()
	pc := listenUDP(t)
	if tlsB == nil {
		tlsB = serverTLS("b")
	}
	b = mustNew(t, quiclink.Config{PacketConn: pc, TLS: tlsB, Liveness: link.LivenessFast})
	a = mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String(), protos...), Liveness: link.LivenessFast})
	if idle > 0 {
		quiclink.SetEndAckIdleForTest(a, idle)
	}
	acc = make(chan peerlink.Session, 4)
	go func() { _ = b.Serve(func(_ string, _ []byte, s peerlink.Session) { acc <- s }) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = a.Drain(ctx)
		_ = b.Drain(ctx)
	})
	return a, b, acc
}

var endAckProtos = []string{quiclink.ALPNEndAck, quiclink.ALPNChain, quiclink.ALPN}

// openSent opens a session from a, writes msg and returns it with b's
// accepted end.
func openSent(t *testing.T, a *quiclink.Node, acc chan peerlink.Session, msg string) (*quiclink.Session, *quiclink.Session) {
	t.Helper()
	s, err := a.Open(context.Background(), "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-acc:
		return s.(*quiclink.Session), r.(*quiclink.Session)
	case <-time.After(10 * time.Second):
		t.Fatal("no session accepted")
		return nil, nil
	}
}

func closeSync(s *quiclink.Session) chan error {
	ch := make(chan error, 1)
	go func() { ch <- s.CloseWriteSync() }()
	return ch
}

// TestEndAckCloseWaitsForFarEnd (spec/EndAckLeg.tla Kind quic, mutant
// ack-at-write: CloseWrite, the code before lane ENDACKQUIC):
// CloseWriteSync returns nil only once the far end read every byte and
// the FIN.
func TestEndAckCloseWaitsForFarEnd(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	s, r := openSent(t, a, acc, "tail")
	if !s.AcksEnds() || !r.AcksEnds() {
		t.Fatal("ALPNEndAck not negotiated")
	}
	start := time.Now()
	done := closeSync(s)
	time.Sleep(300 * time.Millisecond) // the far end reads nothing meanwhile
	select {
	case err := <-done:
		t.Fatalf("CloseWriteSync returned %v before the far end read", err)
	default:
	}
	if b, err := io.ReadAll(r); err != nil || string(b) != "tail" {
		t.Fatalf("read %q, %v", b, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("CloseWriteSync: %v", err)
	}
	t.Logf("acknowledged after %v", time.Since(start))
}

// TestEndAckMuteFarEndIdles (IdleTruth, WaitEnds): a far end that reads
// nothing gives the wait up at the idle bound, though its connection
// stays up, and the session is reset.
func TestEndAckMuteFarEndIdles(t *testing.T) {
	a, _, acc := endAckPair(t, 300*time.Millisecond, nil, endAckProtos...)
	s, r := openSent(t, a, acc, "tail")
	quiclink.TraceMuteForTest(r)
	start := time.Now()
	if err := s.CloseWriteSync(); !errors.Is(err, link.ErrEndAckIdle) {
		t.Fatalf("CloseWriteSync: %v, want ErrEndAckIdle", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("gave up after %v", d)
	}
}

// TestEndAckConnectionEndFailsTheWait (spec/EndAckLeg.tla Cut): the
// connection ends while the far end has not read the end: the wait fails
// with link.ErrConnClosed at once, not at the idle bound.
func TestEndAckConnectionEndFailsTheWait(t *testing.T) {
	a, b, acc := endAckPair(t, 0, nil, endAckProtos...)
	s, _ := openSent(t, a, acc, "tail")
	done := closeSync(s)
	time.Sleep(100 * time.Millisecond) // the wait runs
	cut, cancel := context.WithCancel(context.Background())
	cancel()
	_ = b.Drain(cut) // closes the connection (CodeDrainDeadline)
	if err := wait(t, done, "CloseWriteSync"); !errors.Is(err, link.ErrConnClosed) {
		t.Fatalf("CloseWriteSync: %v, want link.ErrConnClosed", err)
	}
}

// TestEndAckCloseBeforeReadFails (mutant close-acks): a far end that
// closes before it read the end says 'A': the end is not known
// delivered.
func TestEndAckCloseBeforeReadFails(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	s, r := openSent(t, a, acc, "tail")
	done := closeSync(s)
	_ = r.Close()
	if err := <-done; !errors.Is(err, link.ErrReset) {
		t.Fatalf("CloseWriteSync: %v, want link.ErrReset", err)
	}
}

// TestEndAckFarEndResetFails (spec/EndAckLeg.tla RFail): a far end that
// resets the session before it read the end says 'A' on the control
// stream: the wait fails with link.ErrReset, not at the idle bound.
func TestEndAckFarEndResetFails(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	s, r := openSent(t, a, acc, "tail")
	done := closeSync(s)
	time.Sleep(50 * time.Millisecond) // the wait runs
	_ = r.Reset()
	if err := wait(t, done, "CloseWriteSync"); !errors.Is(err, link.ErrReset) {
		t.Fatalf("CloseWriteSync: %v, want link.ErrReset", err)
	}
}

// TestEndAckResetAfterAckIsClean (ErrTruth, mutant wait-on-stream-reset):
// a far end that read the end, then reset the session: its RESET_STREAM
// may overtake the 'E' on the control stream; the wait reads the 'E'.
func TestEndAckResetAfterAckIsClean(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	for i := range 20 {
		s, r := openSent(t, a, acc, "tail")
		done := closeSync(s)
		if b, err := io.ReadAll(r); err != nil || string(b) != "tail" {
			t.Fatalf("%d: read %q, %v", i, b, err)
		}
		_ = r.Reset()
		if err := <-done; err != nil {
			t.Fatalf("%d: CloseWriteSync: %v", i, err)
		}
		_ = s.Close()
	}
}

// TestEndAckOldPeer: against a peer without ALPNEndAck (an older
// dialler, an older listener) the connection negotiates ALPNChain and
// CloseWriteSync is CloseWrite, as before; no 'R', 'E' or 'A' reaches the
// older peer's control stream, so its Drain ('G', 'D') still ends clean.
func TestEndAckOldPeer(t *testing.T) {
	oldListener := serverTLS("b")
	oldListener.NextProtos = []string{quiclink.ALPNChain, quiclink.ALPN}
	for name, tc := range map[string]struct {
		tlsB   *tls.Config
		protos []string
	}{
		"old dialler":  {nil, []string{quiclink.ALPNChain, quiclink.ALPN}},
		"old listener": {oldListener, endAckProtos},
	} {
		t.Run(name, func(t *testing.T) {
			a, b, acc := endAckPair(t, 0, tc.tlsB, tc.protos...)
			s, r := openSent(t, a, acc, "tail")
			if s.AcksEnds() || r.AcksEnds() || !s.Answers() {
				t.Fatalf("AcksEnds %v/%v, Answers %v: want ALPNChain", s.AcksEnds(), r.AcksEnds(), s.Answers())
			}
			if err := s.CloseWriteSync(); err != nil { // returns once written: nothing read yet
				t.Fatalf("CloseWriteSync: %v", err)
			}
			if b, err := io.ReadAll(r); err != nil || string(b) != "tail" {
				t.Fatalf("read %q, %v", b, err)
			}
			_ = r.Close()
			_ = s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := b.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v", err)
			}
		})
	}
}

// tcpConns is a connected TCP pair.
func tcpConns(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ch <- c
	}()
	d, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c := <-ch
	t.Cleanup(func() { _ = d.Close(); _ = c.Close() })
	return d.(*net.TCPConn), c.(*net.TCPConn)
}

// TestEndAckRelayedCloseThenExit (spec/RelayedClose.tla CloseTruth over
// a QUIC agent link): an agent's session over QUIC, joined at the relay
// (peerlink.Join, a hop) to a client's link session. The agent's
// CloseWriteSync returns nil only once the client read every byte and the
// FIN; the agent exits at once after (its Node cut), and the client has
// the whole tail.
func TestEndAckRelayedCloseThenExit(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	ctx := context.Background()
	cr, rc := tcpConns(t)
	client := make(chan *link.Session, 1)
	go func() {
		c, _, err := link.Server(ctx, cr, link.Config{Auth: link.NoAuth})
		if err != nil {
			return
		}
		_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) { client <- s })
	}()
	rconn, err := link.Client(ctx, rc, link.Config{Hop: true})
	if err != nil {
		t.Fatal(err)
	}
	rm := link.NewMux(rconn)
	go func() { _ = rm.Serve(nil) }()
	t.Cleanup(func() { _ = rconn.Close() })

	tail := bytes.Repeat([]byte("x"), 8<<20)
	s, err := a.Open(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	in := (<-acc).(*quiclink.Session)
	out, err := rm.Open(ctx, []byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan error, 1)
	go func() { joined <- peerlink.Join(in, out) }()
	done := make(chan error, 1)
	go func() { // the tail is over the client's window: its Mux holds it only as the client reads
		if _, err := s.Write(tail); err != nil {
			done <- err
			return
		}
		done <- s.(*quiclink.Session).CloseWriteSync()
	}()
	cs := <-client
	time.Sleep(300 * time.Millisecond) // the client reads nothing meanwhile
	select {
	case err := <-done:
		t.Fatalf("CloseWriteSync returned %v before the client read", err)
	default:
	}
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(cs)
		got <- b
	}()
	if err := <-done; err != nil {
		t.Fatalf("CloseWriteSync: %v", err)
	}
	cut, cancel := context.WithCancel(ctx)
	cancel()
	_ = a.Drain(cut) // the agent exits
	if b := <-got; !bytes.Equal(b, tail) {
		t.Fatalf("client read %d bytes of %d", len(b), len(tail))
	}
	_ = cs.Close()
	if err := wait(t, joined, "relay's join"); err != nil {
		t.Logf("relay's join after the agent's exit: %v", err)
	}
}

// TestEndAckJointsCrossingOverQUIC (spec/HopHold.tla, mutant
// hold-until-ack, over a QUIC leg): two joints, the agent's (TCP target
// <-> QUIC session) and the relay's (QUIC session <-> TCP visitor), both
// apps half-closing at once. A joint whose hold waited for the far end's
// 'E' rather than the next hop's 'R' held the end the other's 'E' waited
// for: both apps reset at the idle bound.
func TestEndAckJointsCrossingOverQUIC(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	visitor, vRelay := tcpConns(t)
	target, tAgent := tcpConns(t)
	s, err := a.Open(context.Background(), "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	gJoin := make(chan error, 1)
	go func() { gJoin <- peerlink.Join(tAgent, s) }()
	if _, err := target.Write([]byte("up")); err != nil { // the open reaches b
		t.Fatal(err)
	}
	rJoin := make(chan error, 1)
	go func() { rJoin <- peerlink.Join(vRelay, <-acc) }()
	b := make([]byte, 2)
	if _, err := io.ReadFull(visitor, b); err != nil || string(b) != "up" {
		t.Fatalf("visitor read %q, %v", b, err)
	}

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
	_ = target.CloseWrite()
	_ = visitor.CloseWrite()
	for name, ch := range map[string]chan error{"visitor's EOF": readEnd(visitor), "target's EOF": readEnd(target),
		"agent's join": gJoin, "relay's join": rJoin} {
		if err := wait(t, ch, name); err != nil {
			t.Fatalf("%s: %v after %v", name, err, time.Since(start))
		}
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("both ends after %v: the joints waited for each other", d)
	}
	t.Logf("both ends in %v", time.Since(start))
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

// TestEndAckTraceEventsMapped: every event the end handshake's recorder
// writes (traceEA, failEnd's, TraceMuteForTest's) is a step of link's
// spec/TraceEndAckLeg.tla, and each of its steps has a hook point.
func TestEndAckTraceEventsMapped(t *testing.T) {
	code := map[string]bool{}
	lit := regexp.MustCompile(`traceEA\(s, "([A-Za-z]+)"|failEnd\("([A-Za-z]+)"\)`)
	for _, f := range []string{"endack.go", "quiclink.go", "chain.go", "export_test.go"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range lit.FindAllStringSubmatch(string(b), -1) {
			code[m[1]+m[2]] = true
		}
	}
	b, err := os.ReadFile("../../spec/TraceEndAckLeg.tla")
	if err != nil {
		t.Fatal(err)
	}
	spec := map[string]bool{}
	for _, m := range regexp.MustCompile(`E\.ev = "([A-Za-z]+)"`).FindAllStringSubmatch(string(b), -1) {
		spec[m[1]] = true
	}
	if len(code) < 8 {
		t.Fatalf("found only %d end handshake events in the code: the pattern no longer matches the hooks", len(code))
	}
	for e := range code {
		if !spec[e] {
			t.Errorf("the recorder writes %q, which no step of spec/TraceEndAckLeg.tla maps", e)
		}
	}
	for e := range spec {
		if !code[e] {
			t.Errorf("spec/TraceEndAckLeg.tla maps %q, but no hook point of the code writes it", e)
		}
	}
}
