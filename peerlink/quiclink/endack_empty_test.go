// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	"golang.org/x/net/nettest"
)

// openEmpty opens a session from a that writes nothing and returns it
// with b's accepted end.
func openEmpty(t *testing.T, a *quiclink.Node, acc chan peerlink.Session) (*quiclink.Session, *quiclink.Session) {
	t.Helper()
	s, err := a.Open(context.Background(), "b", nil)
	if err != nil {
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

// TestEndAckEmptyStreamAfterThePeersWholeClose (spec/EndAckLeg.tla Kind
// quic, EmptyClosed, NoFalseReset, mutant close-resets: the bare 'A' of
// quiclink's Close before lane QUICEND): the peer closed the session
// whole, cleanly, before or while this side ends a stream that carried no
// byte: nothing is lost, CloseWriteSync and Close are nil, whether the
// peer's 'C' 'A' or its STOP_SENDING arrives first (link's
// TestEndAckEmptyStreamAfterThePeersWholeClose).
func TestEndAckEmptyStreamAfterThePeersWholeClose(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	for i := range 20 {
		s, r := openEmpty(t, a, acc)
		_ = r.Close()
		if i%2 == 0 { // the peer's FIN read first: its 'C' 'A' and STOP_SENDING in any order
			if _, err := io.ReadAll(s); err != nil {
				t.Fatalf("%d: read %v", i, err)
			}
		}
		if err := wait(t, closeSync(s), "CloseWriteSync"); err != nil {
			t.Fatalf("%d: CloseWriteSync of an empty stream after the peer's clean close: %v, want nil", i, err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("%d: Close of an empty stream after the peer's clean close: %v, want nil", i, err)
		}
	}
}

// TestEndAckEmptyStreamPeerResetFails: only a clean close is: the peer's
// Reset of an empty stream still fails the wait (EmptyClosed needs
// rclosed; link reads an empty stream's reset as ErrReset too).
func TestEndAckEmptyStreamPeerResetFails(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	s, r := openEmpty(t, a, acc)
	done := closeSync(s)
	time.Sleep(50 * time.Millisecond) // the wait runs
	_ = r.Reset()
	if err := wait(t, done, "CloseWriteSync"); !errors.Is(err, link.ErrReset) {
		t.Fatalf("CloseWriteSync: %v, want link.ErrReset", err)
	}
}

// TestSessionIsANetConn: a session pair passes x/net/nettest's net.Conn
// suite (relay-core's TestConnIsANetConn over link; BasicIO's c2, which
// wrote nothing, Closes after c1's whole Close: its error was the peer's
// STOP_SENDING before lane QUICEND).
func TestSessionIsANetConn(t *testing.T) {
	a, _, acc := endAckPair(t, 0, nil, endAckProtos...)
	nettest.TestConn(t, func() (net.Conn, net.Conn, func(), error) {
		s, err := a.Open(context.Background(), "b", nil)
		if err != nil {
			return nil, nil, nil, err
		}
		var r net.Conn
		select {
		case r = <-acc:
		case <-time.After(10 * time.Second):
			return nil, nil, nil, errors.New("no session accepted")
		}
		return s, r, func() { _ = s.Close(); _ = r.Close() }, nil
	})
}

// TestEndAckEmptyStreamRelayedCloseIsClean (spec/EndAckLeg.tla Kind quic
// NoFalseReset, spec/RelayedClose.tla RelayedCloseEmpty.cfg; nfltr-pipe
// TestSecondDialIsReset's shape over a QUIC agent link: the listener's
// stdin carried nothing, the dialer wrote and closed): an agent's session
// over QUIC, joined at the relay to a client's link session; the client
// writes and closes the session whole while the agent, which wrote
// nothing and reads the session to its end, ends its own: the agent's
// CloseWriteSync is nil, never the relay's close read as a reset (link's
// TestEndAckEmptyStreamRelayedCloseRaceIsClean).
func TestEndAckEmptyStreamRelayedCloseIsClean(t *testing.T) {
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
	for i := range 50 {
		s, in := openEmpty(t, a, acc)
		out, err := rm.Open(ctx, []byte("h"))
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = peerlink.Join(in, out) }()
		cs := <-client
		if _, err := cs.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		read := make(chan error, 1)
		go func() { _, err := io.Copy(io.Discard, s); read <- err }() // as the pipe's listener copies the session to its stdout
		go func() { _ = cs.Close() }()
		if err := wait(t, closeSync(s), "CloseWriteSync"); err != nil {
			t.Fatalf("%d: the agent's CloseWriteSync of an empty stream: %v, want nil", i, err)
		}
		if err := wait(t, read, "the agent's read"); err != nil {
			t.Fatalf("%d: the agent read %v, want the client's clean end", i, err)
		}
		_ = s.Close()
	}
}
