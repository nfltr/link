// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
)

// anyAuth accepts every credential.
type anyAuth struct{}

func (anyAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return MethodNone, nil, nil
}

func (anyAuth) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (Identity, error) {
	return Identity{Method: string(rune(method)), Subject: string(payload)}, nil
}

// FuzzServerHello feeds a dialer's arbitrary bytes to Server: no panic, it
// reads no more than a hello, and binds only a well-formed one (an
// in-range window and max sessions, a valid liveness).
// Seeds: testdata/fuzz/FuzzServerHello (the recorded hellos).
func FuzzServerHello(f *testing.F) {
	v2 := appendHelloV2(nil, helloV2{maxVersion: 2, window: testWindow, maxSessions: 4, liveness: LivenessFast, method: MethodToken, auth: []byte("tok")})
	f.Add(v2)
	f.Add(v2[:len(v2)-1])
	f.Add(append([]byte("NFDP\x01"), bytes.Repeat([]byte("a"), TokenLen+4)...))
	f.Add([]byte("NFDP\x03"))
	f.Add([]byte("NFDP\x00"))
	f.Add([]byte("PRI * HTTP/2.0"))
	f.Fuzz(func(t *testing.T, wire []byte) {
		cr := &countingReader{r: bytes.NewReader(wire)}
		c, _, err := Server(context.Background(), closingConn{bufConn{r: cr, w: io.Discard}}, Config{Window: testWindow, Auth: anyAuth{}})
		if limit := len(Magic) + 1 + helloV2Fixed + MaxAuth; cr.n > limit {
			t.Fatalf("read %d bytes, a hello is at most %d", cr.n, limit)
		}
		if err != nil {
			return
		}
		defer c.Close() //nolint:errcheck
		if c.PeerWindow() < MinWindow || c.PeerWindow() > MaxWindow {
			t.Fatalf("bound a peer window of %d", c.PeerWindow())
		}
		if c.Version() == 2 {
			if c.PeerMaxSessions() < 1 || c.PeerMaxSessions() > maxSessionsWire || c.live.Validate() != nil {
				t.Fatalf("bound max sessions %d liveness %v", c.PeerMaxSessions(), c.live)
			}
		}
	})
}

// FuzzClientReply feeds a listener's arbitrary reply to a version-2
// Client: it reads no more than a reply, binds only on StatusBound of
// version 2 with an in-range window, and reports refusals and versions.
func FuzzClientReply(f *testing.F) {
	f.Add(appendReplyV2(nil, replyV2{window: testWindow, maxSessions: 8}))
	f.Add([]byte{StatusRefused})
	f.Add([]byte{StatusVersion, 1})
	f.Add([]byte{StatusBound, 1, 0, 0, 0, 0})
	f.Add([]byte{9})
	f.Fuzz(func(t *testing.T, reply []byte) {
		cr := &countingReader{r: bytes.NewReader(reply)}
		c, err := Client(context.Background(), closingConn{bufConn{r: cr, w: io.Discard}}, Config{Window: testWindow, Auth: testToken})
		if cr.n > 14 {
			t.Fatalf("read %d bytes, a reply is at most 14", cr.n)
		}
		var ve *VersionError
		switch {
		case len(reply) > 0 && reply[0] == StatusRefused:
			if !errors.Is(err, ErrRefused) {
				t.Fatalf("refused: %v", err)
			}
		case len(reply) > 1 && reply[0] == StatusVersion:
			if !errors.As(err, &ve) || ve.Max != int(reply[1]) {
				t.Fatalf("version: %v", err)
			}
		case err == nil:
			defer c.Close() //nolint:errcheck
			if reply[0] != StatusBound || reply[1] != 2 || c.PeerWindow() != int(binary.BigEndian.Uint32(reply[6:])) ||
				c.PeerWindow() < MinWindow || c.PeerWindow() > MaxWindow {
				t.Fatalf("bound on %x", reply)
			}
		}
	})
}

// FuzzRecvV2 feeds a peer's arbitrary frames to a version-2 Conn: no
// panic, the credit caps hold for session "7", and the peer never has
// more sessions open than this side's max.
// Seeds: testdata/fuzz/FuzzRecvV2 (the recorded version-2 frames).
func FuzzRecvV2(f *testing.F) {
	for _, seed := range [][]byte{
		frame(4, typeOpen, 1, '7', 'h', 'i'),
		frame(3, typeClose, 1, '7', closeHalf),
		frame(3, typeClose, 1, '7', 9),
		frame(9, typePing, 1, 2, 3, 4, 5, 6, 7, 8),
		frame(9, typePong, 0, 0, 0, 0, 0, 0, 0, 1),
		frame(5, typeGoAway, 0, 0, 0, 1),
		frame(3, typeGoAway, 0, 0),
		frame(6, typeWindow, 1, '7', 0, 1, 0, 0),
		append(frame(2+testWindow, typeData, 1, '7'), make([]byte, testWindow)...),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		c := v2Conn(bufConn{r: bytes.NewReader(wire)})
		c.Open("7")
		accepted := 0
		for {
			m, err := c.Recv()
			if s := c.Credit.sessions["7"]; s != nil && s.n > testWindow {
				t.Fatalf("send credit %d past the peer's window", s.n)
			}
			if c.count(byPeer) > c.maxSessions {
				t.Fatalf("%d sessions open by the peer, max %d", c.count(byPeer), c.maxSessions)
			}
			if err != nil {
				return
			}
			if len(m.Header) > maxHeader || len(m.Data)+len(m.Msg) > maxFrame {
				t.Fatalf("frame %+v past the bounds", m)
			}
			if m.SID == "7" && !m.Open {
				if accepted += len(m.Data); accepted > testWindow {
					t.Fatalf("accepted %d bytes for a session, window %d", accepted, testWindow)
				}
			}
		}
	})
}

// FuzzMuxServeV2 feeds a peer's arbitrary frames to a version-2 Mux that
// accepts sessions: no panic, Serve returns when the bytes run out, and
// the sessions get no more bytes than arrived.
func FuzzMuxServeV2(f *testing.F) {
	var sent bytes.Buffer
	s := v2Conn(bufConn{w: &sent})
	for _, err := range []error{
		s.SendOpen("1", []byte("h")),
		s.SendData("1", []byte("hello")),
		s.SendClose("1", true),
		s.SendOpen("3", nil),
		s.SendClose("3", false),
		s.SendData("5", []byte("orphan")),
		s.Flush(),
	} {
		if err != nil {
			f.Fatal(err)
		}
	}
	f.Add(sent.Bytes())
	f.Fuzz(func(t *testing.T, wire []byte) {
		m := NewMux(v2Conn(closingConn{bufConn{r: bytes.NewReader(wire), w: io.Discard}}), nil)
		var mu sync.Mutex
		var got int
		err := m.Serve(func(_ []byte, s *Session) {
			n, _ := io.Copy(io.Discard, s)
			mu.Lock()
			got += int(n)
			mu.Unlock()
			_ = s.Close()
		})
		if err == nil {
			t.Fatal("Serve returned nil")
		}
		mu.Lock()
		defer mu.Unlock()
		if got > len(wire) {
			t.Fatalf("sessions got %d bytes of %d", got, len(wire))
		}
	})
}
