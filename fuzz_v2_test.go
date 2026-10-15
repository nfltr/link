// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
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
	f.Add(append([]byte("NFDP\x01"), bytes.Repeat([]byte("a"), 32+4)...))
	f.Add([]byte("NFDP\x03"))
	f.Add([]byte("NFDP\x00"))
	f.Add([]byte("PRI * HTTP/2.0"))
	addVectors(f, "hello", nil)
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
		if c.Credit().PeerWindow() < MinWindow || c.Credit().PeerWindow() > MaxWindow {
			t.Fatalf("bound a peer window of %d", c.Credit().PeerWindow())
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
	f.Add([]byte{statusRefused})
	f.Add([]byte{statusVersion, 1})
	f.Add([]byte{statusBound, 1, 0, 0, 0, 0})
	f.Add([]byte{9})
	addVectors(f, "reply", nil)
	f.Fuzz(func(t *testing.T, reply []byte) {
		cr := &countingReader{r: bytes.NewReader(reply)}
		c, err := Client(context.Background(), closingConn{bufConn{r: cr, w: io.Discard}}, Config{Window: testWindow, Auth: testToken})
		if cr.n > 14 {
			t.Fatalf("read %d bytes, a reply is at most 14", cr.n)
		}
		var ve *VersionError
		switch {
		case len(reply) > 0 && reply[0] == statusRefused:
			if !errors.Is(err, ErrRefused) {
				t.Fatalf("refused: %v", err)
			}
		case len(reply) > 1 && reply[0] == statusVersion:
			if !errors.As(err, &ve) || ve.Max != int(reply[1]) {
				t.Fatalf("version: %v", err)
			}
		case err == nil:
			defer c.Close() //nolint:errcheck
			if reply[0] != statusBound || reply[1] != 2 || c.Credit().PeerWindow() != int(binary.BigEndian.Uint32(reply[6:])) ||
				c.Credit().PeerWindow() < MinWindow || c.Credit().PeerWindow() > MaxWindow {
				t.Fatalf("bound on %x", reply)
			}
		}
	})
}

// FuzzRecvV2 feeds a peer's arbitrary frames to a version-2 Conn, without
// and with FeatureChain: no panic, the credit caps hold for session "7",
// the peer never has more sessions open than this side's max, and on a
// FeatureChain connection session "7" (opened by this side) gets at most
// one answer and no data before it, and aborts are within their bounds.
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
	for _, seed := range chainSeeds(f) {
		f.Add(seed)
	}
	addVectors(f, "frame", nil)
	f.Fuzz(func(t *testing.T, wire []byte) {
		recvV2(t, wire, false)
		recvV2(t, wire, true)
	})
}

// chainSeeds are frames a FeatureChain peer sends: an accept of "7",
// data, a refusal, a reset, a chain-open.
func chainSeeds(f *testing.F) [][]byte {
	var out bytes.Buffer
	c := v2Conn(bufConn{w: &out})
	c.features, c.peerFeatures = features, features
	var seeds [][]byte
	for _, send := range []func() error{
		func() error { return c.sendAccept("7", 2) },
		func() error { return c.SendData("7", []byte("after the accept")) },
		func() error { return c.sendAbort("7", &Abort{Refused: true, Code: CodeHopLimit, Hops: 3, Msg: "loop"}) },
		func() error { return c.sendAbort("7", &Abort{Code: CodeApp + 1, Msg: "reset"}) },
		func() error { return c.sendOpen("9", []byte("hdr"), Chain{Hops: 4, Trace: Trace{1, 2, 3}}) },
	} {
		out.Reset()
		if err := send(); err != nil {
			f.Fatal(err)
		}
		if err := c.Flush(); err != nil {
			f.Fatal(err)
		}
		seeds = append(seeds, bytes.Clone(out.Bytes()))
	}
	return append(seeds, bytes.Join(seeds, nil))
}

func recvV2(t *testing.T, wire []byte, chain bool) {
	c := v2Conn(bufConn{r: bytes.NewReader(wire)})
	if chain {
		c.features, c.peerFeatures = features, features
		c.credit.open("7", byUs)
	} else {
		c.Credit().Open("7")
	}
	answers := 0
	{
		accepted := 0
		for {
			m, err := c.Recv()
			if s := c.credit.sessions["7"]; s != nil && s.n > testWindow {
				t.Fatalf("send credit %d past the peer's window", s.n)
			}
			if c.Credit().count(byPeer) > c.maxSessions {
				t.Fatalf("%d sessions open by the peer, max %d", c.Credit().count(byPeer), c.maxSessions)
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
			if !chain && (m.Accept || m.Abort != nil || m.Chain != (Chain{})) {
				t.Fatalf("a FeatureChain frame %+v on a connection without it", m)
			}
			if a := m.Abort; a != nil && (len(a.Msg) > MaxAbortMsg || !utf8.ValidString(a.Msg)) {
				t.Fatalf("abort %+v past its bounds", a)
			}
			if chain && m.SID == "7" && (m.Accept || m.Abort != nil && m.Abort.Refused) {
				if answers++; answers > 1 {
					t.Fatal("two answers to one open")
				}
			}
			if s := c.credit.sessions["7"]; chain && m.SID == "7" && len(m.Data) > 0 && s != nil && s.by == byUs && !s.answered {
				t.Fatal("data before the answer")
			}
		}
	}
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
	for _, seed := range chainSeeds(f) {
		f.Add(seed)
	}
	addVectors(f, "frame", nil)
	f.Fuzz(func(t *testing.T, wire []byte) {
		muxServeV2(t, wire, false)
		muxServeV2(t, wire, true)
	})
}

func muxServeV2(t *testing.T, wire []byte, chain bool) {
	c := v2Conn(closingConn{bufConn{r: bytes.NewReader(wire), w: io.Discard}})
	if chain {
		c.features, c.peerFeatures = features, features
	}
	m := NewMux(c)
	{
		var mu sync.Mutex
		var got int
		err := m.Serve(func(h []byte, s *Session) {
			if len(h) > 0 && h[0]&1 == 1 {
				_ = s.Refuse(CodeApp, "odd")
				return
			}
			_ = s.Accept()
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
	}
}

// FuzzForward feeds a peer's arbitrary frames, on a FeatureChain
// connection, to a Forward hop whose Route opens onward to an echo: no
// panic, Serve returns when the bytes run out, and every onward session
// it opened ends (no leak).
func FuzzForward(f *testing.F) {
	var sent bytes.Buffer
	s := v2Conn(bufConn{w: &sent})
	s.features, s.peerFeatures = features, features
	for _, err := range []error{
		s.sendOpen("1", []byte("echo"), Chain{Trace: Trace{7}}),
		s.SendData("1", []byte("hello")),
		s.SendClose("1", true),
		s.sendOpen("3", []byte("refuse"), Chain{Hops: 7}),
		s.sendOpen("5", []byte("echo"), Chain{Hops: 200}),
		s.sendAbort("1", &Abort{Code: CodeApp}),
		s.Flush(),
	} {
		if err != nil {
			f.Fatal(err)
		}
	}
	f.Add(sent.Bytes())
	for _, seed := range chainSeeds(f) {
		f.Add(seed)
	}
	var once sync.Once
	var onward *Mux
	f.Fuzz(func(t *testing.T, wire []byte) {
		once.Do(func() { // one echo per fuzz process
			a, b := net.Pipe()
			go func() {
				c, _, err := Server(context.Background(), b, Config{Window: testWindow, Auth: NoAuth})
				if err == nil {
					_ = NewMux(c).Serve(func(h []byte, s *Session) {
						if string(h) == "refuse" {
							_ = s.Refuse(CodeApp, "no")
							return
						}
						farEcho(h, s)
					})
				}
			}()
			c, err := Client(context.Background(), a, Config{Window: testWindow})
			if err != nil {
				t.Fatal(err)
			}
			onward = NewMux(c)
			go func() { _ = onward.Serve(nil) }()
		})
		c := v2Conn(closingConn{bufConn{r: bytes.NewReader(wire), w: io.Discard}})
		c.features, c.peerFeatures = features, features
		m := NewMux(c)
		route := func(ctx context.Context, in *Session) (*Session, error) {
			ch, _ := ChainFromContext(ctx)
			return onward.OpenChain(in.Header(), ch)
		}
		if err := m.Serve(Forward(route, ForwardConfig{AnswerTimeout: time.Second})); err == nil {
			t.Fatal("Serve returned nil")
		}
		for deadline := time.Now().Add(10 * time.Second); onward.Sessions() > 0; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%d onward sessions left", onward.Sessions())
			}
		}
	})
}

// FuzzParseWindow: the window update body a caller with a transport of its
// own parses (ParseWindow) reads back what AppendWindow wrote, and nothing
// else parses.
func FuzzParseWindow(f *testing.F) {
	addVectors(f, "frame", func(b []byte) []byte {
		if len(b) > 5 && b[4] == typeWindow {
			return b[5:]
		}
		return nil
	})
	f.Add([]byte{0, 0, 0, 0, 1})
	f.Add([]byte{3, 'a', 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, body []byte) {
		sid, n, err := ParseWindow(body)
		if err != nil {
			return
		}
		if len(sid) > MaxSID || n < 0 || !bytes.Equal(AppendWindow(nil, sid, n), body) {
			t.Fatalf("%x parsed to %q %d", body, sid, n)
		}
	})
}

// addVectors seeds f with the wire vectors of kind (testdata/wire_vectors.json),
// each through pick (nil: as they are; a nil result skips one).
func addVectors(f *testing.F, kind string, pick func([]byte) []byte) {
	raw, err := os.ReadFile(vectorsFile)
	if err != nil {
		f.Fatal(err)
	}
	var vs []wireVector
	if err := json.Unmarshal(raw, &vs); err != nil {
		f.Fatal(err)
	}
	for _, v := range vs {
		b := []byte(v.Hex)
		if pick != nil {
			b = pick(b)
		}
		if v.Kind == kind && b != nil {
			f.Add(b)
		}
	}
}
