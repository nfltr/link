// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// countingReader counts the bytes a decoder consumed.
type countingReader struct {
	r io.Reader
	n int
}

// Read: io.Reader over r, counting. allow:wrapper
func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += n
	return n, err
}

// FuzzRecv feeds a peer's arbitrary bytes to Conn.Recv (data, message and
// window frames): no panic, no frame larger than maxFrame accepted, and the
// credit caps hold: session "7" never has more send credit than the peer's
// window nor more data accepted (none consumed) than this side's window.
// Seeds: testdata/fuzz/FuzzRecv (the recorded v1 session's frames).
func FuzzRecv(f *testing.F) {
	for _, seed := range [][]byte{
		frame(maxFrame+1, typeData),
		frame(1, 9, 0),
		frame(2, typeData, 5, 'a'),
		frame(0, typeData),
		frame(1, typeData, 2),
		frame(1, typeData, 0),
		frame(2, typeMsg, 0xff, 0xff),
		frame(10, typeData, 1, 'a'),
		frame(3, typeWindow, 0, 0, 1),
		frame(6, typeWindow, 2, 'a', 0, 0, 0, 1),
		frame(6, typeWindow, 1, '7', 0, 1, 0, 0),
		frame(7, typeData, 1, '7', 'h', 'e', 'l', 'l', 'o'),
		frame(6, typeWindow, 1, '7', 0xff, 0xff, 0xff, 0xff), // a huge grant
		append(frame(2+testWindow, typeData, 1, '7'), make([]byte, testWindow)...), // past the window
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		c := NewConn(bufConn{r: bytes.NewReader(wire)}, testWindow, testWindow)
		c.Open("7") // window frames for "7" reach the credit path
		accepted := 0
		for {
			m, err := c.Recv()
			if credit := c.cr.send["7"].n; credit > testWindow {
				t.Fatalf("send credit %d past the peer's window %d", credit, testWindow)
			}
			if err != nil {
				return
			}
			if n := len(m.Data) + len(m.Msg); n > maxFrame {
				t.Fatalf("frame of %d bytes, maxFrame %d", n, maxFrame)
			}
			if (m.Data == nil) == (m.Msg == nil) {
				t.Fatalf("frame %q is neither data nor a message", m)
			}
			if m.SID == "7" {
				if accepted += len(m.Data); accepted > testWindow {
					t.Fatalf("accepted %d bytes for a session, window %d", accepted, testWindow)
				}
			}
		}
	})
}

// FuzzSendRecvRoundTrip: any frame the sends write, Recv reads back equal,
// consuming exactly what was written.
func FuzzSendRecvRoundTrip(f *testing.F) {
	f.Add("7", []byte("hello"), false)
	f.Add("", []byte{0x0a, 0x00}, true)
	f.Add("_heartbeat", []byte(nil), true)
	f.Add(strings.Repeat("s", 255), []byte("x"), false)
	f.Fuzz(func(t *testing.T, sid string, body []byte, msg bool) {
		var wire bytes.Buffer
		s := NewConn(bufConn{w: &wire}, testWindow, testWindow)
		want, send := Frame{SID: sid, Data: body}, func() error { return s.SendData(sid, body) }
		if msg {
			want, send = Frame{Msg: body}, func() error { return s.SendMsg(body) }
		}
		err := send()
		if err != nil || len(body) == 0 || len(body) > maxData {
			return // refused, nothing sent, or split (TestFramesRoundTrip)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
		cr := &countingReader{r: bytes.NewReader(wire.Bytes())}
		got, err := NewConn(bufConn{r: cr}, testWindow, testWindow).Recv()
		if err != nil {
			t.Fatalf("Recv of a sent frame: %v", err)
		}
		if got.SID != want.SID || !bytes.Equal(got.Data, want.Data) || !bytes.Equal(got.Msg, want.Msg) {
			t.Fatalf("got %q, want %q", got, want)
		}
		if cr.n != wire.Len() {
			t.Fatalf("Recv consumed %d of %d bytes", cr.n, wire.Len())
		}
	})
}

// FuzzReadHello: the listener's hello parser reads exactly the hello, and
// accepts only this version with an in-range window, returning its token.
func FuzzReadHello(f *testing.F) {
	token := strings.Repeat("ab", TokenLen/2)
	hello := func(ver byte, window uint32) []byte {
		return binary.BigEndian.AppendUint32(append([]byte{ver}, token...), window)
	}
	f.Add(hello(Version, testWindow))
	f.Add(hello(2, testWindow))
	f.Add(hello(Version, MaxChunk))
	f.Add(hello(Version, MaxWindow+1))
	f.Add([]byte{Version})
	f.Fuzz(func(t *testing.T, wire []byte) {
		cr := &countingReader{r: bytes.NewReader(wire)}
		tok, w, err := ReadHello(cr)
		if cr.n > 1+TokenLen+4 {
			t.Fatalf("consumed %d bytes, hello is %d", cr.n, 1+TokenLen+4)
		}
		if err != nil {
			return
		}
		if wire[0] != Version || tok != string(wire[1:1+TokenLen]) ||
			w != int(binary.BigEndian.Uint32(wire[1+TokenLen:])) || w < MinWindow || w > MaxWindow {
			t.Fatalf("accepted %x as token %q window %d", wire, tok, w)
		}
	})
}

// FuzzHelloReply: the dialer's parser of the listener's reply binds only
// on StatusBound with an in-range window, and refuses any other status.
func FuzzHelloReply(f *testing.F) {
	f.Add([]byte{StatusBound, 0, 4, 0, 0})
	f.Add([]byte{StatusRefused})
	f.Add([]byte{StatusBound, 0, 0, 0, 1})
	f.Add([]byte{StatusBound, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{})
	token := strings.Repeat("ab", TokenLen/2)
	f.Fuzz(func(t *testing.T, reply []byte) {
		cr := &countingReader{r: bytes.NewReader(reply)}
		w, err := Hello(bufConn{r: cr, w: io.Discard}, token, testWindow)
		if cr.n > 5 {
			t.Fatalf("consumed %d bytes, reply is at most 5", cr.n)
		}
		switch {
		case len(reply) > 0 && reply[0] != StatusBound:
			if !errors.Is(err, ErrRefused) {
				t.Fatalf("status %d: %v, want ErrRefused", reply[0], err)
			}
		case err == nil && (w < MinWindow || w > MaxWindow || w != int(binary.BigEndian.Uint32(reply[1:]))):
			t.Fatalf("bound with window %d from %x", w, reply)
		}
	})
}

// closingConn is a bufConn whose Close does nothing (Serve closes it).
type closingConn struct{ bufConn }

// Close: no-op. allow:wrapper
func (closingConn) Close() error { return nil }

// FuzzMuxServe feeds a peer's arbitrary frames to the accepting side of a
// Mux: no panic, Serve returns when the bytes run out, and the sessions
// get no more bytes than arrived.
// Seeds: testdata/fuzz/FuzzMuxServe (the recorded v1 Mux session).
func FuzzMuxServe(f *testing.F) {
	var sent bytes.Buffer
	s := NewConn(bufConn{w: &sent}, testWindow, testWindow)
	open, _ := opening("agent")("1")
	reopen, _ := opening("")("1")
	for _, err := range []error{
		s.SendMsg(open),
		s.SendData("1", []byte("hello")),
		s.SendMsg(reopen),
		s.SendData("_ctl", []byte("x")),
		s.SendData("2", []byte("orphan")),
		s.SendMsg([]byte("c1")),
		s.Flush(),
	} {
		if err != nil {
			f.Fatal(err)
		}
	}
	f.Add(sent.Bytes())
	f.Add(frame(2, typeData, 1, '1'))
	f.Fuzz(func(t *testing.T, wire []byte) {
		m := NewMux(NewConn(closingConn{bufConn{r: bytes.NewReader(wire), w: io.Discard}}, testWindow, testWindow), testCodec{})
		var mu sync.Mutex
		var got int
		err := m.Serve(func(_ []byte, s *Session) {
			n, _ := io.Copy(io.Discard, s) // ends: Serve's return ends every session
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

// FuzzParseLiveness: a spec ParseLiveness accepts is valid and reads back
// the same from its String.
func FuzzParseLiveness(f *testing.F) {
	for _, s := range []string{"", "default", "fast", "nat", "datacenter", "mobile", "mobile,timeout=90s",
		"interval=2s,timeout=6s", "interval=0s", "timeout=-1s", "fast,interval=1h", "x=1", ",", "MOBILE"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, spec string) {
		l, err := ParseLiveness(spec)
		if err != nil {
			return
		}
		if err := l.Validate(); err != nil {
			t.Fatalf("%q parsed to invalid %v: %v", spec, l, err)
		}
		if back, err := ParseLiveness(l.String()); err != nil || back != l {
			t.Fatalf("%q: %v reads back as %v, %v", spec, l, back, err)
		}
	})
}
