// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// countingReader counts the bytes a decoder consumed.
type countingReader struct {
	r io.Reader
	n int
}

// Read: io.Reader over r, counting.
func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += n
	return n, err
}

// FuzzRecv feeds a peer's arbitrary bytes to Conn.Recv (data, message and
// window frames): no panic, no frame larger than maxFrame accepted, and the
// credit caps hold: session "7" never has more send credit than the peer's
// window nor more data accepted (none consumed) than this side's window.
// Seeds: testdata/fuzz/FuzzRecv (frames of a recorded session).
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
		c := newTestConn(bufConn{r: bytes.NewReader(wire)}, testWindow, testWindow)
		c.Open("7") // window frames for "7" reach the credit path
		accepted := 0
		for {
			m, err := c.Recv()
			if s := c.Credit.sessions["7"]; s != nil && s.n > testWindow { // a close frame forgets it
				t.Fatalf("send credit %d past the peer's window %d", s.n, testWindow)
			}
			if err != nil {
				return
			}
			if n := len(m.Data) + len(m.Msg); n > maxFrame {
				t.Fatalf("frame of %d bytes, maxFrame %d", n, maxFrame)
			}
			if !m.Open && !m.Close && (m.Data == nil) == (m.Msg == nil) {
				t.Fatalf("frame %+v is neither data, a message, an open nor a close", m)
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
		s := newTestConn(bufConn{w: &wire}, testWindow, testWindow)
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
		got, err := newTestConn(bufConn{r: cr}, testWindow, testWindow).Recv()
		if err != nil {
			t.Fatalf("Recv of a sent frame: %v", err)
		}
		if got.SID != want.SID || !bytes.Equal(got.Data, want.Data) || !bytes.Equal(got.Msg, want.Msg) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		if cr.n != wire.Len() {
			t.Fatalf("Recv consumed %d of %d bytes", cr.n, wire.Len())
		}
	})
}

// closingConn is a bufConn whose Close does nothing (Serve closes it).
type closingConn struct{ bufConn }

// Close: no-op.
func (closingConn) Close() error { return nil }

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
