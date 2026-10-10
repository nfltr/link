// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testWindow = 4 * MaxChunk

// bufConn is a net.Conn that reads r and writes w.
type bufConn struct {
	net.Conn
	r io.Reader
	w io.Writer
}

// Read: net.Conn over r. allow:wrapper
func (c bufConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// Write: net.Conn over w. allow:wrapper
func (c bufConn) Write(b []byte) (int, error) { return c.w.Write(b) }

func pair(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	a, b := net.Pipe()
	ca, cb := NewConn(a, testWindow, testWindow), NewConn(b, testWindow, testWindow)
	t.Cleanup(func() { _ = ca.Close(); _ = cb.Close() })
	return ca, cb
}

// frame builds a raw frame: length n, type, body.
func frame(n uint32, typ byte, body ...byte) []byte {
	return append(append(binary.BigEndian.AppendUint32(nil, n), typ), body...)
}

func TestFramesRoundTrip(t *testing.T) {
	ca, cb := pair(t)
	big := bytes.Repeat([]byte("x"), maxData+5) // split in two frames
	go func() {
		for _, err := range []error{
			ca.SendMsg([]byte("open 7")),
			ca.SendData("7", []byte("hello")),
			ca.SendMsg([]byte{0x0a, 0x00}),
			ca.SendData("_fingerprint", []byte("ab:cd")),
			ca.SendData("7", big),
			ca.SendMsg([]byte("close 7")),
			ca.Flush(),
		} {
			if err != nil {
				t.Error(err)
			}
		}
	}()
	for i, w := range []Frame{
		{Msg: []byte("open 7")},
		{SID: "7", Data: []byte("hello")},
		{Msg: []byte{0x0a, 0x00}},
		{SID: "_fingerprint", Data: []byte("ab:cd")},
		{SID: "7", Data: big[:maxData]},
		{SID: "7", Data: big[maxData:]},
		{Msg: []byte("close 7")},
	} {
		got, err := cb.Recv()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, w) {
			t.Fatalf("frame %d: got %+v, want %+v", i, got, w)
		}
	}
}

// TestDataFrameIsRaw pins the data frame's encoding: header, session id,
// payload as sent.
func TestDataFrameIsRaw(t *testing.T) {
	var buf bytes.Buffer
	c := NewConn(bufConn{w: &buf}, testWindow, testWindow)
	if err := c.SendData("12", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	_ = c.Flush()
	want := []byte{0, 0, 0, 6, typeData, 2, '1', '2', 'a', 'b', 'c'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("got % x, want % x", buf.Bytes(), want)
	}
}

func TestSendRefusesWhatTheFormatCannotCarry(t *testing.T) {
	c := NewConn(bufConn{w: io.Discard}, testWindow, testWindow)
	if c.SendMsg(nil) == nil {
		t.Fatal("empty message sent")
	}
	if c.SendData(strings.Repeat("s", MaxSID+1), []byte("x")) == nil {
		t.Fatal("session id of 256 bytes sent")
	}
}

func TestRecvRejectsMalformedFrames(t *testing.T) {
	for name, raw := range map[string][]byte{
		"too large":        frame(maxFrame+1, typeData),
		"unknown type":     frame(1, 9, 0),
		"sid past body":    frame(2, typeData, 5, 'a'),
		"empty body":       frame(0, typeData),
		"empty data":       frame(1, typeData, 2),
		"truncated body":   frame(10, typeData, 1, 'a'),
		"short window":     frame(3, typeWindow, 0, 0, 1),
		"window sid wrong": frame(6, typeWindow, 2, 'a', 0, 0, 0, 1),
	} {
		t.Run(name, func(t *testing.T) {
			c := NewConn(bufConn{r: bytes.NewReader(raw)}, testWindow, testWindow)
			if _, err := c.Recv(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestHello(t *testing.T) {
	token := strings.Repeat("ab", TokenLen/2)
	for _, status := range []byte{StatusBound, StatusRefused} {
		a, b := net.Pipe()
		type result struct {
			w   int
			err error
		}
		res := make(chan result, 1)
		go func() { w, err := Hello(a, token, 3*MaxChunk); res <- result{w, err} }()
		magic := make([]byte, len(Magic))
		if _, err := io.ReadFull(b, magic); err != nil || string(magic) != Magic {
			t.Fatalf("magic %q, %v", magic, err)
		}
		got, w, err := ReadHello(b)
		if err != nil || got != token || w != 3*MaxChunk {
			t.Fatalf("token %q window %d, %v", got, w, err)
		}
		_ = Reply(b, status, 5*MaxChunk)
		r := <-res
		if status == StatusBound && (r.err != nil || r.w != 5*MaxChunk) || status != StatusBound && !errors.Is(r.err, ErrRefused) {
			t.Fatalf("status %d: window %d, %v", status, r.w, r.err)
		}
		_ = a.Close()
		_ = b.Close()
	}
	hello := func(ver byte, window uint32) []byte {
		return binary.BigEndian.AppendUint32(append([]byte{ver}, token...), window)
	}
	for name, b := range map[string][]byte{
		"another version":  hello(2, testWindow),
		"window too small": hello(Version, MaxChunk),
	} {
		if _, _, err := ReadHello(bytes.NewReader(b)); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, err := Hello(bufConn{w: io.Discard}, "short", testWindow); err == nil {
		t.Fatal("a short token sent")
	}
}

// TestCreditBoundsUnacknowledgedData guards per-session credit: a sender
// sends at most the peer's window of a session before the receiver
// consumed half of it; consuming returns credit in a window frame; another
// session is not held up; forgetting or closing ends a waiting sender.
func TestCreditBoundsUnacknowledgedData(t *testing.T) {
	ca, cb := pair(t)
	for _, sid := range []string{"slow", "other"} {
		ca.Open(sid)
		cb.Open(sid)
	}
	go func() { // cb's receive loop: frames out, window frames applied
		for {
			if _, err := cb.Recv(); err != nil {
				return
			}
		}
	}()
	for range testWindow / MaxChunk {
		if !ca.Acquire("slow", MaxChunk) {
			t.Fatal("credit within the window refused")
		}
	}
	got := make(chan bool, 1)
	go func() { got <- ca.Acquire("slow", MaxChunk) }()
	select {
	case <-got:
		t.Fatal("sent past the peer's window")
	case <-time.After(50 * time.Millisecond):
	}
	if !ca.Acquire("other", MaxChunk) {
		t.Fatal("a full session held up another")
	}

	// The receiver consumes: a window frame goes ca's way only at half a
	// window, so ca's receive loop (here) applies it.
	go func() {
		for {
			if _, err := ca.Recv(); err != nil {
				return
			}
		}
	}()
	cb.Consumed("slow", testWindow/2-1)
	select {
	case <-got:
		t.Fatal("credit returned before half a window was consumed")
	case <-time.After(50 * time.Millisecond):
	}
	cb.Consumed("slow", 1)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("the waiting sender gave up")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumed data returned no credit")
	}

	go func() { got <- ca.Acquire("slow", testWindow) }() // more than is left
	ca.Forget("slow")
	if ok := <-got; ok || ca.Acquire("slow", 1) {
		t.Fatal("a forgotten session still sends")
	}
	ca.Open("late")
	if !ca.Acquire("late", testWindow) {
		t.Fatal("a new session has no credit")
	}
	go func() { got <- ca.Acquire("late", 1) }()
	_ = ca.Close()
	select {
	case ok := <-got:
		if ok {
			t.Fatal("sent on a closed connection")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing did not end a waiting sender")
	}
}

// TestCreditCapsMaliciousPeer: a peer that grants more credit than its own
// window, or sends a session more than this side's window, is ErrCredit;
// honest traffic up to the window, and grants of what was sent, are not.
func TestCreditCapsMaliciousPeer(t *testing.T) {
	window := func(sid string, n uint32) []byte {
		return binary.BigEndian.AppendUint32(append([]byte{byte(len(sid))}, sid...), n)
	}
	frameOf := func(typ byte, body []byte) []byte {
		return append(append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), typ), body...)
	}
	data := func(sid string, n int) []byte {
		return frameOf(typeData, append(append([]byte{byte(len(sid))}, sid...), make([]byte, n)...))
	}
	recvAll := func(wire []byte, setup func(*Conn)) error {
		c := NewConn(bufConn{r: bytes.NewReader(wire)}, testWindow, testWindow)
		c.Open("s")
		if setup != nil {
			setup(c)
		}
		for {
			if _, err := c.Recv(); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}

	cases := map[string]struct {
		wire    []byte
		setup   func(*Conn)
		wantErr bool
	}{
		"huge grant":             {wire: frameOf(typeWindow, window("s", 1<<32-1)), wantErr: true},
		"grant on full credit":   {wire: frameOf(typeWindow, window("s", 1)), wantErr: true},
		"grant of what was sent": {wire: frameOf(typeWindow, window("s", MaxChunk)), setup: func(c *Conn) { c.Acquire("s", MaxChunk) }},
		"grant for unknown sid":  {wire: frameOf(typeWindow, window("x", 1<<32-1))},
		"data up to the window":  {wire: append(data("s", testWindow/2), data("s", testWindow/2)...)},
		"data past the window":   {wire: append(data("s", testWindow), data("s", 1)...), wantErr: true},
		"data for unknown sid":   {wire: data("x", testWindow+1)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := recvAll(tc.wire, tc.setup)
			if tc.wantErr != errors.Is(err, ErrCredit) || !tc.wantErr && err != nil {
				t.Fatalf("Recv: %v, want ErrCredit %v", err, tc.wantErr)
			}
		})
	}
	// Data a message carries is charged by the caller (Received).
	c := NewConn(bufConn{r: bytes.NewReader(nil)}, testWindow, testWindow)
	c.Open("s")
	if err := c.Received("s", testWindow); err != nil {
		t.Fatal(err)
	}
	if err := c.Received("s", 1); !errors.Is(err, ErrCredit) {
		t.Fatalf("message data past the window: %v, want ErrCredit", err)
	}
}

// TestCreditReturnedDataIsAcceptedAgain: once this side acknowledges
// consumed data, the peer may send that much more without ErrCredit.
func TestCreditReturnedDataIsAcceptedAgain(t *testing.T) {
	var wire bytes.Buffer
	peer := NewConn(bufConn{w: &wire}, testWindow, testWindow)
	for range 3 {
		if err := peer.SendData("s", make([]byte, testWindow/2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := peer.Flush(); err != nil {
		t.Fatal(err)
	}
	c := NewConn(bufConn{r: &wire, w: io.Discard}, testWindow, testWindow)
	c.Open("s")
	for i := range 3 {
		if _, err := c.Recv(); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if i == 1 {
			c.Consumed("s", testWindow/2) // returns half a window
		}
	}
}
