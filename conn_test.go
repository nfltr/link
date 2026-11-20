// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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

// Read: net.Conn over r.
func (c bufConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// Write: net.Conn over w.
func (c bufConn) Write(b []byte) (int, error) { return c.w.Write(b) }

// newTestConn is a bound Conn over nc without a hello: window is this
// side's window, peerWindow the peer's.
func newTestConn(nc net.Conn, window, peerWindow int) *Conn {
	return newConnV2(nc, Config{Window: window, MaxSessions: DefaultMaxSessions}, peerWindow, DefaultMaxSessions, false)
}

func pair(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	a, b := net.Pipe()
	ca, cb := newTestConn(a, testWindow, testWindow), newTestConn(b, testWindow, testWindow)
	t.Cleanup(func() { _ = ca.Close(); _ = cb.Close() })
	return ca, cb
}

// frame builds a raw frame: length n, type, body.
func frame(n uint32, typ byte, body ...byte) []byte {
	return append(append(binary.BigEndian.AppendUint32(nil, n), typ), body...)
}

func TestFramesRoundTrip(t *testing.T) {
	ca, cb := pair(t)
	big := bytes.Repeat([]byte("x"), MaxData+5) // split in two frames
	go func() {
		for _, err := range []error{
			ca.sendMsg([]byte("open 7")),
			ca.sendDataFrame("7", []byte("hello")),
			ca.sendMsg([]byte{0x0a, 0x00}),
			ca.sendDataFrame("_fingerprint", []byte("ab:cd")),
			ca.sendDataFrame("7", big),
			ca.sendMsg([]byte("close 7")),
			ca.flush(),
		} {
			if err != nil {
				t.Error(err)
			}
		}
	}()
	for i, w := range []rxFrame{
		{Msg: []byte("open 7")},
		{SID: "7", Data: []byte("hello")},
		{Msg: []byte{0x0a, 0x00}},
		{SID: "_fingerprint", Data: []byte("ab:cd")},
		{SID: "7", Data: big[:MaxData]},
		{SID: "7", Data: big[MaxData:]},
		{Msg: []byte("close 7")},
	} {
		got, err := cb.recvFrame()
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
	c := newTestConn(bufConn{w: &buf}, testWindow, testWindow)
	if err := c.sendDataFrame("12", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	_ = c.flush()
	want := []byte{0, 0, 0, 6, typeData, 2, '1', '2', 'a', 'b', 'c'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("got % x, want % x", buf.Bytes(), want)
	}
}

func TestSendRefusesWhatTheFormatCannotCarry(t *testing.T) {
	c := newTestConn(bufConn{w: io.Discard}, testWindow, testWindow)
	if c.sendMsg(nil) == nil {
		t.Fatal("empty message sent")
	}
	if c.sendDataFrame(strings.Repeat("s", MaxSID+1), []byte("x")) == nil {
		t.Fatal("session id of 256 bytes sent")
	}
}

func TestRecvRejectsMalformedFrames(t *testing.T) {
	for name, raw := range map[string][]byte{
		"too large":        frame(MaxFrame+1, typeData),
		"unknown type":     frame(1, 9, 0),
		"sid past body":    frame(2, typeData, 5, 'a'),
		"empty body":       frame(0, typeData),
		"empty data":       frame(1, typeData, 2),
		"truncated body":   frame(10, typeData, 1, 'a'),
		"short window":     frame(3, typeWindow, 0, 0, 1),
		"window sid wrong": frame(6, typeWindow, 2, 'a', 0, 0, 0, 1),
	} {
		t.Run(name, func(t *testing.T) {
			c := newTestConn(bufConn{r: bytes.NewReader(raw)}, testWindow, testWindow)
			if _, err := c.recvFrame(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestCreditBoundsUnacknowledgedData(t *testing.T) {
	ca, cb := pair(t)
	for _, sid := range []string{"slow", "other"} {
		ca.credit.Open(sid)
		cb.credit.Open(sid)
	}
	go func() { // cb's receive loop: frames out, window frames applied
		for {
			if _, err := cb.recvFrame(); err != nil {
				return
			}
		}
	}()
	for range testWindow / MaxChunk {
		if !ca.credit.Acquire("slow", MaxChunk) {
			t.Fatal("credit within the window refused")
		}
	}
	got := make(chan bool, 1)
	go func() { got <- ca.credit.Acquire("slow", MaxChunk) }()
	select {
	case <-got:
		t.Fatal("sent past the peer's window")
	case <-time.After(50 * time.Millisecond):
	}
	if !ca.credit.Acquire("other", MaxChunk) {
		t.Fatal("a full session held up another")
	}

	// The receiver consumes: a window frame goes ca's way only at a quarter
	// window, so ca's receive loop (here) applies it.
	go func() {
		for {
			if _, err := ca.recvFrame(); err != nil {
				return
			}
		}
	}()
	cb.credit.Consumed("slow", testWindow/4-1)
	select {
	case <-got:
		t.Fatal("credit returned before a quarter window was consumed")
	case <-time.After(50 * time.Millisecond):
	}
	cb.credit.Consumed("slow", 1)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("the waiting sender gave up")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumed data returned no credit")
	}

	go func() { got <- ca.credit.Acquire("slow", testWindow) }() // more than is left
	ca.credit.Forget("slow")
	if ok := <-got; ok || ca.credit.Acquire("slow", 1) {
		t.Fatal("a forgotten session still sends")
	}
	ca.credit.Open("late")
	if !ca.credit.Acquire("late", testWindow) {
		t.Fatal("a new session has no credit")
	}
	go func() { got <- ca.credit.Acquire("late", 1) }()
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
		c := newTestConn(bufConn{r: bytes.NewReader(wire)}, testWindow, testWindow)
		c.credit.Open("s")
		if setup != nil {
			setup(c)
		}
		for {
			if _, err := c.recvFrame(); err != nil {
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
		"grant of what was sent": {wire: frameOf(typeWindow, window("s", MaxChunk)), setup: func(c *Conn) { c.credit.Acquire("s", MaxChunk) }},
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
	c := newTestConn(bufConn{r: bytes.NewReader(nil)}, testWindow, testWindow)
	c.credit.Open("s")
	if err := c.credit.Received("s", testWindow); err != nil {
		t.Fatal(err)
	}
	if err := c.credit.Received("s", 1); !errors.Is(err, ErrCredit) {
		t.Fatalf("message data past the window: %v, want ErrCredit", err)
	}
}

// TestCreditReturnedDataIsAcceptedAgain: once this side acknowledges
// consumed data, the peer may send that much more without ErrCredit.
func TestCreditReturnedDataIsAcceptedAgain(t *testing.T) {
	var wire bytes.Buffer
	peer := newTestConn(bufConn{w: &wire}, testWindow, testWindow)
	for range 3 {
		if err := peer.sendDataFrame("s", make([]byte, testWindow/2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := peer.flush(); err != nil {
		t.Fatal(err)
	}
	c := newTestConn(bufConn{r: &wire, w: io.Discard}, testWindow, testWindow)
	c.credit.Open("s")
	for i := range 3 {
		if _, err := c.recvFrame(); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if i == 1 {
			c.credit.Consumed("s", testWindow/2) // returns half a window
		}
	}
}

// TestSendOpenHoldsThePeersMaxSessions: concurrent opens at the peer's
// max sessions never send one OPEN too many (which the peer takes for a
// broken connection and ends it, every session on it): the count is
// checked with the OPEN buffered under one lock. Rounds, as the window
// between check and count was a few instructions wide.
func TestSendOpenHoldsThePeersMaxSessions(t *testing.T) {
	for round := range 500 {
		a, b := net.Pipe()
		c := newConnV2(a, Config{Window: testWindow, MaxSessions: DefaultMaxSessions}, testWindow, 2, false)
		go func() { _, _ = io.Copy(io.Discard, b) }()
		if err := c.sendOpenFrame("s0", nil); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var opened atomic.Int32
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				<-start
				if c.sendOpenFrame(fmt.Sprint("s", i+1), nil) == nil {
					opened.Add(1)
				}
			})
		}
		close(start)
		wg.Wait()
		_ = c.Close()
		_ = b.Close()
		if n := opened.Load(); n != 1 {
			t.Fatalf("round %d: %d opens past one open session with the peer's max sessions 2, want 1", round, n)
		}
	}
}
