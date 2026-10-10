// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
)

// golden reads testdata/v1_session.golden: a version-1 session recorded
// from the code this package was extracted from (nfltr's TCP data
// plane, v0.6.0 of its protocol module), one "name hex" line per segment.
func golden(t *testing.T) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/v1_session.golden")
	if err != nil {
		t.Fatal(err)
	}
	segs := map[string][]byte{}
	for line := range strings.Lines(string(raw)) {
		name, h, _ := strings.Cut(strings.TrimSpace(line), " ")
		if segs[name], err = hex.DecodeString(h); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	return segs
}

// rawFrame is a frame as the format doc spells it, decoded here without
// the package's code.
type rawFrame struct {
	typ  byte
	sid  string
	body []byte // data, message, or a window frame's increment
}

func splitFrames(t *testing.T, b []byte) []rawFrame {
	t.Helper()
	var fs []rawFrame
	for len(b) > 0 {
		n := int(binary.BigEndian.Uint32(b))
		f, body := rawFrame{typ: b[4]}, b[5:5+n]
		if f.typ != typeMsg {
			l := int(body[0])
			f.sid, body = string(body[1:1+l]), body[1+l:]
		}
		f.body = body
		fs = append(fs, f)
		b = b[5+n:]
	}
	return fs
}

// TestV1GoldenReplay: the recorded frames decode to what the format doc
// says, and encoding those frames again gives the recorded bytes.
func TestV1GoldenReplay(t *testing.T) {
	segs := golden(t)
	frames := splitFrames(t, segs["frames"])
	if len(frames) < 10 {
		t.Fatalf("%d frames in the recording", len(frames))
	}

	// Encode.
	var out bytes.Buffer
	enc := NewConn(bufConn{w: &out}, testWindow, testWindow)
	for _, f := range frames {
		var err error
		switch f.typ {
		case typeData:
			err = enc.SendData(f.sid, f.body)
		case typeMsg:
			err = enc.SendMsg(f.body)
		case typeWindow:
			enc.Open(f.sid)
			enc.Consumed(f.sid, int(binary.BigEndian.Uint32(f.body)))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), segs["frames"]) {
		t.Fatalf("re-encoded frames differ:\ngot  %x\nwant %x", out.Bytes(), segs["frames"])
	}

	// Decode: the same frames; window frames are applied, not returned.
	dec := NewConn(bufConn{r: bytes.NewReader(segs["frames"])}, testWindow, testWindow)
	var want []Frame
	for _, f := range frames {
		switch f.typ {
		case typeData:
			want = append(want, Frame{SID: f.sid, Data: f.body})
		case typeMsg:
			want = append(want, Frame{Msg: f.body})
		case typeWindow:
			dec.Open(f.sid)
			if n := int(binary.BigEndian.Uint32(f.body)); !dec.Acquire(f.sid, n) {
				t.Fatal("no credit to return")
			}
		}
	}
	var got []Frame
	for {
		f, err := dec.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv after %d frames: %v", len(got), err)
		}
		got = append(got, f)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded %q\nwant %q", got, want)
	}
}

func TestV1GoldenHello(t *testing.T) {
	segs := golden(t)
	hello := segs["hello"]
	token, window := string(hello[5:5+TokenLen]), int(binary.BigEndian.Uint32(hello[5+TokenLen:]))
	var out bytes.Buffer
	if _, err := Hello(bufConn{r: bytes.NewReader(segs["reply-bound"]), w: &out}, token, window); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), hello) {
		t.Fatalf("hello %x, want %x", out.Bytes(), hello)
	}
	if tok, w, err := ReadHello(bytes.NewReader(hello[len(Magic):])); err != nil || tok != token || w != window {
		t.Fatalf("ReadHello: %q %d %v", tok, w, err)
	}
	for name, status := range map[string]byte{"reply-bound": StatusBound, "reply-refused": StatusRefused} {
		out.Reset()
		w := 0
		if status == StatusBound {
			w = int(binary.BigEndian.Uint32(segs[name][1:]))
		}
		_ = Reply(&out, status, w)
		if !bytes.Equal(out.Bytes(), segs[name]) {
			t.Fatalf("%s %x, want %x", name, out.Bytes(), segs[name])
		}
	}
}

// replayCodec opens and closes with the recorded messages.
type replayCodec struct{ open, close []byte }

func (c replayCodec) Close(string) ([]byte, error)   { return c.close, nil }
func (replayCodec) Parse([]byte) (Message, error)    { return Message{}, ErrFrame }
func (c replayCodec) opening(string) ([]byte, error) { return c.open, nil }

// TestV1GoldenMuxSession: a Mux session (open, a write, close) writes the
// recorded opener's bytes.
func TestV1GoldenMuxSession(t *testing.T) {
	rec := golden(t)["mux-opener"]
	fs := splitFrames(t, rec)
	if len(fs) != 3 || fs[0].typ != typeMsg || fs[1].typ != typeData || fs[2].typ != typeMsg {
		t.Fatalf("recording: %v", fs)
	}
	codec := replayCodec{open: fs[0].body, close: fs[2].body}
	a, b := net.Pipe()
	var got bytes.Buffer
	done := make(chan struct{})
	go func() { _, _ = io.Copy(&got, b); close(done) }()
	m := NewMux(NewConn(a, testWindow, testWindow), codec)
	s, err := m.Open(codec.opening)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(fs[1].body); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	_ = a.Close()
	<-done
	if !bytes.Equal(got.Bytes(), rec) {
		t.Fatalf("mux session %x, want %x", got.Bytes(), rec)
	}
}
