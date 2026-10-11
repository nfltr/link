// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The version-2 recording: testdata/v2_session.golden, one "name hex"
// line per segment, as for version 1. LINK_UPDATE_GOLDEN=1 writes it from
// this code; every release after is held to those bytes.
const v2Golden = "testdata/v2_session.golden"

var (
	v2Token   = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"
	v2Ping    = [pingLen]byte{1, 2, 3, 4, 5, 6, 7, 8}
	v2Session = "\x01"
	v2Header  = []byte("agent\x00localhost:22")
)

// v2Conn is a version-2 Conn over nc without liveness (a recording).
func v2Conn(nc net.Conn) *Conn {
	c := newTestConn(nc, testWindow, testWindow)
	c.peerMaxSessions, c.maxSessions = 16, 16
	c.t0, c.pong, c.peerGoAway = time.Now(), make(chan [pingLen]byte, 1), make(chan struct{})
	return c
}

// v2Record writes the scripted version-2 session.
func v2Record(t *testing.T) map[string][]byte {
	t.Helper()
	segs := map[string][]byte{}
	var out bytes.Buffer
	reply := appendReplyV2(nil, replyV2{features: FeatureReset, window: 0x00040000, maxSessions: 512})
	hc, err := Client(context.Background(), closingConn{bufConn{r: bytes.NewReader(reply), w: &out}},
		Config{Window: 0x00050000, Liveness: LivenessFast, Auth: tokenAuth(v2Token)})
	if err != nil {
		t.Fatal(err)
	}
	_ = hc.Close() // stops its liveness before it pings
	segs["hello"], segs["reply-bound"] = bytes.Clone(out.Bytes()), reply
	segs["reply-refused"] = []byte{StatusRefused}
	segs["reply-version"] = []byte{StatusVersion, 1}

	out.Reset()
	c := v2Conn(bufConn{w: &out})
	for _, err := range []error{
		c.SendOpen(v2Session, v2Header),
		c.SendData(v2Session, []byte("hello")),
		c.SendMsg([]byte("_ctl")),
		c.SendClose(v2Session, true),
		c.SendClose("\x02", false),
		c.SendReset("\x03"), // a peer without FeatureReset: code 0
		func() error { c.peerFeatures = FeatureReset; return c.SendReset("\x04") }(),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	c.sendWindow(v2Session, 0x8000)
	for _, typ := range []byte{typePing, typePong} { // as control, written inline
		c.wmu.Lock()
		_ = c.frameLocked(typ, "", v2Ping[:])
		c.flushLocked()
		c.wmu.Unlock()
	}
	if err := c.GoAway(5*time.Second, "restart"); err != nil {
		t.Fatal(err)
	}
	segs["frames"] = out.Bytes()
	return segs
}

func readGoldenFile(t *testing.T, path string) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
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

// TestV2Golden: the code writes the recorded bytes, and the recording
// reads as doc.go specifies (decoded here without the package's code).
func TestV2Golden(t *testing.T) {
	got := v2Record(t)
	if os.Getenv("LINK_UPDATE_GOLDEN") != "" {
		var b strings.Builder
		for _, name := range []string{"hello", "reply-bound", "reply-refused", "reply-version", "frames"} {
			fmt.Fprintf(&b, "%s %x\n", name, got[name])
		}
		if err := os.WriteFile(v2Golden, []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want := readGoldenFile(t, v2Golden)
	if !reflect.DeepEqual(got, want) {
		for name := range want {
			if !bytes.Equal(got[name], want[name]) {
				t.Errorf("%s:\ngot  %x\nwant %x", name, got[name], want[name])
			}
		}
		t.FailNow()
	}

	// The hello, field by field (doc.go).
	h := want["hello"]
	be := binary.BigEndian
	if string(h[:4]) != "NFDP" || h[4] != 2 || h[5] != 2 || be.Uint32(h[6:]) != FeatureReset ||
		be.Uint32(h[10:]) != 0x00050000 || be.Uint32(h[14:]) != MaxSessions ||
		be.Uint32(h[18:]) != 1500 || be.Uint32(h[22:]) != 3000 || h[26] != MethodToken ||
		int(be.Uint16(h[27:])) != 32 || string(h[29:]) != v2Token {
		t.Fatalf("hello %x", h)
	}
	r := want["reply-bound"]
	if r[0] != StatusBound || r[1] != 2 || be.Uint32(r[2:]) != FeatureReset || be.Uint32(r[6:]) != 0x00040000 || be.Uint32(r[10:]) != 512 || len(r) != 14 {
		t.Fatalf("reply %x", r)
	}

	// The frames.
	type raw struct {
		typ  byte
		sid  string
		body []byte
	}
	var fs []raw
	for b := want["frames"]; len(b) > 0; {
		n := int(be.Uint32(b))
		f, body := raw{typ: b[4]}, b[5:5+n]
		switch f.typ {
		case typeData, typeWindow, typeOpen, typeClose:
			l := int(body[0])
			f.sid, body = string(body[1:1+l]), body[1+l:]
		}
		f.body = body
		fs = append(fs, f)
		b = b[5+n:]
	}
	wantFrames := []raw{
		{typeOpen, v2Session, v2Header},
		{typeData, v2Session, []byte("hello")},
		{typeMsg, "", []byte("_ctl")},
		{typeClose, v2Session, []byte{closeHalf}},
		{typeClose, "\x02", []byte{closeFull}},
		{typeClose, "\x03", []byte{closeFull}},
		{typeClose, "\x04", []byte{closeReset}},
		{typeWindow, v2Session, []byte{0, 0, 0x80, 0}},
		{typePing, "", v2Ping[:]},
		{typePong, "", v2Ping[:]},
		{typeGoAway, "", append([]byte{0, 0, 0x13, 0x88}, "restart"...)},
	}
	if !reflect.DeepEqual(fs, wantFrames) {
		t.Fatalf("frames %+v\nwant %+v", fs, wantFrames)
	}

	// Recv: opens, data, messages and closes surface; window, ping, pong
	// and goaway are applied.
	dec := v2Conn(bufConn{r: bytes.NewReader(want["frames"])})
	var surfaced []Frame
	for {
		f, err := dec.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv after %d frames: %v", len(surfaced), err)
		}
		if f.Open { // the window frame below returns credit sent meanwhile
			dec.Acquire(f.SID, 0x8000)
		}
		surfaced = append(surfaced, f)
	}
	wantSurfaced := []Frame{
		{SID: v2Session, Open: true, Header: v2Header},
		{SID: v2Session, Data: []byte("hello")},
		{Msg: []byte("_ctl")},
		{SID: v2Session, Close: true, Half: true},
		{SID: "\x02", Close: true},
		{SID: "\x03", Close: true},
		{SID: "\x04", Close: true, Reset: true},
	}
	if !reflect.DeepEqual(surfaced, wantSurfaced) {
		t.Fatalf("surfaced %+v\nwant %+v", surfaced, wantSurfaced)
	}
	if got := <-dec.pong; got != v2Ping {
		t.Fatalf("pong queued %x", got)
	}
	select {
	case <-dec.PeerGoAway():
	default:
		t.Fatal("GOAWAY not applied")
	}
	if !dec.Acquire(v2Session, testWindow) {
		t.Fatal("window frame not applied")
	}
}
