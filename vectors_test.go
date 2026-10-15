// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// The wire's test vectors (WIRE.md): testdata/wire_vectors.json, one
// object per hello, reply or frame, its fields and its bytes, or bytes a
// decoder refuses and why. The file is written from this code
// (LINK_UPDATE_GOLDEN=1, as the golden recording) and the test holds the
// code to it both ways: the fields encode to the bytes, the bytes decode
// to the fields.
const vectorsFile = "testdata/wire_vectors.json"

// hexBytes is JSON as a hex string.
type hexBytes []byte

func (h hexBytes) MarshalJSON() ([]byte, error) { return json.Marshal(hex.EncodeToString(h)) }

func (h *hexBytes) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	d, err := hex.DecodeString(s)
	*h = d
	return err
}

type vecHello struct {
	MaxVersion  byte     `json:"max_version"`
	Features    uint32   `json:"features"`
	Window      uint32   `json:"window"`
	MaxSessions uint32   `json:"max_sessions"`
	IntervalMS  uint32   `json:"interval_ms"`
	TimeoutMS   uint32   `json:"timeout_ms"`
	AuthMethod  byte     `json:"auth_method"`
	Auth        hexBytes `json:"auth"`
}

type vecReply struct {
	Status      byte   `json:"status"`
	Features    uint32 `json:"features,omitempty"`
	Window      uint32 `json:"window,omitempty"`
	MaxSessions uint32 `json:"max_sessions,omitempty"`
	MaxVersion  byte   `json:"max_version,omitempty"` // status 2
}

type vecFrame struct {
	Type      string   `json:"type"`
	SID       hexBytes `json:"sid,omitempty"`
	Payload   hexBytes `json:"payload,omitempty"` // data, message
	Header    hexBytes `json:"header,omitempty"`  // open
	Increment uint32   `json:"increment,omitempty"`
	Code      byte     `json:"code,omitempty"`     // close
	Ping      hexBytes `json:"ping,omitempty"`     // ping, pong
	DrainMS   uint32   `json:"drain_ms,omitempty"` // goaway
	Reason    string   `json:"reason,omitempty"`   // goaway
}

type wireVector struct {
	Name  string    `json:"name"`
	Kind  string    `json:"kind"` // hello, reply, frame
	Hello *vecHello `json:"hello,omitempty"`
	Reply *vecReply `json:"reply,omitempty"`
	Frame *vecFrame `json:"frame,omitempty"`
	// Invalid: the decoder refuses Hex; the error it returns (errors.Is
	// the named variable, or "error": any).
	Invalid string   `json:"invalid,omitempty"`
	Hex     hexBytes `json:"hex"`
}

// vecSID is the session the frame decoder knows (open, full credit).
var vecSID = hexBytes{0x01}

func h2(f func(*vecHello)) *vecHello {
	h := &vecHello{MaxVersion: 2, Features: featureReset, Window: 1 << 20, MaxSessions: 1024, IntervalMS: 5000, TimeoutMS: 10000, AuthMethod: MethodToken, Auth: hexBytes("token")}
	if f != nil {
		f(h)
	}
	return h
}

// wireVectors is the vectors file's content: valid ones carry fields (Hex
// is encoded from them), invalid ones bytes.
func wireVectors() []wireVector {
	be := func(n uint32) []byte { return []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)} }
	cat := func(bs ...[]byte) hexBytes { return hexBytes(bytes.Join(bs, nil)) }
	hello := func(h *vecHello) hexBytes { return hexBytes(appendHelloV2(nil, h.toHello())) }
	return []wireVector{
		{Name: "hello", Kind: "hello", Hello: h2(nil)},
		{Name: "hello-no-auth", Kind: "hello", Hello: h2(func(h *vecHello) { h.Features, h.AuthMethod, h.Auth = 0, MethodNone, hexBytes{} })},
		{Name: "hello-bounds", Kind: "hello", Hello: h2(func(h *vecHello) {
			h.Window, h.MaxSessions, h.IntervalMS, h.TimeoutMS, h.Auth = MinWindow, 1, 1000, 1000, bytes.Repeat([]byte{0xa5}, MaxAuth)
		})},
		{Name: "hello-unknown-features", Kind: "hello", Hello: h2(func(h *vecHello) { h.Features, h.MaxVersion = 0xffffffff, 9 })},

		{Name: "hello-bad-magic", Kind: "hello", Invalid: "ErrFrame", Hex: hexBytes("PRI * HTTP/2.0")},
		{Name: "hello-version-0", Kind: "hello", Invalid: "ErrFrame", Hex: hexBytes("NFDP\x00")},
		{Name: "hello-version-1", Kind: "hello", Invalid: "ErrV1", Hex: hexBytes("NFDP\x01")},
		{Name: "hello-version-3", Kind: "hello", Invalid: "ErrVersion", Hex: hexBytes("NFDP\x03")},
		{Name: "hello-auth-too-long", Kind: "hello", Invalid: "ErrFrame", Hex: cat(hello(h2(nil))[:27], []byte{0x10, 0x01})},
		{Name: "hello-window-too-small", Kind: "hello", Invalid: "ErrFrame", Hex: hello(h2(func(h *vecHello) { h.Window = MinWindow - 1 }))},
		{Name: "hello-window-too-large", Kind: "hello", Invalid: "ErrFrame", Hex: hello(h2(func(h *vecHello) { h.Window = MaxWindow + 1 }))},
		{Name: "hello-max-sessions-0", Kind: "hello", Invalid: "ErrFrame", Hex: hello(h2(func(h *vecHello) { h.MaxSessions = 0 }))},
		{Name: "hello-max-sessions-too-many", Kind: "hello", Invalid: "ErrFrame", Hex: hello(h2(func(h *vecHello) { h.MaxSessions = maxSessionsWire + 1 }))},
		{Name: "hello-interval-too-short", Kind: "hello", Invalid: "ErrFrame", Hex: hello(h2(func(h *vecHello) { h.IntervalMS = 999 }))},
		{Name: "hello-interval-too-long", Kind: "hello", Invalid: "ErrFrame", Hex: hello(h2(func(h *vecHello) { h.IntervalMS, h.TimeoutMS = 15001, 15001 }))},
		{Name: "hello-timeout-under-interval", Kind: "hello", Invalid: "ErrFrame", Hex: hello(h2(func(h *vecHello) { h.TimeoutMS = 4999 }))},
		{Name: "hello-truncated", Kind: "hello", Invalid: "error", Hex: hello(h2(nil))[:20]},

		{Name: "reply-bound", Kind: "reply", Reply: &vecReply{Status: statusBound, Features: featureReset, Window: 1 << 20, MaxSessions: 1024}},
		{Name: "reply-refused", Kind: "reply", Reply: &vecReply{Status: statusRefused}},
		{Name: "reply-version", Kind: "reply", Reply: &vecReply{Status: statusVersion, MaxVersion: MaxVersion}},
		{Name: "reply-bad-status", Kind: "reply", Invalid: "ErrFrame", Hex: hexBytes{3}},
		{Name: "reply-bound-version-3", Kind: "reply", Invalid: "ErrFrame", Hex: cat([]byte{statusBound, 3}, be(0), be(1<<20), be(1024))},
		{Name: "reply-window-too-small", Kind: "reply", Invalid: "ErrFrame", Hex: cat([]byte{statusBound, 2}, be(0), be(MinWindow-1), be(1024))},
		{Name: "reply-max-sessions-0", Kind: "reply", Invalid: "ErrFrame", Hex: cat([]byte{statusBound, 2}, be(0), be(1<<20), be(0))},

		{Name: "data", Kind: "frame", Frame: &vecFrame{Type: "data", SID: vecSID, Payload: hexBytes("hello")}},
		{Name: "data-empty", Kind: "frame", Frame: &vecFrame{Type: "data", SID: hexBytes("\x02"), Payload: hexBytes{}}},
		{Name: "message", Kind: "frame", Frame: &vecFrame{Type: "message", Payload: hexBytes("_ctl")}},
		{Name: "window", Kind: "frame", Frame: &vecFrame{Type: "window", SID: vecSID, Increment: 0x8000}},
		{Name: "open", Kind: "frame", Frame: &vecFrame{Type: "open", SID: hexBytes("\x03"), Header: hexBytes("agent\x00localhost:22")}},
		{Name: "open-no-header", Kind: "frame", Frame: &vecFrame{Type: "open", SID: hexBytes("\x05"), Header: hexBytes{}}},
		{Name: "close", Kind: "frame", Frame: &vecFrame{Type: "close", SID: vecSID, Code: closeFull}},
		{Name: "close-half", Kind: "frame", Frame: &vecFrame{Type: "close", SID: vecSID, Code: closeHalf}},
		{Name: "close-reset", Kind: "frame", Frame: &vecFrame{Type: "close", SID: vecSID, Code: closeReset}},
		{Name: "ping", Kind: "frame", Frame: &vecFrame{Type: "ping", Ping: hexBytes{1, 2, 3, 4, 5, 6, 7, 8}}},
		{Name: "pong", Kind: "frame", Frame: &vecFrame{Type: "pong", Ping: hexBytes{1, 2, 3, 4, 5, 6, 7, 8}}},
		{Name: "goaway", Kind: "frame", Frame: &vecFrame{Type: "goaway", DrainMS: 5000, Reason: "restart"}},
		{Name: "goaway-no-drain", Kind: "frame", Frame: &vecFrame{Type: "goaway"}},

		{Name: "frame-length-0", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(0), []byte{typeData, 0})},
		{Name: "frame-too-long", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(maxFrame+1), []byte{typeData, 0})},
		{Name: "frame-unknown-type", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(1), []byte{8, 0})},
		{Name: "data-sid-past-body", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(2), []byte{typeData, 2, 1})},
		{Name: "window-short", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(4), []byte{typeWindow, 0, 0, 0, 1})},
		{Name: "window-past-peer-window", Kind: "frame", Invalid: "ErrCredit", Hex: cat(be(6), []byte{typeWindow, 1, 1}, be(1))},
		{Name: "open-header-too-long", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(2+maxHeader+1), []byte{typeOpen, 0})},
		{Name: "close-code-3", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(3), []byte{typeClose, 1, 1, 3})},
		{Name: "close-sid-mismatch", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(4), []byte{typeClose, 1, 1, 1, 0})},
		{Name: "ping-short", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(7), []byte{typePing, 1, 2, 3, 4, 5, 6, 7})},
		{Name: "pong-long", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(9), []byte{typePong, 1, 2, 3, 4, 5, 6, 7, 8, 9})},
		{Name: "goaway-short", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(3), []byte{typeGoAway, 0, 0, 0})},
		{Name: "goaway-reason-too-long", Kind: "frame", Invalid: "ErrFrame", Hex: cat(be(4+maxReason+1), []byte{typeGoAway, 0})},
	}
}

func (h *vecHello) toHello() helloV2 {
	return helloV2{
		maxVersion: h.MaxVersion, features: h.Features, window: h.Window, maxSessions: h.MaxSessions,
		liveness: Liveness{Interval: time.Duration(h.IntervalMS) * time.Millisecond, Timeout: time.Duration(h.TimeoutMS) * time.Millisecond},
		method:   h.AuthMethod, auth: h.Auth,
	}
}

// encode is a valid vector's bytes, written by the package's encoders.
func (v wireVector) encode(t *testing.T) hexBytes {
	t.Helper()
	switch {
	case v.Hello != nil:
		return appendHelloV2(nil, v.Hello.toHello())
	case v.Reply != nil && v.Reply.Status == statusBound:
		return appendReplyV2(nil, replyV2{features: v.Reply.Features, window: v.Reply.Window, maxSessions: v.Reply.MaxSessions})
	case v.Reply != nil: // what Server writes to a hello of version 3, and to one it refuses
		hello := []byte("NFDP\x03")
		if v.Reply.Status == statusRefused {
			hello = appendHelloV2(nil, h2(nil).toHello())
		}
		var out bytes.Buffer
		_, _, _ = Server(context.Background(), bufConn{r: bytes.NewReader(hello), w: &out}, Config{Auth: NoAuth})
		return out.Bytes()
	}
	var out bytes.Buffer
	c := v2Conn(bufConn{w: &out})
	f, sid := v.Frame, string(v.Frame.SID)
	var err error
	switch f.Type {
	case "data":
		c.wmu.Lock() // SendData sends nothing for no bytes; the wire allows an empty data frame
		err = c.frameLocked(typeData, sid, f.Payload)
		c.wmu.Unlock()
	case "message":
		err = c.SendMsg(f.Payload)
	case "window":
		c.sendWindow(sid, int(f.Increment))
	case "open":
		err = c.SendOpen(sid, f.Header)
	case "close":
		switch f.Code {
		case closeFull, closeHalf:
			err = c.SendClose(sid, f.Code == closeHalf)
		default:
			c.peerFeatures = featureReset
			err = c.SendReset(sid)
		}
	case "ping", "pong":
		typ := byte(typePing)
		if f.Type == "pong" {
			typ = typePong
		}
		c.wmu.Lock()
		err = c.frameLocked(typ, "", f.Ping)
		c.wmu.Unlock()
	case "goaway":
		err = c.GoAway(time.Duration(f.DrainMS)*time.Millisecond, f.Reason)
	default:
		t.Fatalf("%s: frame type %q", v.Name, f.Type)
	}
	if err == nil {
		err = c.Flush()
	}
	if err != nil {
		t.Fatalf("%s: %v", v.Name, err)
	}
	return out.Bytes()
}

// decodeHello reads a hello as a listener: Magic and version, then the rest.
func decodeHello(b []byte) (*vecHello, error) {
	var got *vecHello
	cfg := Config{Window: testWindow, Auth: anyAuth{}}
	c, _, err := Server(context.Background(), closingConn{bufConn{r: bytes.NewReader(b), w: io.Discard}}, cfg)
	if err != nil {
		return nil, err
	}
	_ = c.Close()
	h, err := readHelloV2(bytes.NewReader(b[len(Magic)+1:])) // the fields Server does not keep
	if err != nil {
		return nil, err
	}
	got = &vecHello{
		MaxVersion: h.maxVersion, Features: h.features, Window: h.window, MaxSessions: h.maxSessions,
		IntervalMS: uint32(h.liveness.Interval.Milliseconds()), TimeoutMS: uint32(h.liveness.Timeout.Milliseconds()), // #nosec G115 -- test
		AuthMethod: h.method, Auth: h.auth,
	}
	return got, nil
}

func decodeReply(b []byte) (*vecReply, error) {
	r, err := readReplyV2(bytes.NewReader(b))
	var ve *VersionError
	switch {
	case errors.Is(err, ErrRefused):
		return &vecReply{Status: statusRefused}, nil
	case errors.As(err, &ve):
		return &vecReply{Status: statusVersion, MaxVersion: byte(ve.Max)}, nil // #nosec G115 -- a byte on the wire
	case err != nil:
		return nil, err
	}
	return &vecReply{Status: statusBound, Features: r.features, Window: r.window, MaxSessions: r.maxSessions}, nil
}

// decodeFrame reads one frame on a connection where session vecSID is
// open with its full credit; the frames Recv applies are read back from
// their effect. Every byte must be consumed.
func decodeFrame(t *testing.T, name string, b []byte, inc uint32) (*vecFrame, error) {
	c := v2Conn(bufConn{r: bytes.NewReader(b), w: io.Discard})
	c.Credit().Open(string(vecSID))
	if inc > 0 && !c.Credit().Acquire(string(vecSID), int(inc)) { // room for the window frame's grant
		t.Fatalf("%s: acquire", name)
	}
	f, err := c.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	var got *vecFrame
	switch {
	case err == nil && f.Open:
		got = &vecFrame{Type: "open", SID: hexBytes(f.SID), Header: f.Header}
	case err == nil && f.Close:
		code := byte(closeFull)
		if f.Half {
			code = closeHalf
		} else if f.Reset {
			code = closeReset
		}
		got = &vecFrame{Type: "close", SID: hexBytes(f.SID), Code: code}
	case err == nil && f.Msg != nil:
		got = &vecFrame{Type: "message", Payload: f.Msg}
	case err == nil:
		got = &vecFrame{Type: "data", SID: hexBytes(f.SID), Payload: f.Data}
	case closed(c.PeerGoAway()):
		got = &vecFrame{Type: "goaway"} // drain and reason are informational: Recv keeps neither
	case len(c.pong) == 1:
		p := <-c.pong
		got = &vecFrame{Type: "ping", Ping: p[:]}
	case inc > 0 && c.credit.sessions[string(vecSID)].n == testWindow:
		got = &vecFrame{Type: "window", SID: vecSID, Increment: inc}
	default:
		got = &vecFrame{Type: "pong"} // applied with no effect: no GOAWAY of this side's waits on it
	}
	if err == nil {
		if _, err := c.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("%s: bytes left after the frame: %v", name, err)
		}
	}
	return got, nil
}

var vectorErrs = map[string]error{
	"ErrFrame": ErrFrame, "ErrCredit": ErrCredit, "ErrV1": errV1, "ErrVersion": ErrVersion, "ErrRefused": ErrRefused,
}

func TestWireVectors(t *testing.T) {
	gen := wireVectors()
	for i := range gen {
		if gen[i].Invalid == "" {
			gen[i].Hex = gen[i].encode(t)
		}
	}
	if os.Getenv("LINK_UPDATE_GOLDEN") != "" {
		b, err := json.MarshalIndent(gen, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vectorsFile, append(b, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(vectorsFile)
	if err != nil {
		t.Fatal(err)
	}
	var file []wireVector
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	// Encode: the code writes the file's bytes (and the file has every vector).
	if len(gen) != len(file) {
		t.Fatalf("%d vectors, %s has %d (LINK_UPDATE_GOLDEN=1 rewrites it on purpose)", len(gen), vectorsFile, len(file))
	}
	for i := range gen {
		if normalize(gen[i]) != normalize(file[i]) {
			t.Fatalf("vector %s differs from %s (LINK_UPDATE_GOLDEN=1 rewrites it on purpose)", gen[i].Name, vectorsFile)
		}
	}
	// Decode: the file's bytes read as its fields, or are refused.
	for _, v := range file {
		t.Run(v.Name, func(t *testing.T) {
			var got any
			var err error
			switch v.Kind {
			case "hello":
				got, err = decodeHello(v.Hex)
			case "reply":
				got, err = decodeReply(v.Hex)
			case "frame":
				var inc uint32
				if v.Frame != nil {
					inc = v.Frame.Increment
				}
				got, err = decodeFrame(t, v.Name, v.Hex, inc)
			default:
				t.Fatalf("kind %q", v.Kind)
			}
			if v.Invalid != "" {
				want := vectorErrs[v.Invalid]
				if err == nil || (want != nil && !errors.Is(err, want)) {
					t.Fatalf("decoded to %+v, %v; want %s", got, err, v.Invalid)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := any(v.Hello)
			switch {
			case v.Reply != nil:
				want = v.Reply
			case v.Frame != nil:
				f := *v.Frame
				if f.Type == "goaway" {
					f.DrainMS, f.Reason = 0, ""
				}
				if f.Type == "pong" {
					f.Ping = nil
				}
				want = &f
			}
			if normalize(got) != normalize(want) {
				t.Fatalf("decoded %+v, want %+v", got, want)
			}
		})
	}
}

// normalize compares empty and nil byte fields alike.
func normalize(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
