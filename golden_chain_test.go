// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

// The FeatureChain recording: testdata/v2_chain_session.golden, the
// frames on the link between an origin and hop 1 of a 2-hop path (hop 1
// forwards to a far end): the hello and reply announcing FeatureChain,
// what the origin sends (two CHAIN-OPENs, data before the answer, a half
// close) and what hop 1 sends back (the ACCEPT with the path's length,
// data, a refusal of the second session from hop 2, then a mid-session
// abort of the first from hop 2). LINK_UPDATE_GOLDEN=1 writes it.
const chainGolden = "testdata/v2_chain_session.golden"

var chainTrace = Trace{0xc0, 0xff, 0xee, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

func chainRecord(t *testing.T) map[string][]byte {
	t.Helper()
	segs := map[string][]byte{}
	var out bytes.Buffer
	reply := appendReplyV2(nil, replyV2{features: features, window: 0x00040000, maxSessions: 512})
	hc, err := Client(context.Background(), closingConn{bufConn{r: bytes.NewReader(reply), w: &out}},
		Config{Window: 0x00050000, Liveness: LivenessFast, Auth: tokenAuth(v2Token)})
	if err != nil {
		t.Fatal(err)
	}
	_ = hc.Close()
	segs["hello"], segs["reply-bound"] = bytes.Clone(out.Bytes()), reply

	out.Reset()
	o := v2Conn(bufConn{w: &out})
	o.features, o.peerFeatures = features, features
	for _, err := range []error{
		o.sendOpen("\x01", v2Header, Chain{Trace: chainTrace}),
		o.SendData("\x01", []byte("hello")), // before the answer: no round trip
		o.sendOpen("\x03", []byte("agent\x00elsewhere:22"), Chain{Trace: chainTrace}),
		o.SendClose("\x01", true),
		o.Flush(),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	segs["origin"] = bytes.Clone(out.Bytes())

	out.Reset()
	h := v2Conn(bufConn{w: &out})
	h.features, h.peerFeatures = features, features
	for _, err := range []error{
		h.sendAccept("\x01", 1),
		h.SendData("\x01", []byte("hello back")),
		h.sendAbort("\x03", &Abort{Refused: true, Code: CodeHopLimit, Hops: 1, Msg: "the path crossed 8 hops"}),
		h.sendAbort("\x01", &Abort{Code: CodeConnLost, Hops: 1, Msg: "dead peer"}),
		h.Flush(),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	segs["hop"] = out.Bytes()
	return segs
}

// TestChainGolden: the code writes the recorded bytes; the hop's side
// reads them back as the frames recorded, and the origin's.
func TestChainGolden(t *testing.T) {
	got := chainRecord(t)
	names := []string{"hello", "reply-bound", "origin", "hop"}
	if os.Getenv("LINK_UPDATE_GOLDEN") != "" {
		var b strings.Builder
		for _, name := range names {
			fmt.Fprintf(&b, "%s %x\n", name, got[name])
		}
		if err := os.WriteFile(chainGolden, []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want := readGoldenFile(t, chainGolden)
	for _, name := range names {
		if !bytes.Equal(got[name], want[name]) {
			t.Fatalf("%s:\ngot  %x\nwant %x", name, got[name], want[name])
		}
	}
	if h := want["hello"]; h[9]&byte(FeatureChain|featureReset) != 3 {
		t.Fatalf("hello features %x", h[6:10])
	}

	recv := func(b []byte, opened ...string) []Frame {
		c := v2Conn(bufConn{r: bytes.NewReader(b)})
		c.features, c.peerFeatures = features, features
		for _, sid := range opened {
			c.credit.open(sid, byUs)
		}
		var fs []Frame
		for {
			f, err := c.Recv()
			if errors.Is(err, io.EOF) {
				return fs
			}
			if err != nil {
				t.Fatalf("Recv after %d frames: %v", len(fs), err)
			}
			fs = append(fs, f)
		}
	}
	ch := Chain{Trace: chainTrace}
	if fs, want := recv(want["origin"]), []Frame{
		{SID: "\x01", Open: true, Header: v2Header, Chain: ch},
		{SID: "\x01", Data: []byte("hello")},
		{SID: "\x03", Open: true, Header: []byte("agent\x00elsewhere:22"), Chain: ch},
		{SID: "\x01", Close: true, Half: true},
	}; !reflect.DeepEqual(fs, want) {
		t.Fatalf("origin's frames %+v\nwant %+v", fs, want)
	}
	if fs, want := recv(want["hop"], "\x01", "\x03"), []Frame{
		{SID: "\x01", Accept: true, Chain: Chain{Hops: 1}},
		{SID: "\x01", Data: []byte("hello back")},
		{SID: "\x03", Close: true, Reset: true, Abort: &Abort{Refused: true, Code: CodeHopLimit, Hops: 1, Msg: "the path crossed 8 hops"}},
		{SID: "\x01", Close: true, Reset: true, Abort: &Abort{Code: CodeConnLost, Hops: 1, Msg: "dead peer"}},
	}; !reflect.DeepEqual(fs, want) {
		t.Fatalf("hop's frames %+v\nwant %+v", fs, want)
	}
}
