// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLivenessPresets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		want      Liveness
		deadAfter time.Duration // the peer keeps the interval asked
	}{
		{"default", LivenessDefault, 15 * time.Second},
		{"fast", LivenessFast, 4500 * time.Millisecond},
		{"nat", LivenessNAT, 30 * time.Second},
		{"datacenter", LivenessDatacenter, 45 * time.Second},
		{"mobile", LivenessMobile, 60 * time.Second},
	} {
		l, err := ParseLiveness(tc.name)
		if err != nil || l != tc.want {
			t.Fatalf("ParseLiveness(%q) = %+v, %v", tc.name, l, err)
		}
		if got := l.DeadAfter(l.Interval); got != tc.deadAfter {
			t.Errorf("%s: dead after %s, want %s", tc.name, got, tc.deadAfter)
		}
		if l.String() != tc.name {
			t.Errorf("%s: String() = %q", tc.name, l.String())
		}
	}
	if len(LivenessPresets) != len(livenessByName) {
		t.Fatal("LivenessPresets and the table differ")
	}
	if l, err := ParseLiveness(""); err != nil || l != LivenessDefault {
		t.Fatalf(`ParseLiveness("") = %+v, %v`, l, err)
	}
}

func TestParseLivenessRawAndInvalid(t *testing.T) {
	for spec, want := range map[string]Liveness{
		"interval=2s,timeout=6s":   {2 * time.Second, 6 * time.Second},
		"mobile,timeout=90s":       {15 * time.Second, 90 * time.Second},
		" FAST , interval=2s ":     {2 * time.Second, 3 * time.Second},
		"interval=1s,timeout=1s":   {time.Second, time.Second},
		"timeout=12s,interval=10s": {10 * time.Second, 12 * time.Second},
	} {
		if l, err := ParseLiveness(spec); err != nil || l != want {
			t.Errorf("ParseLiveness(%q) = %+v, %v; want %+v", spec, l, err, want)
		}
	}
	for spec, msg := range map[string]string{
		"slow":                     "not a preset",
		"interval=500ms":           "want 1s to 15s",
		"interval=30s,timeout=60s": "want 1s to 15s",
		"interval=4s,timeout=3s":   "at least the interval",
		"interval=abc":             "invalid duration",
		"fast,mobile":              "want a preset first",
		"timeout=5s,fast":          "want a preset first",
		"ping=30s":                 "want a preset first",
	} {
		if _, err := ParseLiveness(spec); err == nil || !strings.Contains(err.Error(), msg) {
			t.Errorf("ParseLiveness(%q) = %v, want an error with %q", spec, err, msg)
		}
	}
}

// A peer that keeps another interval than the one asked (an older peer's
// fixed 15s, a peer's bounds) is never declared dead before three of its
// intervals; one that states none leaves liveness to the transport.
func TestLivenessDeadAfterFollowsTheStatedInterval(t *testing.T) {
	if got := LivenessFast.DeadAfter(5 * time.Second); got != 15*time.Second {
		t.Errorf("fast against a 5s peer: %s, want 15s", got)
	}
	if got := LivenessMobile.DeadAfter(5 * time.Second); got != 50*time.Second {
		t.Errorf("mobile against a 5s peer: %s, want 50s", got)
	}
	if got := LivenessDefault.DeadAfter(0); got != 0 {
		t.Errorf("no statement: %s, want 0", got)
	}
}

// A frame that takes longer to arrive than Interval + Timeout, its bytes
// coming all along, is no dead peer: the bytes of a frame are as much a
// sign of life as its header (spec/README.md, "The Pool and liveness",
// the slow frame).
func TestSlowFrameIsNoDeadPeer(t *testing.T) {
	live := Liveness{Interval: time.Second, Timeout: time.Second}
	for _, size := range []int{viewMax / 2, 4 * viewMax} { // read ahead; read straight into the frame
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			a, raw := net.Pipe()
			c := newConnV2(a, Config{Window: testWindow, MaxSessions: MaxSessions, Liveness: live}, testWindow, MaxSessions, false)
			t.Cleanup(func() { _ = c.Close(); _ = raw.Close() })
			go func() { _, _ = io.Copy(io.Discard, raw) }() // c's pings
			c.up(nil, Identity{})
			got := make(chan error, 1)
			go func() {
				f, err := c.Recv()
				if err == nil && len(f.Msg) != size {
					err = fmt.Errorf("message of %d bytes, want %d", len(f.Msg), size)
				}
				got <- err
			}()
			// The header, then the body a few bytes at a time over
			// 3 x (Interval + Timeout); the peer never answers a ping.
			if _, err := raw.Write(frame(uint32(size), typeMsg)); err != nil {
				t.Fatal(err)
			}
			body := make([]byte, size)
			step := size / 60
			for off := 0; off < size; off += step {
				time.Sleep(100 * time.Millisecond)
				if _, err := raw.Write(body[off:min(off+step, size)]); err != nil {
					t.Fatalf("at %d of %d: %v (%v)", off, size, err, c.Err())
				}
			}
			if err := <-got; err != nil {
				t.Fatal(err)
			}
		})
	}
}
