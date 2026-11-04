// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mbtFiles are the behaviours TLC -simulate wrote for the model-based
// tests (scripts/spec.sh behaviours): testdata/mbt, or MBT_DIR (nightly:
// a fresh, larger set).
func mbtFiles(t *testing.T, prefix string) []string {
	t.Helper()
	dir := os.Getenv("MBT_DIR")
	if dir == "" {
		dir = filepath.Join("testdata", "mbt")
	}
	fs, err := filepath.Glob(filepath.Join(dir, prefix+"-*.ndjson"))
	if err != nil || len(fs) == 0 {
		t.Fatalf("no behaviours %s-*.ndjson in %s (scripts/spec.sh behaviours): %v", prefix, dir, err)
	}
	return fs
}

// mbtRead decodes one behaviour, a state per line, into rows of T.
func mbtRead[T any](t *testing.T, f string) []T {
	t.Helper()
	fh, err := os.Open(f)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	var rows []T
	sc := bufio.NewScanner(fh)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var r T
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// liveRow is a state of spec/Liveness.tla (spec/MBTLive.tla) and the
// action that led to it.
type liveRow struct {
	A                      string
	Now, Rx, PingAt, Timer int
	Dead, Looked           bool
}

// TestLivenessFollowsTheModel replays every Fire step of the model's
// behaviours on keepAlive's decision (liveFire), one tick a nanosecond,
// with Liveness.cfg's Interval 2, Timeout 4 and Gap 1: the code must reach
// the model's next state (the verdict, the timer's deadline, pingAt,
// looked), and ping exactly when the model does.
func TestLivenessFollowsTheModel(t *testing.T) {
	const iv, to, gap = 2, 4, 1 // spec/Liveness.cfg
	steps := 0
	for _, f := range mbtFiles(t, "live") {
		rows := mbtRead[liveRow](t, f)
		for i := 1; i < len(rows); i++ {
			prev, next := rows[i-1], rows[i]
			if next.A != "Fire" {
				continue
			}
			steps++
			d := func(n int) time.Duration { return time.Duration(n) }
			dead, ping, due, at, look := liveFire(d(next.Now), d(next.Rx), d(prev.PingAt), prev.Looked, iv, to, gap)
			mping := next.PingAt == next.Now && !(prev.PingAt >= 0 && prev.Rx < prev.PingAt)
			if dead != next.Dead || (!dead && (due != d(next.Timer) || at != d(next.PingAt) || look != next.Looked)) || ping != mping {
				t.Errorf("%s step %d (now %d, rx %d, pingAt %d, looked %v): code dead %v, ping %v, timer %d, pingAt %d, looked %v; model dead %v, ping %v, timer %d, pingAt %d, looked %v",
					filepath.Base(f), i, next.Now, next.Rx, prev.PingAt, prev.Looked, dead, ping, due, at, look, next.Dead, mping, next.Timer, next.PingAt, next.Looked)
			}
		}
	}
	if steps == 0 {
		t.Fatal("no Fire step in the behaviours")
	}
	t.Logf("%d Fire steps replayed", steps)
}
