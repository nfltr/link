// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	quic "github.com/nfltr/quic-go"
)

// watchRow is a state of spec/QuicWatch.tla (spec/MBTWatch.tla) and the
// action that led to it.
type watchRow struct {
	A                           string
	Now, Seen, Prev, Idle, Look int
	Fresh                       bool
	Closed                      string
}

// TestWatchFollowsTheModel replays every WatchAt step of the model's
// behaviours (link's testdata/mbt/qwatch-<cfg>-*.ndjson, written by
// scripts/spec.sh behaviours; MBT_DIR for another set) on watchState.tick,
// a model tick a millisecond, with the model's idle start (IdleStart), and
// every Look on watchState.look: the code must reach the model's seen,
// prev and verdict (its first look at WatchAt, the close at Look),
// including the verdicts the idle start withholds (a keep-alive a stop
// delayed, QuicWatchStops: finding QW2). The second look taking a verdict
// back (finding QW4) is TestWatchSecondLookTakesBack's: random walks do not
// reach it (none in 200000 QuicWatchStops walks: the peer's own packets
// keep coming). G = 4 and Quiet = K + Timeout/2 are each configuration's.
func TestWatchFollowsTheModel(t *testing.T) {
	dir := os.Getenv("MBT_DIR")
	if dir == "" {
		dir = filepath.Join("..", "..", "testdata", "mbt")
	}
	quiet := map[string]int{"QuicWatch": 16, "QuicWatchPTO": 20, "QuicWatchStarve": 16, "QuicWatchStops": 16} // spec/QuicWatch*.cfg
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	t0 := time.Unix(0, 0)
	steps, verdicts, gaps, withheld, looks := 0, 0, 0, 0, 0
	for cfg, q := range quiet {
		fs, _ := filepath.Glob(filepath.Join(dir, "qwatch-"+cfg+"-*.ndjson"))
		if len(fs) == 0 {
			t.Fatalf("no behaviours qwatch-%s-*.ndjson in %s (scripts/spec.sh behaviours)", cfg, dir)
		}
		for _, f := range fs {
			rows := readRows(t, f)
			for i := 1; i < len(rows); i++ {
				prev, next := rows[i-1], rows[i]
				var n uint64
				if prev.Fresh {
					n = 1
				}
				w := watchState{seen: t0.Add(ms(prev.Seen)), prev: t0.Add(ms(prev.Prev))}
				if next.A == "Look" {
					dead := w.look(t0.Add(ms(next.Now)), n)
					if dead != (next.Closed == "watch") || w.seen != t0.Add(ms(next.Seen)) {
						t.Fatalf("%s line %d: second look at %d (first %d) from seen %d fresh %v: code dead %v seen %v, model %+v",
							filepath.Base(f), i+1, next.Now, prev.Look, prev.Seen, prev.Fresh, dead, w.seen.Sub(t0), next)
					}
					looks++
					continue
				}
				if next.A != "WatchAt" {
					continue
				}
				steps++
				dead := w.tick(t0.Add(ms(next.Now)), n, t0.Add(ms(next.Idle)), ms(4), ms(q))
				if dead != (next.Look >= 0) || w.seen != t0.Add(ms(next.Seen)) || w.prev != t0.Add(ms(next.Now)) {
					t.Fatalf("%s line %d: tick at %d from seen %d prev %d fresh %v: code dead %v seen %v, model %+v",
						filepath.Base(f), i+1, next.Now, prev.Seen, prev.Prev, prev.Fresh, dead, w.seen.Sub(t0), next)
				}
				if dead {
					verdicts++
				}
				if !prev.Fresh && next.Seen != prev.Seen {
					gaps++
				} else if !prev.Fresh && !dead && next.Now-prev.Seen >= q {
					withheld++
				}
			}
		}
	}
	if verdicts == 0 || gaps == 0 || withheld == 0 || looks == 0 {
		t.Fatalf("the behaviours take no verdict (%d), no discounted gap (%d), no verdict withheld by the idle start (%d) or no second look (%d)", verdicts, gaps, withheld, looks)
	}
	t.Logf("%d ticks replayed, %d verdicts, %d gaps discounted, %d verdicts withheld by the idle start, %d second looks", steps, verdicts, gaps, withheld, looks)
}

// TestWatchSecondLookTakesBack: TLC's walk to WitnessSecondLook
// (QuicWatchStops.cfg; mutant one-look, the code before QW4, closes here),
// a model tick a millisecond: the keep-alive went out at 9, the peer's
// answer reached the socket, and the process stopped from 12 to 17 (no
// gap: 5 is not over 1.25 ticks). The tick at 17 runs before quic-go
// reads the answer: nothing new for Quiet (16) and Timeout/2 since the
// keep-alive, the verdict. quic-go then reads it; the second look, D = 1
// later, takes the verdict back.
func TestWatchSecondLookTakesBack(t *testing.T) {
	ms := func(n int) time.Time { return time.Unix(0, 0).Add(time.Duration(n) * time.Millisecond) }
	w := watchState{seen: ms(0), prev: ms(12)}
	if !w.tick(ms(17), 0, ms(9), 4*time.Millisecond, 16*time.Millisecond) {
		t.Fatal("tick at 17: no verdict, want the first look's")
	}
	if w.look(ms(18), 1) {
		t.Fatal("second look at 18 with the answer read: dead, want the verdict taken back (finding QW4)")
	}
	if w.seen != ms(18) || w.last != 1 {
		t.Fatalf("after the second look: seen %v last %d, want 18 ms and 1", w.seen.Sub(ms(0)), w.last)
	}
	if !w.look(ms(23), 1) {
		t.Fatal("a second look with nothing new: alive, want dead")
	}
}

func readRows(t *testing.T, f string) []watchRow {
	t.Helper()
	b, err := os.ReadFile(f)
	if err != nil {
		t.Fatal(err)
	}
	var rows []watchRow
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var r watchRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		rows = append(rows, r)
	}
	return rows
}

// TestKeepAliveAfter: quic-go's keep-alive period, Interval or 1.5 PTO.
func TestKeepAliveAfter(t *testing.T) {
	iv := 1500 * time.Millisecond
	for _, c := range []struct {
		srtt, dev, want time.Duration
	}{
		{0, 0, iv}, // no sample yet
		{time.Millisecond, time.Millisecond, iv},
		{800 * time.Millisecond, 300 * time.Millisecond, (800 + 1200 + 25) * time.Millisecond * 3 / 2},
		{2 * time.Millisecond, 0, iv}, // the 1 ms floor
	} {
		if got := keepAliveAfter(iv, quic.ConnectionStats{SmoothedRTT: c.srtt, MeanDeviation: c.dev}); got != c.want {
			t.Errorf("srtt %v dev %v: %v, want %v", c.srtt, c.dev, got, c.want)
		}
	}
}
