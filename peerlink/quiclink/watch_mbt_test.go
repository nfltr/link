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
	A               string
	Now, Seen, Prev int
	Fresh           bool
	Closed          string
}

// TestWatchFollowsTheModel replays every WatchAt step of the model's
// behaviours (link's testdata/mbt/qwatch-<cfg>-*.ndjson, written by
// scripts/spec.sh behaviours; MBT_DIR for another set) on watchState.tick,
// a model tick a millisecond: the code must reach the model's seen, prev
// and verdict. G = 4 and Quiet = K + Timeout/2 are each configuration's.
func TestWatchFollowsTheModel(t *testing.T) {
	dir := os.Getenv("MBT_DIR")
	if dir == "" {
		dir = filepath.Join("..", "..", "testdata", "mbt")
	}
	quiet := map[string]int{"QuicWatch": 16, "QuicWatchPTO": 20, "QuicWatchStarve": 16} // spec/QuicWatch*.cfg
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	t0 := time.Unix(0, 0)
	steps, verdicts, gaps := 0, 0, 0
	for cfg, q := range quiet {
		fs, _ := filepath.Glob(filepath.Join(dir, "qwatch-"+cfg+"-*.ndjson"))
		if len(fs) == 0 {
			t.Fatalf("no behaviours qwatch-%s-*.ndjson in %s (scripts/spec.sh behaviours)", cfg, dir)
		}
		for _, f := range fs {
			rows := readRows(t, f)
			for i := 1; i < len(rows); i++ {
				prev, next := rows[i-1], rows[i]
				if next.A != "WatchAt" {
					continue
				}
				steps++
				var n uint64
				if prev.Fresh {
					n = 1
				}
				w := watchState{seen: t0.Add(ms(prev.Seen)), prev: t0.Add(ms(prev.Prev))}
				dead := w.tick(t0.Add(ms(next.Now)), n, ms(4), ms(q))
				if dead != (next.Closed == "watch") || w.seen != t0.Add(ms(next.Seen)) || w.prev != t0.Add(ms(next.Now)) {
					t.Fatalf("%s line %d: tick at %d from seen %d prev %d fresh %v: code dead %v seen %v, model %+v",
						filepath.Base(f), i+1, next.Now, prev.Seen, prev.Prev, prev.Fresh, dead, w.seen.Sub(t0), next)
				}
				if dead {
					verdicts++
				}
				if !prev.Fresh && next.Seen != prev.Seen {
					gaps++
				}
			}
		}
	}
	if verdicts == 0 || gaps == 0 {
		t.Fatalf("the behaviours take no verdict (%d) or no discounted gap (%d)", verdicts, gaps)
	}
	t.Logf("%d ticks replayed, %d verdicts, %d gaps discounted", steps, verdicts, gaps)
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
