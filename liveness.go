// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"fmt"
	"strings"
	"time"
)

// Liveness is how a connection notices its peer is gone without a FIN (it
// lost power, a NAT dropped the path): the peer sends a frame at least
// every Interval while idle, and the connection is dead once Interval +
// Timeout pass without any frame. Where one side sends the frames (its
// heartbeat), the other asks for Interval, the sender states the interval
// it keeps, and the asker waits DeadAfter(stated).
type Liveness struct {
	Interval time.Duration
	Timeout  time.Duration
}

// The presets. Idle traffic is one small frame each way per Interval per
// connection; the bound is Interval + Timeout from the peer's last frame.
var (
	// LivenessDefault: dead within 15 s, a frame each way every 5 s.
	LivenessDefault = Liveness{Interval: 5 * time.Second, Timeout: 10 * time.Second}
	// LivenessFast: quick failover and interactive use; dead within 4.5 s,
	// a frame each way every 1.5 s.
	LivenessFast = Liveness{Interval: 1500 * time.Millisecond, Timeout: 3 * time.Second}
	// LivenessNAT: long idle behind NATs, conntrack and load balancers that
	// drop a mapping idle for 30 s or more; dead within 30 s, a frame each
	// way every 10 s.
	LivenessNAT = Liveness{Interval: 10 * time.Second, Timeout: 20 * time.Second}
	// LivenessDatacenter: stable networks; dead within 45 s, a frame each
	// way every 15 s.
	LivenessDatacenter = Liveness{Interval: 15 * time.Second, Timeout: 30 * time.Second}
	// LivenessMobile: laptops and phones on flaky Wi-Fi or metered links:
	// the fewest wakeups, and a radio that sleeps or hands over is not
	// taken for a dead peer; dead within 60 s, a frame each way every 15 s.
	LivenessMobile = Liveness{Interval: 15 * time.Second, Timeout: 45 * time.Second}
)

// LivenessPresets names the presets, in the order help and docs list them.
var LivenessPresets = []string{"default", "fast", "nat", "datacenter", "mobile"}

var livenessByName = map[string]Liveness{
	"default": LivenessDefault, "fast": LivenessFast, "nat": LivenessNAT,
	"datacenter": LivenessDatacenter, "mobile": LivenessMobile,
}

// Bounds Validate enforces. A heartbeat sender keeps an interval in
// [MinLivenessInterval, MaxLivenessInterval]: below it the frames cost more
// than they tell, above it a 45 s limit for a silent peer (one that answers
// each frame) would drop the connection. Timeout is at least one more
// interval, since a peer skips a frame right after sending data.
const (
	MinLivenessInterval = time.Second
	MaxLivenessInterval = 15 * time.Second
)

// ParseLiveness reads a preset name, raw values ("interval=2s,timeout=6s"),
// or a preset with raw values over it ("mobile,timeout=90s"); "" is
// LivenessDefault. The result is validated.
func ParseLiveness(spec string) (Liveness, error) {
	l := LivenessDefault
	for i, part := range strings.Split(strings.TrimSpace(spec), ",") {
		part = strings.TrimSpace(part)
		key, val, raw := strings.Cut(part, "=")
		switch {
		case part == "" && i == 0:
		case !raw && i == 0:
			p, ok := livenessByName[strings.ToLower(part)]
			if !ok {
				return Liveness{}, fmt.Errorf("liveness %q: not a preset (%s) or interval=<duration>,timeout=<duration>", part, strings.Join(LivenessPresets, ", "))
			}
			l = p
		case raw && (key == "interval" || key == "timeout"):
			d, err := time.ParseDuration(strings.TrimSpace(val))
			if err != nil {
				return Liveness{}, fmt.Errorf("liveness %s: %w", key, err)
			}
			if key == "interval" {
				l.Interval = d
			} else {
				l.Timeout = d
			}
		default:
			return Liveness{}, fmt.Errorf("liveness %q: want a preset first, then interval=<duration> or timeout=<duration>", part)
		}
	}
	return l, l.Validate()
}

// Validate refuses values a heartbeat sender would not keep or that would
// take a live connection for a dead one.
func (l Liveness) Validate() error {
	if l.Interval < MinLivenessInterval || l.Interval > MaxLivenessInterval {
		return fmt.Errorf("liveness interval %s: want %s to %s", l.Interval, MinLivenessInterval, MaxLivenessInterval)
	}
	if l.Timeout < l.Interval {
		return fmt.Errorf("liveness timeout %s: want at least the interval (%s): a peer skips a frame right after sending data", l.Timeout, l.Interval)
	}
	return nil
}

// DeadAfter is how long to wait for any frame once the peer stated the
// interval it keeps: stated + Timeout, and never under three stated
// intervals (a peer that keeps another interval than the one asked for, or
// skips a frame after data). 0 when the peer stated none: liveness is then
// the transport keepalive's.
func (l Liveness) DeadAfter(stated time.Duration) time.Duration {
	if stated <= 0 {
		return 0
	}
	return max(stated+l.Timeout, 3*stated)
}

// String is the spec ParseLiveness reads back.
func (l Liveness) String() string {
	for _, name := range LivenessPresets {
		if livenessByName[name] == l {
			return name
		}
	}
	return fmt.Sprintf("interval=%s,timeout=%s", l.Interval, l.Timeout)
}
