// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package benchprof writes the goroutine profiles of link's benchmark
// binaries (link, pathswitch, quiclink). Run is their TestMain: with
// $LINK_PERF_EXIT_PROFILE set (scripts/perf.sh baseline), once the run
// ends it writes Write's profiles to that path: what the benchmarks left
// behind.
package benchprof

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Run runs the tests and benchmarks, writes the exit profiles when asked
// and returns the exit code for os.Exit.
func Run(m *testing.M) int {
	code := m.Run()
	if p := os.Getenv("LINK_PERF_EXIT_PROFILE"); p != "" {
		settle()
		if err := Write(p); err != nil {
			_, _ = os.Stderr.WriteString("benchprof: " + err.Error() + "\n")
			code = 1
		}
	}
	return code
}

// Write writes the goroutine profile (debug=2: every stack) to
// PREFIX.goroutine.txt and the goroutine leak profile to
// PREFIX.goroutineleak.txt, at debug=1: its "total N" counts the leaked
// goroutines only (debug=2 would dump every goroutine).
func Write(prefix string) error {
	if err := write(prefix+".goroutine.txt", "goroutine", 2); err != nil {
		return err
	}
	return write(prefix+".goroutineleak.txt", "goroutineleak", 1)
}

// settle waits for the goroutines a closed rig is still ending: until the
// count holds for 50 ms, at most 2 s.
// ponytail: a poll on NumGoroutine; a goroutine parked longer than 50 ms
// before exiting is counted as left behind (the leak profile tells them apart).
func settle() {
	n, same := runtime.NumGoroutine(), 0
	for deadline := time.Now().Add(2 * time.Second); same < 5 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
		if m := runtime.NumGoroutine(); m == n {
			same++
		} else {
			n, same = m, 0
		}
	}
}

func write(path, name string, debug int) error {
	prof := pprof.Lookup(name)
	if prof == nil {
		return nil // a toolchain without the leak profile
	}
	f, err := os.Create(path) // #nosec G304 G703 -- the operator's directory
	if err != nil {
		return err
	}
	if err := prof.WriteTo(f, debug); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
