// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"os"
	"regexp"
	"testing"
)

// TestSpecReadWaitEventsMapped: every event the read, wait and write
// recorders write (trace_read_on.go, trace_wait_on.go, trace_write_on.go
// and their hook points, the queue's in internal/bytequeue) is a step of
// spec/TraceSessionRead.tla, spec/TraceCreditWaiters.tla or
// spec/TraceSessionWrite.tla, and each of theirs is written by some hook
// point (lanes LINKTRACE, LINKTRACE2); a trace's first line (Open) counts as its
// constants' (Log[1].ev): a new step the code logs, or one it stopped
// logging, changes what spec-traces checks.
func TestSpecReadWaitEventsMapped(t *testing.T) {
	for _, c := range []struct {
		spec  string
		files []string
	}{
		// the queue's Stopped and Ended are logged as Pop (trace_read_on.go)
		{"spec/TraceSessionRead.tla", []string{"mux.go", "trace_read_on.go", "internal/bytequeue/bytequeue.go"}},
		{"spec/TraceCreditWaiters.tla", []string{"credit.go", "trace_wait_on.go"}},
		{"spec/TraceSessionWrite.tla", []string{"trace_write_on.go"}},
	} {
		code := map[string]bool{}
		lit := regexp.MustCompile(`trace(?:Read|Wait)\("([A-Za-z]+)"|"ev": "([A-Za-z]+)"|q\.trace\("([A-Za-z]+)"|\bev :?= "([A-Za-z]+)"`)
		for _, f := range c.files {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range lit.FindAllStringSubmatch(string(b), -1) {
				if e := m[1] + m[2] + m[3] + m[4]; e != "Stopped" && e != "Ended" {
					code[e] = true
				}
			}
		}
		b, err := os.ReadFile(c.spec)
		if err != nil {
			t.Fatal(err)
		}
		spec := map[string]bool{}
		for _, m := range regexp.MustCompile(`\.ev = "([A-Za-z]+)"`).FindAllStringSubmatch(string(b), -1) {
			spec[m[1]] = true
		}
		if len(code) < 3 {
			t.Fatalf("%s: found only %d trace events in the code: the pattern no longer matches the hooks", c.spec, len(code))
		}
		for e := range code {
			if !spec[e] {
				t.Errorf("the recorder writes %q, which no step of %s maps", e, c.spec)
			}
		}
		for e := range spec {
			if !code[e] {
				t.Errorf("%s maps %q, but no hook point of the code writes it", c.spec, e)
			}
		}
	}
}
