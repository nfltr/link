// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linkdebug

package link

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// debugReports captures debug reports for the test; wait returns the
// first that contains want.
func debugReports(t *testing.T) (wait func(want string, gc bool) string, none func(want string)) {
	var mu sync.Mutex
	var got []string
	f := func(m string) { mu.Lock(); got = append(got, m); mu.Unlock() }
	debugReportTo.Store(&f)
	t.Cleanup(func() { debugReportTo.Store(nil) })
	find := func(want string) string {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range got {
			if strings.Contains(m, want) {
				return m
			}
		}
		return ""
	}
	wait = func(want string, gc bool) string {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if gc {
				runtime.GC()
			}
			if m := find(want); m != "" {
				return m
			}
		}
		t.Fatalf("no debug report with %q", want)
		return ""
	}
	none = func(want string) {
		t.Helper()
		for range 20 {
			runtime.GC()
			time.Sleep(5 * time.Millisecond)
		}
		if m := find(want); m != "" {
			t.Fatalf("unexpected debug report:\n%s", m)
		}
	}
	return wait, none
}

func TestDebugReportsSessionCollectedUnclosed(t *testing.T) {
	wait, none := debugReports(t)
	client := muxPair(t, func(open []byte, s *Session) {
		if string(open) != "debug-handler-leak" {
			_ = s.Close()
		}
		// debug-handler-leak: the handler returns without closing; the
		// opener's Close ends it.
	})
	open := func(h string, closeIt bool) {
		s, err := client.Open([]byte(h))
		if err != nil {
			t.Fatal(err)
		}
		if closeIt {
			_ = s.Close()
		} else {
			<-s.Done() // the peer closed it: dropped, collectable, never closed here
		}
	}
	open("debug-leak", false)
	m := wait(`(header "debug-leak") opened and garbage-collected, never closed`, true)
	if !strings.Contains(m, "TestDebugReportsSessionCollectedUnclosed") {
		t.Fatalf("report without the opener's stack:\n%s", m)
	}
	open("debug-handler-leak", true)
	wait(`(header "debug-handler-leak") accepted and garbage-collected, never closed`, true)
	open("debug-closed", true)
	none(`"debug-closed"`)
}

func TestDebugReportsConcurrentReads(t *testing.T) {
	wait, _ := debugReports(t)
	client := muxPair(t, func(open []byte, s *Session) { <-s.Done(); _ = s.Close() })
	s, err := client.Open([]byte("debug-reads"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _, _ = s.Read(make([]byte, 1)) })
	}
	m := wait(`concurrent Reads on session`, false)
	if strings.Count(m, "TestDebugReportsConcurrentReads") < 2 {
		t.Fatalf("report without both readers' stacks:\n%s", m)
	}
	_ = s.Close()
	wg.Wait()
}

func TestDebugReportsConcurrentWrites(t *testing.T) {
	wait, none := debugReports(t)
	release := make(chan struct{})
	client := muxPair(t, func(open []byte, s *Session) { <-release; _ = s.Close() })
	defer close(release)
	s, err := client.Open([]byte("debug-writes"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// One write at a time is fine.
	_, _ = s.Write([]byte("a"))
	_, _ = s.Write([]byte("b"))
	none("concurrent Writes")
	// Two at once, each past the window so both wait for credit.
	big := make([]byte, 4*testWindow)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _, _ = s.Write(big) })
	}
	wait(`concurrent Writes on session`, false)
	_ = s.Reset()
	wg.Wait()
}

func TestDebugReportsIdleSession(t *testing.T) {
	wait, _ := debugReports(t)
	old := debugIdle
	debugIdle = func() time.Duration { return 20 * time.Millisecond }
	t.Cleanup(func() { debugIdle = old })
	go debugWatchIdle(20 * time.Millisecond)
	client := muxPair(t, func(open []byte, s *Session) { <-s.Done(); _ = s.Close() })
	s, err := client.Open([]byte("debug-idle"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := wait(`(header "debug-idle") opened and open 20ms with no Read or Write`, false)
	if !strings.Contains(m, "TestDebugReportsIdleSession") {
		t.Fatalf("report without the opener's stack:\n%s", m)
	}
}

// A failed open (here: a header past the limit) hands the caller no session
// to close: debug mode must not report it collected unclosed.
func TestDebugFailedOpenNotReported(t *testing.T) {
	_, none := debugReports(t)
	client := muxPair(t, echoAccept)
	if _, err := client.Open(make([]byte, maxHeader+1)); err == nil {
		t.Fatal("an open with a header past the limit succeeded")
	}
	none("garbage-collected")
}
