// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package peerlink

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Trace mode (go test -tags linktrace, LINKTRACE=<dir>) writes one JSON
// line per step of spec/Join.tla a joint of a plain conn (the client) and
// a link session takes, to <dir>/join-<n>.ndjson, under the step's lock
// and one process-wide lock; spec/TraceJoin.tla checks each file against
// the model (make spec-traces). The client's, the peer's and the
// network's steps are not logged: the check finds them.
//
//	Copy        client bytes written onto the session (one model byte)
//	CloseWrite  the client's FIN: the session's CloseWrite, its Sync next
//	Fin         the peer's FIN read; n: 1 held while the Sync waits, 0 passed on
//	Synced      the Sync answered
//	Finish      both directions ended cleanly: the client closed
//	Lost        the client aborted (the session's error: the connection's end)
const traceOn = true

type traceState = *joinTrace

type joinTrace struct {
	path   string
	client int  // the index of the plain conn in joint.c
	lost   bool // Lost written: the client is aborted once (traceMu)
}

var (
	traceMu    sync.Mutex
	traceFiles = map[string]*os.File{}
	traceN     atomic.Int64
)

// traceStart starts j's trace when LINKTRACE names a directory and j joins
// exactly one link session to a plain conn.
func traceStart(j *joint) {
	dir := os.Getenv("LINKTRACE")
	a, b := native(j.c[0]) != nil, native(j.c[1]) != nil
	if dir == "" || a == b {
		return
	}
	client := 0
	if a {
		client = 1
	}
	j.tr = &joinTrace{client: client, path: filepath.Join(dir, fmt.Sprintf("join-%d.ndjson", traceN.Add(1)))}
}

// traceJoin writes one step of j's.
func traceJoin(j *joint, ev string, n int) {
	t := j.tr
	if t == nil {
		return
	}
	b, err := json.Marshal(map[string]any{"ev": ev, "n": n})
	if err != nil {
		panic(err)
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	if ev == "Lost" {
		if t.lost {
			return
		}
		t.lost = true
	}
	f := traceFiles[t.path]
	if f == nil {
		if f, err = os.OpenFile(t.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err != nil { // #nosec G304 -- the tester's LINKTRACE
			panic(err)
		}
		traceFiles[t.path] = f
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}

// traceClient reports whether c is j's client (the plain conn).
func traceClient(j *joint, c net.Conn) bool { return j.tr != nil && j.c[j.tr.client] == c }
