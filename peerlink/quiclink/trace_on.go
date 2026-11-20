// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package quiclink

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

// Trace mode (go test -tags linktrace, LINKTRACE_ENDACK=<dir>): both ends
// of a session on an ALPNEndAck connection write one JSON line per step of
// link's spec/EndAckLeg.tla (Kind quic) they take to one file per stream
// (the connection's two addresses and the stream id), under one
// process-wide lock, each logged before the control message it sends and
// after the one it read, so the file's order is one the two ends could
// have taken; spec/TraceEndAckLeg.tla checks the direction of the first
// SClose (make spec-traces). by: "o" the opener's line, "a" the
// accepter's.
//
//	SClose  CloseWriteSync: the FIN, then the wait
//	SRecv   t: "eack" ('E') or "abort" ('A') read for the session
//	SRet    r: the wait's result, "clean" or "err"
//	SIdle   the wait gave up at the idle bound
//	Cut     the wait saw the connection end
//	REnd    Read returned io.EOF; ack: "1" when it sent 'E'
//	RFail   'A' from a Reset (Abort, Refuse)
//	RClose  'A' from a Close before the end was read
//	RMute   the test's far end that reads nothing (TraceMuteForTest)
const traceOn = true

var (
	traceMu    sync.Mutex
	traceFiles = map[string]*os.File{}
)

func traceEA(s *Session, ev string, kv ...string) {
	dir := os.Getenv("LINKTRACE_ENDACK")
	if dir == "" {
		return
	}
	a, b := port(s.c.q.LocalAddr()), port(s.c.q.RemoteAddr()) // a side's local address may be unspecified
	if b < a {
		a, b = b, a
	}
	name := fmt.Sprintf("%d-%d-%d", a, b, s.StreamID())
	m := map[string]string{"ev": ev, "by": "o"}
	if s.accepter {
		m["by"] = "a"
	}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	line, _ := json.Marshal(m)
	traceMu.Lock()
	defer traceMu.Unlock()
	f := traceFiles[name]
	if f == nil {
		var err error
		if f, err = os.Create(dir + "/" + name + ".ndjson"); err != nil {
			panic(err)
		}
		traceFiles[name] = f
	}
	_, _ = f.Write(append(line, '\n'))
}

func port(a net.Addr) int {
	if u, ok := a.(*net.UDPAddr); ok {
		return u.Port
	}
	return 0
}
