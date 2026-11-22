// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linktrace

package quiclink

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/nfltr/quic-go"
	"github.com/nfltr/quic-go/qlog"
	"github.com/nfltr/quic-go/qlogwriter"
)

// Window trace (go test -tags linktrace, LINKTRACE_AUTOWIN=<dir>): the
// steps of link's spec/AutoWindow.tla that one stream direction takes, from
// both ends' qlog events (Config.Tracer: no hook in the fork) and the
// receiver's reads, one JSON line each to
// <dir>/<odcid>-<stream>-<c|s>.ndjson, the receiving end's role last,
// under one process-wide lock; spec/TraceAutoWindow.tla checks it (make
// spec-traces). A side's packet is logged before it goes out and after it
// was handled, so a line for the far end comes after the one it answers.
//
//	Up      the receiver's InitialStreamReceiveWindow n and MaxStreamReceiveWindow max (the file's first line)
//	Send    the sender sent a STREAM frame ending at n (a resend: an n not past the last)
//	Data    the receiver got a STREAM frame ending at n
//	Update  the receiver sent MAX_STREAM_DATA n (the window it chose: n less its reads then)
//	Hear    the sender got MAX_STREAM_DATA n
//	Read    the receiver read n bytes (Session.Read, the opener's answer, the accepter's preamble)
//
// A Read is logged after the stream counted it, an Update when its packet
// is packed: one may come before the other.
var aw = struct {
	sync.Mutex
	conns map[string]awConn    // a side's "<local port>-<remote port>": its connection
	wins  map[string][2]uint64 // "<odcid>-<c|s>": that end's windows
	files map[string]*os.File  // by name
}{conns: map[string]awConn{}, wins: map[string][2]uint64{}, files: map[string]*os.File{}}

type awConn struct{ id, role string }

// autoWinTracer is quic.Config.Tracer for a Node of window w0 up to maxW
// when LINKTRACE_AUTOWIN names a directory.
func autoWinTracer(w0, maxW uint64) func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	if os.Getenv("LINKTRACE_AUTOWIN") == "" {
		return nil
	}
	return func(_ context.Context, client bool, id quic.ConnectionID) qlogwriter.Trace {
		role := "s"
		if client {
			role = "c"
		}
		return &awTrace{id: hex.EncodeToString(id.Bytes()), role: role, w0: w0, max: maxW}
	}
}

type awTrace struct {
	id, role string
	w0, max  uint64
}

func (t *awTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *awTrace) SupportsSchemas(string) bool      { return true }
func (t *awTrace) Close() error                     { return nil }

func (t *awTrace) peer() string {
	if t.role == "c" {
		return "s"
	}
	return "c"
}

func (t *awTrace) RecordEvent(e qlogwriter.Event) {
	switch e := e.(type) {
	case qlog.StartedConnection:
		aw.Lock()
		aw.wins[t.id+"-"+t.role] = [2]uint64{t.w0, t.max}
		aw.conns[fmt.Sprintf("%d-%d", awPort(e.Local), awPort(e.Remote))] = awConn{t.id, t.role}
		aw.Unlock()
	case qlog.PacketSent:
		t.frames(e.Frames, true)
	case qlog.PacketReceived:
		t.frames(e.Frames, false)
	}
}

// frames logs a packet's stream frames and MAX_STREAM_DATA frames (out: sent).
func (t *awTrace) frames(fs []qlog.Frame, out bool) {
	for _, f := range fs {
		switch f := f.Frame.(type) {
		case *qlog.StreamFrame:
			if out {
				awLog(t.id, int64(f.StreamID), t.peer(), "Send", uint64(f.Offset+f.Length)) // #nosec G115 -- non-negative
			} else {
				awLog(t.id, int64(f.StreamID), t.role, "Data", uint64(f.Offset+f.Length)) // #nosec G115 -- non-negative
			}
		case *qlog.MaxStreamDataFrame:
			if out {
				awLog(t.id, int64(f.StreamID), t.role, "Update", uint64(f.MaximumStreamData))
			} else {
				awLog(t.id, int64(f.StreamID), t.peer(), "Hear", uint64(f.MaximumStreamData))
			}
		}
	}
}

func awPort(p qlog.PathEndpointInfo) uint16 {
	if p.IPv4.IsValid() {
		return p.IPv4.Port()
	}
	return p.IPv6.Port()
}

// traceAWRead logs n bytes read from st on q (the application's, the
// accepter's preamble).
func traceAWRead(q *quic.Conn, st *quic.Stream, n int) {
	if n <= 0 || os.Getenv("LINKTRACE_AUTOWIN") == "" {
		return
	}
	aw.Lock()
	c, ok := aw.conns[fmt.Sprintf("%d-%d", port(q.LocalAddr()), port(q.RemoteAddr()))]
	aw.Unlock()
	if ok {
		awLog(c.id, int64(st.StreamID()), c.role, "Read", uint64(n)) // #nosec G115 -- positive
	}
}

// awLog writes one line to the file of stream sid's direction toward
// recv; a file whose receiver is not in this process is not written.
func awLog(id string, sid int64, recv, ev string, n uint64) {
	aw.Lock()
	defer aw.Unlock()
	name := fmt.Sprintf("%s-%d-%s", id, sid, recv)
	f := aw.files[name]
	if f == nil {
		w, ok := aw.wins[id+"-"+recv]
		if !ok {
			return
		}
		var err error
		if f, err = os.Create(os.Getenv("LINKTRACE_AUTOWIN") + "/" + name + ".ndjson"); err != nil {
			panic(err)
		}
		aw.files[name] = f
		awWrite(f, map[string]any{"ev": "Up", "n": w[0], "max": w[1]})
	}
	awWrite(f, map[string]any{"ev": ev, "n": n})
}

func awWrite(f *os.File, m map[string]any) {
	line, _ := json.Marshal(m)
	_, _ = f.Write(append(line, '\n'))
}

// preambleLen: the bytes acceptStream read for a header h.
func preambleLen(chain bool, h []byte) int {
	if chain {
		return 17 + 2 + len(h)
	}
	return 2 + len(h)
}
