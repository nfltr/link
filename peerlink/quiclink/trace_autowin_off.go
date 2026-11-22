// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package quiclink

import (
	"context"

	"github.com/nfltr/quic-go"
	"github.com/nfltr/quic-go/qlogwriter"
)

// autoWinTracer: the window trace's (trace_autowin_on.go); none here.
func autoWinTracer(uint64, uint64) func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	return nil
}

func traceAWRead(*quic.Conn, *quic.Stream, int) {}

func preambleLen(bool, []byte) int { return 0 }
