// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"testing"

	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/nfltr/quic-go"
)

// TestNodeUsesBBR: a Node's connections use BBR, not NewReno, whose window
// a long path's random loss holds at a few packets (quic-go's
// TestBulkUploadOnReorderingPath: 3 Mbit/s of 300 at 260 ms RTT, 0.05 %
// loss; BBR 190).
func TestNodeUsesBBR(t *testing.T) {
	n, err := quiclink.New(quiclink.Config{Dial: func(context.Context, string, *quic.Config) (*quic.Conn, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if got := quiclink.CongestionControl(n); got != quic.CongestionControlBBR {
		t.Fatalf("CongestionControl = %d, want BBR (%d)", got, quic.CongestionControlBBR)
	}
}
