// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/nfltr/quic-go"
)

// TestMaxWindowAutoTunes: a session's stream window starts at Window and
// may grow to MaxWindow (quic-go's auto-tuning; 0: Window, fixed), the
// connection's stays 16 Windows (lane LONGFAT: one session on a 260 ms
// path needs more than its first 16 MiB, a connection's memory bound does
// not move).
func TestMaxWindowAutoTunes(t *testing.T) {
	dial := func(context.Context, string, *quic.Config) (*quic.Conn, error) { return nil, nil }
	const mib = 1 << 20
	for _, tc := range []struct {
		cfg              quiclink.Config
		initial, max, cn uint64
	}{
		{quiclink.Config{Dial: dial}, mib, mib, 16 * mib},
		{quiclink.Config{Dial: dial, Window: 16 * mib}, 16 * mib, 16 * mib, 256 * mib},
		{quiclink.Config{Dial: dial, Window: 16 * mib, MaxWindow: 64 * mib}, 16 * mib, 64 * mib, 256 * mib},
		{quiclink.Config{Dial: dial, MaxWindow: 4 * mib}, mib, 4 * mib, 16 * mib},
	} {
		qc := mustNew(t, tc.cfg).QUICConfig()
		if qc.InitialStreamReceiveWindow != tc.initial || qc.MaxStreamReceiveWindow != tc.max ||
			qc.InitialConnectionReceiveWindow != tc.cn || qc.MaxConnectionReceiveWindow != tc.cn {
			t.Errorf("Window %d MaxWindow %d: stream %d up to %d, connection %d up to %d; want %d up to %d, %d", tc.cfg.Window, tc.cfg.MaxWindow,
				qc.InitialStreamReceiveWindow, qc.MaxStreamReceiveWindow, qc.InitialConnectionReceiveWindow, qc.MaxConnectionReceiveWindow, tc.initial, tc.max, tc.cn)
		}
	}
	for _, cfg := range []quiclink.Config{{Dial: dial, Window: 16 * mib, MaxWindow: 8 * mib}, {Dial: dial, MaxWindow: -1}} {
		if _, err := quiclink.New(cfg); err == nil || !strings.Contains(err.Error(), "Config.MaxWindow") {
			t.Errorf("Window %d MaxWindow %d: New %v, want an error naming Config.MaxWindow", cfg.Window, cfg.MaxWindow, err)
		}
	}
}
