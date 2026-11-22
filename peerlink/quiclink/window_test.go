// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
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

// TestMaxWindowOnALongPath: 4 MiB in order over a 10 ms path with a
// window of 128 KiB (MinWindow) that may grow to 1 MiB (quic-go's auto-tuning, the
// reader keeping up). make spec-traces runs it with -tags linktrace: both
// ends' window steps (trace_autowin_on.go), checked by
// spec/TraceAutoWindow.tla.
func TestMaxWindowOnALongPath(t *testing.T) {
	if testing.Short() {
		t.Skip("moves data over a delayed path")
	}
	pc := listenUDP(t)
	addr := reorderingPath(t, pc.LocalAddr().String(), 5*time.Millisecond, 0, 0, 0)
	cfg := func(c quiclink.Config) quiclink.Config { c.Window, c.MaxWindow = 128<<10, 1<<20; return c }
	b := mustNew(t, cfg(quiclink.Config{PacketConn: pc, TLS: serverTLS("b")}))
	a := mustNew(t, cfg(quiclink.Config{Dial: dialer("a", "b", addr), Connect: []string{"b"}}))
	const size = 4 << 20
	done := make(chan error, 1)
	go func() {
		_ = a.Serve(func(_ string, _ []byte, s peerlink.Session) {
			buf := make([]byte, 32<<10)
			var n int
			for {
				k, err := s.Read(buf)
				for i := range k {
					if buf[i] != byte(n+i) {
						done <- errors.New("a byte out of order")
						return
					}
				}
				n += k
				if err != nil {
					if n != size {
						err = fmt.Errorf("read %d bytes of %d: %w", n, size, err)
					} else {
						err = nil
					}
					done <- err
					return
				}
			}
		})
	}()
	go func() { _ = b.Serve(func(string, []byte, peerlink.Session) {}) }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := b.Open(ctx, "a", []byte("window"))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i)
	}
	if _, err := s.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_ = s.Close()
	dctx, dcancel := context.WithCancel(context.Background())
	dcancel()
	_ = a.Drain(dctx)
	_ = b.Drain(dctx)
}
