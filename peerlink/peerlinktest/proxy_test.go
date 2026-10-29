// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlinktest_test

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink/peerlinktest"
)

// TestProxyThrottleRate: a throttled Proxy relays at its rate, give or
// take (on Linux it once relayed a seventh of it: Throttle).
func TestProxyThrottleRate(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close() //nolint:errcheck
		buf := make([]byte, 32<<10)
		for {
			if _, err := c.Write(buf); err != nil {
				return
			}
		}
	}()
	p := peerlinktest.NewProxy(t, ln.Addr().String())
	const rate, size = 1 << 20, 2 << 20
	p.Throttle(rate)
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	start := time.Now()
	if _, err := io.CopyN(io.Discard, c, size); err != nil {
		t.Fatal(err)
	}
	got := float64(size) / time.Since(start).Seconds()
	t.Logf("%.0f KiB/s at a rate of %d KiB/s", got/1024, rate>>10)
	if got < 0.6*rate || got > 1.5*rate {
		t.Fatalf("relayed %.0f KiB/s, rate %d KiB/s", got/1024, rate>>10)
	}
}
