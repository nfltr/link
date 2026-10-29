// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"io"
	"runtime"
	"runtime/debug"
	"testing"
)

// WriteTo to a TCP conn (writev: net.Buffers clears the slices it wrote)
// still returns the received buffers to the pool: moving 16 MiB allocates
// a few windows' worth, not the bytes moved.
func TestWriteToRecyclesReceivedBuffers(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items at random under -race")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(-1)) // the pool keeps what it is given
	s, p := openPair(t, v2cfg, v2cfg)
	x, y := tcpPair(t)
	go func() { _, _ = io.Copy(io.Discard, y) }()
	const total = 16 << 20
	chunk := make([]byte, MaxChunk)
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	go func() {
		for range total / MaxChunk {
			if _, err := p.Write(chunk); err != nil {
				t.Errorf("Write: %v", err)
				return
			}
		}
		_ = p.CloseWrite()
	}()
	if n, err := s.WriteTo(x); err != nil || n != total {
		t.Fatalf("WriteTo: %d, %v", n, err)
	}
	runtime.ReadMemStats(&ms1)
	if got := ms1.TotalAlloc - ms0.TotalAlloc; got > total/4 {
		t.Fatalf("moving %d MiB allocated %.1f MiB: received buffers are not reused", total>>20, float64(got)/(1<<20))
	}
}
