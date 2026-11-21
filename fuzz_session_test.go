// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

// FuzzSessionAPI drives one session's API from several goroutines at once,
// shaped by the input, against a reference model (the bytes the peer sent,
// the end it chose), lane SESSIONAPI: readers (Read, with a read deadline
// set and cleared meanwhile, or WriteTo) take each byte once and in order,
// all of them, then each reads the peer's end, always the same; writers'
// bytes reach the peer once, in order, all of them.
//
// data[0]: bits 0-1 readers (1-3), bits 2-4 each reader's kind (WriteTo
// when set), bit 5 the peer resets instead of closing its writes, bit 6 a
// read deadline toggled; data[1]: bits 0-1 local writers (0-2); the rest:
// the sizes of the peer's writes (1-16 bytes each, 255 bytes at most) and
// of the local writers'.
func FuzzSessionAPI(f *testing.F) {
	f.Add([]byte{0x01, 0x00, 3, 7, 1})
	f.Add([]byte{0x0a, 0x02, 15, 15, 15, 2, 9})
	f.Add([]byte{0x63, 0x01, 1, 1, 1, 1, 200})
	f.Add([]byte{0x27, 0x02, 0, 5, 11, 4})
	open := sessionOpener(f) // one connection: one per input ran out of ports
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 3 {
			return
		}
		s, p := open(t)
		defer func() { _ = s.Close(); _ = p.Close() }()
		cfg, wcfg, sizes := data[0], data[1], data[2:]
		size := func(i int) int { return 1 + int(sizes[i%len(sizes)]%16) }
		nr, reset, toggle := 1+int(cfg&3)%3, cfg&0x20 != 0, cfg&0x40 != 0
		nw := int(wcfg&3) % 3
		var sent []byte // the model: bytes 1..len(sent), in order
		for i := 0; len(sent) < 255 && i < len(sizes); i++ {
			for range min(size(i), 255-len(sent)) {
				sent = append(sent, byte(len(sent)+1))
			}
		}
		go func() {
			for i, off := 0, 0; off < len(sent); i++ {
				k := min(size(i), len(sent)-off)
				if _, err := p.Write(sent[off : off+k]); err != nil {
					t.Errorf("peer Write: %v", err)
					return
				}
				off += k
			}
			if reset {
				_ = p.Reset()
			} else {
				_ = p.CloseWrite()
			}
		}()
		peerGot := make(chan []byte, 1)
		go func() { b, _ := io.ReadAll(p); peerGot <- b }()

		stop := make(chan struct{})
		var tg sync.WaitGroup
		if toggle {
			tg.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if i%2 == 0 {
						_ = s.SetReadDeadline(time.Now().Add(-time.Second))
					} else {
						_ = s.SetReadDeadline(time.Time{})
					}
					time.Sleep(50 * time.Microsecond)
				}
			})
		}
		got := make([][]byte, nr)
		ends := make([]error, nr)
		var wg sync.WaitGroup
		for r := range nr {
			wg.Go(func() {
				if cfg&(4<<r) != 0 {
					var c collector
					_, ends[r] = s.WriteTo(&c)
					got[r] = c.b
					return
				}
				b := make([]byte, 1+r*3)
				for {
					n, err := s.Read(b)
					got[r] = append(got[r], b[:n]...)
					if errors.Is(err, os.ErrDeadlineExceeded) {
						continue
					}
					if err != nil {
						ends[r] = err
						return
					}
				}
			})
		}
		sentW := make([]int, nw)
		for w := range nw {
			wg.Go(func() {
				for i, off := 0, 0; off < pairBytes; i++ {
					k := min(size(i+w), pairBytes-off)
					b := make([]byte, k)
					for j := range b {
						b[j] = pairByte(w, off+j)
					}
					n, err := s.Write(b)
					sentW[w] += n
					if err != nil {
						if !reset {
							t.Errorf("writer %d: %v", w, err)
						}
						return
					}
					off += k
				}
			})
		}
		if !waitFor(&wg, 20*time.Second) {
			t.Fatal("a call did not return")
		}
		close(stop)
		tg.Wait()
		_ = s.SetReadDeadline(time.Time{}) // the toggle may have left it past
		_ = s.CloseWrite()

		// readers: each byte once, in order, all of them; one end, the peer's
		var seen [256]int
		total := 0
		for r := range nr {
			for j, b := range got[r] {
				if seen[b]++; b == 0 || int(b) > len(sent) || seen[b] > 1 || (j > 0 && b <= got[r][j-1]) {
					t.Fatalf("reader %d got %v: a byte twice, out of order or never sent", r, got[r])
				}
			}
			total += len(got[r])
			wt := cfg&(4<<r) != 0
			switch {
			case reset && !errors.Is(ends[r], ErrReset):
				t.Fatalf("reader %d ended %v; want ErrReset", r, ends[r])
			case !reset && ends[r] != io.EOF && !(wt && ends[r] == nil):
				t.Fatalf("reader %d ended %v; want io.EOF", r, ends[r])
			}
		}
		if total != len(sent) {
			t.Fatalf("readers got %d bytes, the peer sent %d", total, len(sent))
		}
		if n, err := s.Read(make([]byte, 1)); n != 0 || (reset != errors.Is(err, ErrReset)) || (!reset && err != io.EOF) {
			t.Fatalf("Read after the end: (%d, %v)", n, err)
		}
		// writers: once, in order, all of them (unless the peer reset)
		var b []byte
		select {
		case b = <-peerGot:
		case <-time.After(20 * time.Second):
			t.Fatal("the peer's reads did not end")
		}
		next := make([]int, 2)
		for _, c := range b {
			w, i := int(c>>7), int(c&127)
			if w >= nw || i != next[w] {
				t.Fatalf("the peer read %v: a byte twice, out of order or never written", b)
			}
			next[w]++
		}
		for w := range nw {
			if next[w] > sentW[w] || (!reset && next[w] != pairBytes) {
				t.Fatalf("writer %d sent %d, the peer read %d", w, sentW[w], next[w])
			}
		}
	})
}
