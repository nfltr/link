// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
)

// The path layer's cost over a relayed link session (base: the session
// alone), and across moves. make profile-all / the lane's evidence:
// go test -run '^$' -bench . -benchmem -count 6 ./pathswitch/

type linkStream struct{ *link.Session }

func (s linkStream) Reset() error { return s.Abort(0, "reset") }

// linkPair returns a session's two ends over one link connection on a
// pipe (the relayed path), and the opener's Mux for more sessions.
func linkPair(b *testing.B) (opener *link.Mux, first, accepted *link.Session, more <-chan *link.Session) {
	pa, pb := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := make(chan *link.Conn, 1)
	go func() {
		c, _, err := link.Server(ctx, pb, link.Config{Auth: link.NoAuth})
		if err != nil {
			b.Error(err)
		}
		ch <- c
	}()
	dc, err := link.Client(ctx, pa, link.Config{Auth: link.NoAuth})
	if err != nil {
		b.Fatal(err)
	}
	lc := <-ch
	acc := make(chan *link.Session, 16)
	go link.NewMux(lc).Serve(func(_ []byte, s *link.Session) { acc <- s; <-s.Done() })
	m := link.NewMux(dc)
	go m.Serve(nil)
	b.Cleanup(func() { _ = m.Close() })
	s, err := m.Open(nil)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := s.Write([]byte{0}); err != nil { // the accept
		b.Fatal(err)
	}
	t := <-acc
	if _, err := io.ReadFull(t, make([]byte, 1)); err != nil {
		b.Fatal(err)
	}
	return m, s, t, acc
}

func pump(b *testing.B, w io.Writer, r io.Reader, total int64, mid func(sent int64)) {
	buf := make([]byte, 32<<10)
	done := make(chan error, 1)
	go func() {
		_, err := io.CopyN(io.Discard, r, total)
		done <- err
	}()
	var sent int64
	for sent < total {
		n, err := w.Write(buf[:min(int64(len(buf)), total-sent)])
		if err != nil {
			b.Fatal(err)
		}
		sent += int64(n)
		if mid != nil {
			mid(sent)
		}
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}

const benchBytes = 64 << 20

// BenchmarkRelayedLinkSession: the base, a link session alone.
func BenchmarkRelayedLinkSession(b *testing.B) {
	_, s, t, _ := linkPair(b)
	b.SetBytes(benchBytes)
	b.ResetTimer()
	for b.Loop() {
		pump(b, s, t, benchBytes, nil)
	}
}

// BenchmarkPathswitchRelayed: the same session framed by the path layer.
func BenchmarkPathswitchRelayed(b *testing.B) {
	_, s, t, _ := linkPair(b)
	c, sv := Client(linkStream{s}, "relay", Config{}), Server(linkStream{t}, "relay", Config{})
	b.Cleanup(func() { c.Abort(); sv.Abort() })
	b.SetBytes(benchBytes)
	b.ResetTimer()
	for b.Loop() {
		pump(b, c, sv, benchBytes, nil)
	}
}

// BenchmarkPathswitchMoves: the path layer moving to a new stream every
// 8 MiB (a window in flight resent each time).
func BenchmarkPathswitchMoves(b *testing.B) {
	m, s, t, more := linkPair(b)
	c, sv := Client(linkStream{s}, "relay", Config{}), Server(linkStream{t}, "relay", Config{})
	b.Cleanup(func() { c.Abort(); sv.Abort() })
	go func() {
		for st := range more {
			if o, err := ReadOpen(linkStream{st}); err == nil {
				_ = sv.Attach(o, "relay")
			}
		}
	}()
	tok := func(uint64) []byte { return nil }
	b.SetBytes(benchBytes)
	b.ResetTimer()
	for b.Loop() {
		next := int64(8 << 20)
		pump(b, c, sv, benchBytes, func(sent int64) {
			if sent < next {
				return
			}
			next += 8 << 20
			ns, err := m.Open(nil)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := c.Move(linkStream{ns}, "relay", tok); err != nil {
				b.Fatal(err)
			}
		})
	}
}
