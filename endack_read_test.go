// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// TestEndAckWaitsForTheReader (spec/EndAckLeg.tla ReadTruth, spec/RelayedClose.tla
// ReadTruth, mutant ack-on-receipt; lane ENDACK): a far end's Mux holding
// every byte and the FIN is no acknowledgement: CloseWriteSync returns nil
// only once the far end's reader took the end (io.EOF), or its program
// closed the session after reading every byte; its program's reset after
// the whole stream arrived fails the Close (pipe
// TestReceiverStopsAfterArrival, docs/reliability.md defect 5).
func TestEndAckWaitsForTheReader(t *testing.T) {
	tail := bytes.Repeat([]byte("t"), 4096)
	for _, tc := range []struct {
		name string
		end  func(*Session) error // the far end's program once its Mux holds the stream
		want error                // nil: CloseWriteSync clean
	}{
		{"read to EOF", func(s *Session) error { _, err := io.ReadAll(s); return err }, nil},
		{"read every byte, then Close", func(s *Session) error {
			if _, err := io.ReadFull(s, make([]byte, len(tail))); err != nil {
				return err
			}
			return s.Close()
		}, nil},
		{"reset after arrival", func(s *Session) error { return s.Reset() }, ErrReset},
		{"read a byte, then Close", func(s *Session) error {
			if _, err := s.Read(make([]byte, 1)); err != nil {
				return err
			}
			return s.Close()
		}, ErrReset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accepted := make(chan *Session, 1)
			m := muxPair(t, func(_ []byte, s *Session) { accepted <- s })
			o, err := m.Open(context.Background(), []byte("x"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := o.Write(tail); err != nil {
				t.Fatal(err)
			}
			ret := make(chan error, 1)
			go func() { ret <- o.CloseWriteSync() }()
			s := <-accepted
			waitUntil(t, func() bool { return s.peerHalf.Load() }) // its Mux holds every byte and the FIN
			select {
			case err := <-ret:
				t.Fatalf("CloseWriteSync returned %v before the far end's program took the end", err)
			case <-time.After(200 * time.Millisecond):
			}
			if err := tc.end(s); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-ret:
				if tc.want == nil && err != nil || tc.want != nil && !errors.Is(err, tc.want) {
					t.Fatalf("CloseWriteSync: %v, want %v", err, tc.want)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("CloseWriteSync never returned")
			}
		})
	}
}

// TestJoinPassesTheEndAckBeforeTheClose (lane ENDACK; spec/RelayedClose.tla
// JAck: the join passes the far end's frames on in order): a far end that
// reads to EOF and closes at once sends its ENDACK and its CLOSE back to
// back. The hop's two directions raced: one passed the ENDACK on after its
// wait, the other closed the opener's session whole at the CLOSE, which
// often came first, so the opener's CloseWriteSync read a reset.
func TestJoinPassesTheEndAckBeforeTheClose(t *testing.T) {
	r := newChainRig(t, rigOpts{n: 2}, func(_ []byte, s *Session) {
		_ = s.Accept()
		_, _ = io.Copy(io.Discard, s)
		_ = s.Close()
	})
	for i := range 200 {
		s := r.open("end")
		if _, err := s.Write([]byte("tail")); err != nil {
			t.Fatal(err)
		}
		if err := s.CloseWriteSync(); err != nil {
			t.Fatalf("run %d: CloseWriteSync: %v, want nil (the far end read the end)", i, err)
		}
		_ = s.Close()
	}
}
