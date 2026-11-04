// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"context"
	"errors"
	"testing"
	"time"

	quic "github.com/nfltr/quic-go"
)

// TestIdleTimeoutIsBothSides: the hypothesis for lane RCFOLLOW's two
// short-stall failures (spec/README.md "quiclink's watch", QW3) was that
// B's QUIC idle timeout (3 s, LivenessFast's Timeout before lane
// LIVESTARVE) closed B silently while A, at 10 s, heard nothing until its
// own watch. QUIC's idle timeout is the smaller of the two endpoints'
// (RFC 9000 section 10.1): A's is 3 s too, so A ends by its own idle timer
// (5.2 s measured: the timer runs from its last ack-eliciting packet), never by a 10 s watch. Raw
// quic-go with the old settings (no keep-alive: B silent at once).
func TestIdleTimeoutIsBothSides(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pc := listenUDP(t)
	stls := serverTLS("b")
	stls.NextProtos = []string{"idle-test"}
	ln, err := (&quic.Transport{Conn: pc}).Listen(stls, &quic.Config{MaxIdleTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan *quic.Conn, 1)
	go func() {
		if c, err := ln.Accept(ctx); err == nil {
			accepted <- c
		}
	}()
	a, err := quic.DialAddr(ctx, pc.LocalAddr().String(), clientTLS("a", "b", "idle-test"), &quic.Config{MaxIdleTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	b := <-accepted
	start := time.Now()
	<-a.Context().Done()
	took := time.Since(start)
	var idle *quic.IdleTimeoutError
	if !errors.As(context.Cause(a.Context()), &idle) || took >= 9*time.Second {
		t.Fatalf("A ended after %v by %v, want its own idle timeout, well before its 10 s", took, context.Cause(a.Context()))
	}
	<-b.Context().Done()
	t.Logf("A ended after %v: %v", took, context.Cause(a.Context()))
}
