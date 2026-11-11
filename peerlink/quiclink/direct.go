// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"

	"github.com/nfltr/quic-go"
)

// Direct paths (nfltr docs/decisions/direct-paths-2026-11-11.md, link
// spec/DirectPath.tla): a session moves from the relayed path to a QUIC
// connection straight between its two machines, hole punched through
// their NATs. Both ends use the one quic.Transport (one UDP socket) their
// agent link to the relay runs on, so the NAT mapping the relay saw (QAD)
// is the one the punch uses. The client (the side that dials) runs Punch;
// the agent sends Probe datagrams to the client's candidates at the same
// time, opening its NAT's filter, and accepts on a Node with
// Config.Transport. One Node per end keeps one direct connection per
// machine pair (a Node's connection per peer: Config.Dial runs Punch);
// each session's path stream is a Session on it (pathswitch.Stream).
// Who is at the address is the caller's check: tc's certificate
// verification (the organisation's, the name asked for) and the
// session's token on the stream (pathswitch's OPEN), since the relay
// chooses the addresses.

// MaxCandidates is the most addresses Punch dials at once.
const MaxCandidates = 8

// Punch dials every candidate on tr at once (QUIC retransmits its Initial
// while the peer's Probe opens its NAT) and returns the first connection
// whose handshake passes tc's verification; it closes the others. ctx
// bounds it (the design's 5 s per punch).
func Punch(ctx context.Context, tr *quic.Transport, cands []netip.AddrPort, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
	if len(cands) == 0 || len(cands) > MaxCandidates {
		return nil, fmt.Errorf("quiclink: punch: %d candidates (1 to %d)", len(cands), MaxCandidates)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		q   *quic.Conn
		err error
	}
	ch := make(chan result, len(cands))
	for _, a := range cands {
		go func() {
			q, err := tr.Dial(ctx, net.UDPAddrFromAddrPort(a), withALPN(tc), qc)
			ch <- result{q, err}
		}()
	}
	var errs []error
	var won *quic.Conn
	for range cands {
		r := <-ch
		switch {
		case r.err != nil:
			errs = append(errs, r.err)
		case won == nil:
			won = r.q
			cancel() // the dials still running give up
		default:
			_ = r.q.CloseWithError(CodeDone, "another candidate won")
		}
	}
	if won == nil {
		return nil, fmt.Errorf("quiclink: punch: no candidate answered: %w", errors.Join(errs...))
	}
	return won, nil
}

// Probe sends one datagram from tr to each candidate: the answering
// end's half of a punch, opening its NAT's filter toward the client's
// mappings. The datagram is one zero byte, which a QUIC endpoint drops
// (too short for any packet).
func Probe(tr *quic.Transport, cands []netip.AddrPort) error {
	if len(cands) > MaxCandidates {
		return fmt.Errorf("quiclink: probe: %d candidates (at most %d)", len(cands), MaxCandidates)
	}
	var errs []error
	for _, a := range cands {
		if _, err := tr.WriteTo([]byte{0}, net.UDPAddrFromAddrPort(a)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
