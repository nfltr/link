// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"errors"
	"fmt"
	"time"

	"github.com/nfltr/quic-go"
)

// AckPolicy is the ACK rate a Node asks each peer for with QUIC's ACK
// Frequency extension (draft-ietf-quic-ack-frequency-14, the AckFrequency field of
// the fork's quic.Config): the peer acknowledges after more than
// AckElicitingThreshold ack-eliciting packets, or MaxAckDelay after the
// oldest one not yet acknowledged, and at once only for a hole
// ReorderingThreshold packets deep, which is also this side's packet
// threshold of loss detection. Every ACK costs a packet at both ends:
// on a path that reorders a quarter of its packets the RFC 9000 rule (an
// immediate ACK for each) was a third of both ends' CPU per GiB. Model:
// spec/AckFrequency.tla. A peer without the extension is refused
// (CodeNoAckFrequency).
type AckPolicy struct {
	// AckElicitingThreshold: an ACK after more than this many packets
	// (0: every one); below ReorderingThreshold.
	AckElicitingThreshold int
	// ReorderingThreshold: how many places late a packet may arrive
	// before the peer acknowledges at once; at least 1.
	ReorderingThreshold int
	// MaxAckDelay: at most 25 ms; 0: a quarter of the min RTT.
	MaxAckDelay time.Duration
}

// DefaultAckPolicy: an ACK every 10 packets (Chromium's, and the fork's
// receiver-only decimation before: quic-go's CPU per MB of a bulk
// transfer on macOS 24-30 % lower than every second packet), none for
// reordering within 10 places (Helsinki -> Singapore delivered a quarter
// of its packets 1-9 places late), a quarter of the min RTT. The fork's
// TestBulkUploadOnReorderingPath: 0.39 -> 0.10 ACK packets per data
// packet on a path reordering 30 % of its packets, at the same rate.
var DefaultAckPolicy = AckPolicy{AckElicitingThreshold: 9, ReorderingThreshold: 10}

func (p AckPolicy) validate() error {
	if p.ReorderingThreshold < 1 || p.AckElicitingThreshold < 0 || p.AckElicitingThreshold >= p.ReorderingThreshold ||
		p.MaxAckDelay < 0 || p.MaxAckDelay > 25*time.Millisecond {
		return fmt.Errorf("quiclink: Config.Acks %+v: need 0 <= AckElicitingThreshold < ReorderingThreshold and MaxAckDelay in [0, 25ms]", p)
	}
	return nil
}

// ErrNoAckFrequency: the peer did not advertise QUIC's ACK Frequency
// extension (min_ack_delay): quiclink before v0.8.0. The connection is
// closed with CodeNoAckFrequency.
var ErrNoAckFrequency = errors.New("quiclink: the peer does not support QUIC ACK frequency (min_ack_delay): quiclink before v0.8.0")

// refuseWithoutAckFrequency closes q (CodeNoAckFrequency) when the peer
// did not advertise min_ack_delay: a hard cut, no RFC 9000 ACK fallback.
func refuseWithoutAckFrequency(q *quic.Conn) error {
	if q.ConnectionState().SupportsAckFrequency.Remote {
		return nil
	}
	_ = q.CloseWithError(CodeNoAckFrequency, "no ACK frequency (min_ack_delay)")
	return ErrNoAckFrequency
}
