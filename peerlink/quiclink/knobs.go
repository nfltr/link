// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"errors"
	"fmt"

	"github.com/nfltr/link"
	"github.com/nfltr/quic-go"
)

// The Node's QUIC settings (quicConfig) and the speed knobs of Config
// beside Conns and Acks (README "Speed knobs").

// Congestion is a congestion controller.
type Congestion uint8

const (
	// CongestionBBR: BBR v1 (quic.CongestionControlBBR), the default.
	CongestionBBR Congestion = iota
	// CongestionReno: NewReno (RFC 9002 section 7).
	CongestionReno
)

// Socket is a UDP socket's settings (the fork's quic.Transport fields).
type Socket struct {
	// ReceiveBuffer and SendBuffer are the SO_RCVBUF and SO_SNDBUF sizes
	// asked for, at least (0: quic.SocketBufferWanted, 7 MiB). The host
	// may give less (Linux net.core.rmem_max, wmem_max): Node.SocketBuffers.
	ReceiveBuffer, SendBuffer int
	// NoGSO and NoGRO turn off UDP segmentation offload on send and
	// receive coalescing (Linux 5.0+, on where the kernel has them).
	NoGSO, NoGRO bool
	// NoECN neither marks sent packets ECT(0) nor reads received ones'
	// ECN bits (on by default: Linux 5.0+, darwin, FreeBSD).
	NoECN bool
}

func (c Congestion) quic() quic.CongestionControl {
	if c == CongestionReno {
		return quic.CongestionControlReno
	}
	return quic.CongestionControlBBR
}

// minPacketSize and maxPacketSize bound Config.MaxPacketSize: QUIC's
// smallest datagram (RFC 9000 section 14) and the fork's jumbo packet
// buffers (a 9000-byte MTU less the IPv6 and UDP headers);
// defaultPacketSize is 0's (an Ethernet MTU's).
const minPacketSize, defaultPacketSize, maxPacketSize = 1200, 1452, 8952

// SocketBuffers is the receive and send buffer sizes the host gave the
// socket of Config.PacketConn (quic.SocketBuffers): less than
// Config.Socket asked for (0: quic.SocketBufferWanted) where the host's
// limit is lower. Nothing logs a shortfall: a program compares and tells
// its user (Linux net.core.rmem_max, wmem_max).
func (n *Node) SocketBuffers() (read, write int, err error) {
	if n.cfg.PacketConn == nil {
		return 0, 0, errors.New("quiclink: SocketBuffers: no Config.PacketConn (a Transport's or Dial's socket is the caller's: quic.SocketBuffers)")
	}
	return quic.SocketBuffers(n.cfg.PacketConn)
}

// validateKnobs refuses a speed knob that cannot work, naming the field.
func (cfg Config) validateKnobs() error {
	switch {
	case cfg.Congestion > CongestionReno:
		return fmt.Errorf("quiclink: Config.Congestion %d: want CongestionBBR or CongestionReno", cfg.Congestion)
	case cfg.MaxPacketSize != 0 && (cfg.MaxPacketSize < minPacketSize || cfg.MaxPacketSize > maxPacketSize):
		return fmt.Errorf("quiclink: Config.MaxPacketSize %d: want 0 (%d) or %d to %d", cfg.MaxPacketSize, defaultPacketSize, minPacketSize, maxPacketSize)
	case cfg.Socket.ReceiveBuffer < 0 || cfg.Socket.SendBuffer < 0:
		return fmt.Errorf("quiclink: Config.Socket %+v: want buffer sizes of 0 (quic.SocketBufferWanted) or more", cfg.Socket)
	case cfg.Socket != (Socket{}) && cfg.PacketConn == nil:
		return errors.New("quiclink: Config.Socket needs Config.PacketConn: a Transport's or Dial's socket is the caller's (set the quic.Transport's fields)")
	}
	return nil
}

// quicConfig is a Node's QUIC settings: its liveness l, windows, session
// limit, ACK policy and speed knobs.
func quicConfig(cfg Config, l link.Liveness, acks AckPolicy) *quic.Config {
	window := uint64(max(cmpOr(cfg.Window, link.DefaultWindow), 1)) // #nosec G115 -- at least 1
	return &quic.Config{
		HandshakeIdleTimeout:           l.Interval + l.Timeout,
		MaxIdleTimeout:                 idleBackstop * (l.Interval + l.Timeout), // watch judges; this is its backstop
		KeepAlivePeriod:                l.Interval,
		InitialStreamReceiveWindow:     window,
		MaxStreamReceiveWindow:         max(window, uint64(max(cfg.MaxWindow, 0))), // #nosec G115 -- non-negative; New: 0 or at least Window
		InitialConnectionReceiveWindow: 16 * window,
		MaxConnectionReceiveWindow:     16 * window,
		MaxIncomingStreams:             int64(cmpOr(cfg.MaxSessions, link.DefaultMaxSessions)),
		MaxIncomingUniStreams:          1, // the control stream
		// A reset delivers what was written before it (RESET_STREAM_AT)
		// where both ends support it: an Abort after bytes, as link's.
		EnableStreamResetPartialDelivery: true,
		EnableDatagrams:                  cfg.Datagrams,
		// BBR, not NewReno: on a long path with random loss (0.05 % at
		// 260 ms RTT, Helsinki -> Singapore) NewReno's window falls to a
		// few packets and grows one per RTT: 3 Mbit/s of 300 (BBR: 190).
		// Guard: TestNodeUsesBBR; the gain: quic-go's
		// TestBulkUploadOnReorderingPath (docs/performance.md).
		CongestionControl: cfg.Congestion.quic(),
		DisablePacing:     cfg.NoPacing,
		MaxPacketSize:     uint16(cfg.MaxPacketSize), // #nosec G115 -- validated: at most maxPacketSize
		// #nosec G115 -- validate: non-negative
		AckFrequency: &quic.AckFrequency{AckElicitingThreshold: uint64(acks.AckElicitingThreshold),
			ReorderingThreshold: uint64(acks.ReorderingThreshold), MaxAckDelay: acks.MaxAckDelay},
	}
}
