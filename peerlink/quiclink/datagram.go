// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/nfltr/quic-go"
)

// Datagrams of a session (nfltr docs/decisions/nf1-udp-2026-11-14.md "The
// direct path"): with Config.Datagrams at both ends a session carries QUIC
// DATAGRAM frames (RFC 9221) beside its stream, lossy and unordered. A
// frame's payload is the session's tag, its QUIC stream id (a uvarint:
// unique on the connection, never reused by QUIC, known to both ends),
// then the caller's bytes. A frame is handed only to the session of its
// tag on that connection while that session has a receiver (OnDatagram)
// and has not ended (Close, Reset); any other is dropped and counted
// (DatagramsDropped). Whether the two ends offer datagrams is QUIC's own
// negotiation (max_datagram_frame_size): toward a peer that does not,
// SendDatagram fails and the caller carries the bytes on the stream.

// ErrNoDatagrams: the connection carries no datagrams (an end did not
// offer them) or the session ended.
var ErrNoDatagrams = errors.New("quiclink: no datagrams on this session")

// dgrams is a connection's datagram receivers, by tag.
type dgrams struct {
	mu   sync.Mutex
	on   map[quic.StreamID]func([]byte)
	once sync.Once
}

// SendDatagram sends p as one DATAGRAM frame of the session. It waits
// while quic-go's send queue is full (32 frames): call it from a sender of
// its own, never from a reader that must not wait. Errors:
// ErrNoDatagrams; *quic.DatagramTooLargeError when p does not fit in one
// packet now (its MaxDatagramPayloadSize counts the tag: the caller's
// largest p is MaxDatagramPayloadSize minus TagLen); the connection's
// error once it closed.
func (s *Session) SendDatagram(p []byte) error {
	if s.reset.Load() || !s.c.datagrams() {
		return ErrNoDatagrams
	}
	b := binary.AppendUvarint(make([]byte, 0, binary.MaxVarintLen64+len(p)), uint64(s.StreamID())) // #nosec G115 -- a stream id is at most 2^62
	err := s.c.q.SendDatagram(append(b, p...))
	if e, ok := errors.AsType[*quic.DatagramTooLargeError](err); ok {
		return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: e.MaxDatagramPayloadSize - int64(s.TagLen())}
	}
	return err
}

// TagLen is the bytes the session's tag takes of a frame's payload.
func (s *Session) TagLen() int {
	return len(binary.AppendUvarint(nil, uint64(s.StreamID()))) // #nosec G115 -- a stream id is at most 2^62
}

// OnDatagram sets f to take each datagram the peer sent on the session
// (its SendDatagram's p, valid only during the call) until the session
// ends; nil removes it. f runs on the connection's datagram reader, which
// serves every session of the connection: it must not wait. It is a no-op
// on a connection that carries no datagrams.
func (s *Session) OnDatagram(f func(p []byte)) {
	c := s.c
	if !c.datagrams() {
		return
	}
	id := s.StreamID()
	c.dg.mu.Lock()
	if f == nil {
		delete(c.dg.on, id)
	} else if !s.over.Load() {
		if c.dg.on == nil {
			c.dg.on = map[quic.StreamID]func([]byte){}
		}
		c.dg.on[id] = f
	}
	c.dg.mu.Unlock()
	if f != nil {
		c.dg.once.Do(func() { go c.readDatagrams() }) // #nosec G118 -- it ends with the connection
	}
}

// datagrams: both ends offered DATAGRAM frames on c.
func (c *conn) datagrams() bool {
	d := c.q.ConnectionState().SupportsDatagrams
	return d.Local && d.Remote
}

// readDatagrams hands each frame to its session's receiver until the
// connection closes; it never waits on one.
func (c *conn) readDatagrams() {
	for {
		b, err := c.q.ReceiveDatagram(context.Background())
		if err != nil {
			return
		}
		tag, n := binary.Uvarint(b)
		c.dg.mu.Lock()
		f := c.dg.on[quic.StreamID(tag)] // #nosec G115 -- a map lookup: an unknown tag finds none
		c.dg.mu.Unlock()
		if n <= 0 || f == nil {
			c.n.dgDropped.Add(1)
			continue
		}
		f(b[n:])
	}
}

// forget removes the session's receiver (its end).
func (c *conn) forgetDatagrams(id quic.StreamID) {
	c.dg.mu.Lock()
	delete(c.dg.on, id)
	c.dg.mu.Unlock()
}

// DatagramsDropped counts the DATAGRAM frames this Node's connections
// dropped: malformed, or for no session with a receiver (an ended
// session's, an older stream's).
func (n *Node) DatagramsDropped() uint64 { return n.dgDropped.Load() }
