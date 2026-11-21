// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
	quic "github.com/nfltr/quic-go"
	"github.com/nfltr/quic-go/quicvarint"
)

// ALPNEndAck is the application protocol of a connection whose sessions
// answer opens (ALPNChain's) and acknowledge their ends (since lane
// ENDACKQUIC): a session's CloseWriteSync returns nil only once the far
// end read every byte and the FIN, through any joins (peerlink.Join
// holds the end: HoldEndAck, AckEnd), as link's FeatureEndAck (WIRE.md
// 13). A listener's TLS without NextProtos offers it first; a dialler
// offers NextProtos{ALPNEndAck, ALPNChain, ALPN}. With an older peer the
// connection negotiates ALPNChain or ALPN and CloseWriteSync is
// CloseWrite (best effort, the contract before).
//
// On the wire the control stream (Drain's 'G' and 'D') carries, about
// the stream whose id (an RFC 9000 varint) follows: 'R' the receipt (the
// side read the peer's FIN after every byte), 'E' the end acknowledged
// (the far end holds it; a joint once its onward peer acknowledged it in
// turn), 'A' never acknowledged (the side failed or closed first); 'C'
// before an 'A' says the side closed cleanly (Close, not Reset): a wait
// of a stream that carried no byte reads it nil (lane QUICEND; an older
// peer skips the 'C', no id follows it). A wait reads only these: the
// stream's own RESET_STREAM, unordered with the control stream, may
// overtake an 'E' (spec/EndAckLeg.tla, Kind quic, mutant
// wait-on-stream-reset).
const ALPNEndAck = "peerlink-chain-endack"

const (
	ctlReceipt = 'R'
	ctlEndAck  = 'E'
	ctlAbort   = 'A'
	ctlClean   = 'C' // before an 'A': a clean Close's (emptyEnd)
)

// errClosedClean is a wait's end at the peer's 'C' 'A' (endErr maps it).
var errClosedClean = errors.New("quiclink: the peer closed the session cleanly")

const endAckIdle = 30 * time.Second // the idle bound (Node.endAckIdle in tests)

// endState is a session's end handshake (ALPNEndAck connections).
type endState struct {
	// receiving: the peer's end
	eof   atomic.Bool // Read returned io.EOF
	held  atomic.Bool // HoldEndAck: a joint acknowledges
	ackMu sync.Mutex
	acked bool // 'E' or 'A' sent

	// sending: this side's end
	rOnce, eOnce sync.Once
	rcv, end     chan struct{} // the peer's 'R'; its 'E' or 'A' (endErr)
	endErr       error
	grants       atomic.Int64 // MAX_STREAM_DATA seen: progress (SetSendWindowCallback)
	gOnce        sync.Once
	grantF       atomic.Pointer[func(int)] // OnGrant's
}

// endConn is a connection's sessions by stream id, for the control
// stream's 'R', 'E' and 'A' (ALPNEndAck).
type endConn struct {
	mu   sync.Mutex
	byID map[quic.StreamID]*Session
	wmu  sync.Mutex // one control message at a time
}

// track registers s (ALPNEndAck); untrack (ended) forgets it.
func (s *Session) track() {
	if !s.c.endack {
		return
	}
	s.ends.rcv, s.ends.end = make(chan struct{}), make(chan struct{})
	e := &s.c.ec
	e.mu.Lock()
	if e.byID == nil {
		e.byID = map[quic.StreamID]*Session{}
	}
	e.byID[s.stream.StreamID()] = s
	e.mu.Unlock()
}

func (s *Session) untrack() {
	if !s.c.endack {
		return
	}
	s.endDone(net.ErrClosed) // a wait in flight ends with the session
	e := &s.c.ec
	e.mu.Lock()
	delete(e.byID, s.stream.StreamID())
	e.mu.Unlock()
}

// endDone ends this side's wait once: nil at the peer's 'E'.
func (s *Session) endDone(err error) {
	s.ends.eOnce.Do(func() {
		s.ends.endErr = err
		close(s.ends.end)
	})
}

// readEOF: Read returned io.EOF, the peer's FIN after every byte: the
// receipt, and the acknowledgement unless a joint holds it.
func (s *Session) readEOF() {
	if !s.c.endack || s.ends.eof.Swap(true) {
		return
	}
	if s.ends.held.Load() {
		s.c.sendCtl(ctlReceipt, s.stream.StreamID())
		return
	}
	s.ends.ackMu.Lock()
	defer s.ends.ackMu.Unlock()
	sent := !s.ends.acked
	s.ends.acked = true
	if traceOn {
		traceEA(s, "REnd", "ack", map[bool]string{true: "1", false: "0"}[sent])
		if sent {
			traceHopRead(s) // spec/TraceHopHold.tla AppRead: before the 'E'
		}
	}
	if sent {
		s.c.sendCtl(ctlReceipt, s.stream.StreamID(), ctlEndAck)
	} else {
		s.c.sendCtl(ctlReceipt, s.stream.StreamID())
	}
}

// ackWith sends the end's final word ('E', 'A', or 'C' 'A') once; ev
// names it in a trace.
func (s *Session) ackWith(ev string, b byte, more ...byte) {
	s.ends.ackMu.Lock()
	defer s.ends.ackMu.Unlock()
	if s.ends.acked {
		return
	}
	s.ends.acked = true
	if traceOn && ev != "" && s.c.q.Context().Err() == nil { // on a closed connection it is no message
		traceEA(s, ev)
	}
	s.c.sendCtl(b, s.stream.StreamID(), more...)
}

// failEnd: the session failed (ev "RFail") or closed ("RClose") here
// before its peer's end was acknowledged: 'A', after 'C' when the close
// was clean (spec/EndAckLeg.tla RClose's "close").
func (s *Session) failEnd(ev string) {
	switch {
	case !s.c.endack:
	case ev == "RClose" && !s.reset.Load():
		s.ackWith(ev, ctlClean, ctlAbort)
	default:
		s.ackWith(ev, ctlAbort)
	}
}

// endErr is the wait's end: at the peer's clean close nil when this side
// wrote no byte (emptyEnd), else link.ErrReset.
func (s *Session) endErr() error {
	if s.ends.endErr != errClosedClean {
		return s.ends.endErr
	}
	if s.wrote.Load() {
		return link.ErrReset
	}
	return nil
}

// emptyEnd is a half close's err, nil when the stream's close failed
// only because the peer closed the session cleanly (peerClosed) and this
// side wrote no byte: nothing of it is lost, as a TCP close(2) in
// FIN_WAIT_2 still ACKs a bare FIN (spec/EndAckLeg.tla EmptyClosed,
// NoFalseReset: SClose after sfail).
func (s *Session) emptyEnd(err error) error {
	if err != nil && !s.wrote.Load() && s.peerClosed() {
		return nil
	}
	return err
}

// peerClosed reports whether the peer's clean Close stopped this side's
// writes: its STOP_SENDING CodeClosed (Close sends it, Reset and aborts
// another code).
func (s *Session) peerClosed() bool {
	var se *quic.StreamError
	return errors.As(context.Cause(s.stream.Context()), &se) && se.Remote && se.ErrorCode == CodeClosed
}

// peerStopped reports whether the peer stopped this side's writes (its
// STOP_SENDING: a Close, a Reset or an abort).
func (s *Session) peerStopped() bool {
	var se *quic.StreamError
	return errors.As(context.Cause(s.stream.Context()), &se) && se.Remote
}

// HoldEndAck makes the caller acknowledge the peer's end (AckEnd) once it
// passed it on: a join (peerlink.Join) acknowledges only what its onward
// peer acknowledged. Without ALPNEndAck it does nothing.
func (s *Session) HoldEndAck() { s.ends.held.Store(true) }

// AckEnd acknowledges the peer's end once this side read it (a join's,
// after its onward peer acknowledged it). Without ALPNEndAck, or before
// the peer's FIN was read, it sends nothing.
func (s *Session) AckEnd() {
	if s.c.endack && s.ends.eof.Load() {
		s.ackWith("", ctlEndAck) // a joint's: no step of EndAckLeg (a far end acknowledges at REnd)
	}
}

// AcksEnds reports whether the connection negotiated ALPNEndAck: whether
// CloseWriteSync waits for the far end.
func (s *Session) AcksEnds() bool { return s.c.endack }

// CloseWriteSync is CloseWrite, then the wait for the far end's
// acknowledgement (ALPNEndAck): nil once the far end read every byte and
// the FIN; link.ErrReset when it failed or closed first; link.ErrConnClosed
// (wrapping why) once the connection ended without the acknowledgement;
// link.ErrEndAckIdle, the session reset, once nothing of it was
// acknowledged (the peer's MAX_STREAM_DATA) for the idle bound (30 s).
// Without ALPNEndAck it is CloseWrite.
func (s *Session) CloseWriteSync() error { return s.CloseWriteSyncReceived(nil) }

// CloseWriteSyncReceived is CloseWriteSync that also reports the next
// hop's receipt ('R'): received runs once the peer read the FIN, before
// the far end's acknowledgement (link.Session.CloseWriteSyncReceived; a
// join ends its hold there, spec/HopHold.tla). Without ALPNEndAck it runs
// once the FIN was written.
func (s *Session) CloseWriteSyncReceived(received func()) error {
	if traceOn && s.c.endack && !s.reset.Load() {
		traceEA(s, "SClose", "w", map[bool]string{true: "1", false: "0"}[s.wrote.Load()]) // before the FIN: the peer may read it at once
	}
	if err := s.CloseWrite(); err != nil || !s.c.endack {
		if err == nil && received != nil {
			received()
		}
		if err != nil && s.c.endack && s.peerStopped() {
			return link.ErrReset // the far end closed or failed first: no FIN went out (TestEndAckCloseAfterPeerStopFails)
		}
		return err
	}
	return s.waitEnd(received)
}

// waitEnd waits for the peer's 'E' or 'A' (spec/EndAckLeg.tla SRecv),
// running received at its 'R'; it gives up at the connection's end and
// at the idle bound with no progress (SIdle: pings are none).
func (s *Session) waitEnd(received func()) (err error) {
	if traceOn {
		defer func() { traceEA(s, "SRet", "r", map[bool]string{true: "clean", false: "err"}[err == nil]) }()
	}
	s.watchGrants()
	t := time.NewTicker(s.c.n.endAckIdle)
	defer t.Stop()
	last := s.ends.grants.Load()
	rcv := s.ends.rcv
	for {
		select {
		case <-rcv:
			rcv = nil
			if received != nil {
				received()
			}
		case <-s.ends.end:
			return s.endErr()
		case <-s.c.q.Context().Done():
			select {
			case <-s.ends.end: // read before the connection's end (ErrTruth)
				return s.endErr()
			default:
			}
			if traceOn {
				traceEA(s, "Cut")
			}
			return fmt.Errorf("%w: %w", link.ErrConnClosed, context.Cause(s.c.q.Context()))
		case <-t.C:
			if g := s.ends.grants.Load(); g != last {
				last = g
				continue
			}
			select {
			case <-s.ends.end:
				return s.endErr()
			default:
			}
			if traceOn {
				traceEA(s, "SIdle")
			}
			_ = s.Reset()
			return link.ErrEndAckIdle
		}
	}
}

// watchGrants counts the peer's window updates (and calls OnGrant's f).
func (s *Session) watchGrants() {
	s.ends.gOnce.Do(func() {
		s.stream.SetSendWindowCallback(func(n int64) {
			s.ends.grants.Add(n)
			if f := s.ends.grantF.Load(); f != nil {
				(*f)(int(n))
			}
		})
	})
}

// OnGrant sets f to be called with each window update the peer sends for
// the session (QUIC's MAX_STREAM_DATA), as link's Session.OnGrant: n more
// bytes of what this side wrote left the peer's receive buffer (its reader
// took them). The peer sends one per at least a quarter of its window
// read, so f is how a writer sees its bytes still moving after its last
// Write returned. f runs on the connection's loop: it must not block. A
// later call replaces f; nil removes it.
func (s *Session) OnGrant(f func(n int)) {
	if f == nil {
		s.ends.grantF.Store(nil)
		return
	}
	s.ends.grantF.Store(&f)
	s.watchGrants()
}

// sendCtl writes a control message to the peer (opening the control
// stream): b's bytes, then, when b is 'R', 'E' or 'A', sid's (each
// such byte followed by it).
func (c *conn) sendCtl(b0 byte, sid quic.StreamID, more ...byte) {
	var p []byte
	for _, b := range append([]byte{b0}, more...) {
		switch b {
		case 0:
		case ctlReceipt, ctlEndAck, ctlAbort:
			p = quicvarint.Append(append(p, b), uint64(sid)) // #nosec G115 -- a stream id is under 2^62
		default:
			p = append(p, b)
		}
	}
	c.ec.wmu.Lock()
	defer c.ec.wmu.Unlock()
	c.n.mu.Lock()
	if c.ctl == nil {
		s, err := c.q.OpenUniStream()
		if err != nil {
			c.n.mu.Unlock()
			return
		}
		c.ctl = s
	}
	s := c.ctl
	c.n.mu.Unlock()
	_, _ = s.Write(p)
}

// send writes a control byte ('G', 'D') to the peer.
func (c *conn) send(b byte) { c.sendCtl(b, 0) }

// control reads the peer's control stream: GOAWAY, then DONE; on an
// ALPNEndAck connection also 'R', 'E' and 'A' (an older peer's control
// stream carries 'G' and 'D' only, and an older peer reads only them).
func (c *conn) control() {
	us, err := c.q.AcceptUniStream(context.Background())
	if err != nil {
		return
	}
	r := bufio.NewReader(us)
	clean := false // the last byte was 'C'
	for {
		b, err := r.ReadByte()
		if err != nil {
			return
		}
		after := clean
		clean = b == ctlClean
		switch b {
		case 'G':
			c.gone.Store(true)
			c.n.changedNow()
		case 'D':
			c.dOnce.Do(func() { close(c.done) })
		case ctlReceipt, ctlEndAck, ctlAbort:
			if !c.endack {
				continue
			}
			id, err := quicvarint.Read(r)
			if err != nil {
				return
			}
			c.ec.mu.Lock()
			s := c.ec.byID[quic.StreamID(id)] // #nosec G115 -- a varint is under 2^62
			c.ec.mu.Unlock()
			if s == nil {
				continue
			}
			if traceOn && b != ctlReceipt {
				traceEA(s, "SRecv", "t", map[byte]string{ctlEndAck: "eack", ctlAbort: map[bool]string{true: "close", false: "abort"}[after]}[b])
			}
			switch {
			case b == ctlReceipt:
				s.ends.rOnce.Do(func() { close(s.ends.rcv) })
			case b == ctlEndAck:
				s.endDone(nil)
			case after:
				s.endDone(errClosedClean)
			default:
				s.endDone(link.ErrReset)
			}
		}
	}
}
