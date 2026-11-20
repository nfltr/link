// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"errors"
	"time"
)

// featureEndAck is feature bit 2 (WIRE.md sections 6 and 13): the side
// reads ENDACK frames and sends one when its reader takes the peer's half
// close, so the peer's CloseWriteSync returns once the far end has the
// session's whole stream, through any joins (spec/RelayedClose.tla).
const featureEndAck uint32 = 1 << 2

// ackWait is a half close's wait for the peer's ENDACK.
type ackWait struct {
	done chan struct{}
	err  error
}

// expectAck registers sid's wait for its ENDACK (m.mu not held), before
// the CLOSE is buffered, so the answer is never missed.
func (m *Mux) expectAck(sid string) *ackWait {
	w := &ackWait{done: make(chan struct{})}
	m.mu.Lock()
	if m.acks == nil {
		m.acks = map[string]*ackWait{}
	}
	m.acks[sid] = w
	m.mu.Unlock()
	return w
}

// ackDone ends sid's wait: nil at its ENDACK, else why it never comes.
func (m *Mux) ackDone(sid string, err error) {
	m.mu.Lock()
	w := m.acks[sid]
	delete(m.acks, sid)
	leaving := m.leaving
	m.mu.Unlock()
	if w == nil {
		return
	}
	w.err = err
	close(w.done)
	if leaving {
		m.leave()
	}
}

const endAckIdle = 30 * time.Second // the idle bound (Config.endAckIdle in tests)

// waitAck waits for w: nil at the ENDACK, even when the connection ended
// since; else the session's or the connection's end, or ErrEndAckIdle
// once nothing of the session was acknowledged for endAckIdle (checked
// every endAckIdle: it gives up within twice that after the last
// progress; a mute
// far end, or a hop's onward one: pings keep the connection up, they are
// no progress; spec/RelayedClose.tla AIdle, mutant
// liveness-counts-as-progress). It resets the session then.
func (s *Session) waitAck(w *ackWait) (err error) {
	if traceOn { // spec/TraceRelayedClose.tla ARet: CloseWriteSync's result
		defer func() { traceRCRet(s, err) }()
	}
	c := s.m.c
	t := time.NewTicker(c.endAckIdle)
	defer t.Stop()
	last := c.credit.progress(s.id)
	for {
		select {
		case <-w.done:
			return w.err
		case <-c.Done():
			select {
			case <-w.done:
				return w.err
			default:
			}
			traceRC(s.m, s.id, "A", "ADown")
			return syncEnded(c)
		case <-t.C:
			if p := c.credit.progress(s.id); p != last {
				last = p
				continue
			}
			select {
			case <-w.done:
				return w.err
			default:
			}
			traceRC(s.m, s.id, "A", "AIdle")
			traceRC(s.m, s.id, "Jout", "JIdle")
			s.m.ackDone(s.id, ErrEndAckIdle) // a leaving connection awaits it no more
			_ = s.Reset()
			return ErrEndAckIdle
		}
	}
}

// ErrEndAckIdle is CloseWriteSync's error when the far end acknowledged
// nothing of the session, its end included, for the idle bound (30 s).
var ErrEndAckIdle = errors.New("link: the far end acknowledged nothing of the session within the idle bound")

// HoldEndAck makes the caller send the session's ENDACK (AckEnd) once it
// passed the peer's end on: a join (peerlink.Join) on a hop (Config.Hop)
// acknowledges only what its onward peer acknowledged.
func (s *Session) HoldEndAck() { s.heldAck.Store(true) }

// AckEnd sends the session's ENDACK once: the peer's half close was
// received (a far end), passed on (a join's, HoldEndAck), or taken by
// this side's reader. Without FeatureEndAck, or before the peer's half
// close, it sends nothing.
func (s *Session) AckEnd() {
	if s.m.c.muteEndAck {
		return
	}
	if !s.peerHalf.Load() || s.err.Load() != nil || !s.m.c.endAcks() {
		return
	}
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	if s.acked {
		return
	}
	s.acked = true
	if s.m.c.sendEndAck(s.id) == nil {
		_ = s.m.c.flush()
	}
}

// readEnd: a reader took the peer's half close (a clean io.EOF): on a
// hop, the end of a session no join holds is acknowledged here.
func (s *Session) readEnd() {
	if !s.heldAck.Load() {
		s.AckEnd()
	}
}

// ackAtClose: a Close after the reader took every byte but not yet the
// end acknowledges it (a reader that knows the stream's length).
func (s *Session) ackAtClose() {
	if !s.m.c.hop { // a far end: acknowledged on receipt (this keeps the ENDACK before the close)
		s.AckEnd()
		return
	}
	if s.heldAck.Load() || !s.q.Empty() {
		return
	}
	if !s.rmu.TryLock() { // a Read runs: it takes the end itself
		return
	}
	rest := len(s.rest)
	s.rmu.Unlock()
	if rest == 0 {
		s.AckEnd()
	}
}

// closeAckErr is how the peer's whole close ends this side's wait, its
// ENDACK not read first: never nil. A far end acknowledges an end it holds
// before its close (AckEnd on receipt, ackAtClose), so a close without one
// came before this side's FIN reached it: the end is not known delivered,
// ErrReset (as a TCP close with data unread is an RST; quiclink's 'A');
// an abort its own error (spec/EndAckLeg.tla RClose, spec/RelayedClose.tla
// CClose, mutant close-acks: a clean close read as the ENDACK).
func closeAckErr(f rxFrame) error {
	if f.Abort != nil {
		return f.Abort
	}
	return ErrReset
}

// closedFirst is CloseWriteSync's error once the peer closed the session
// whole before this side's FIN: the end is not known delivered.
func (s *Session) closedFirst() error {
	if e := s.err.Load(); e != nil {
		return *e
	}
	return ErrReset
}

// CloseWriteSyncReceived is CloseWriteSync that also reports the next
// hop's receipt: received runs once that hop's Mux read the CLOSE (the
// pong of a ping buffered with it), before the far end's ENDACK, which it
// then waits for with FeatureEndAck. A join ends its hold at received,
// never at the ENDACK: two joins whose FINs cross each hold the end the
// other's ENDACK waits for (spec/HopHold.tla, mutant hold-until-ack).
// received does not run when the connection ended first, or once
// CloseWrite was called before (it is CloseWriteSync then).
func (s *Session) CloseWriteSyncReceived(received func()) error {
	synced, err := s.closeWrite(true)
	switch {
	case err != nil:
		return err
	case synced == nil:
		return s.CloseWriteSync()
	}
	err = waitSync(s.m.c, synced)
	if err == nil {
		received()
	}
	if w := s.ack.Load(); w != nil {
		return s.waitAck(w)
	}
	return err
}

// onwardTook waits until the peer took this side's half close: its
// ENDACK, or without FeatureEndAck the pong of a ping after the close.
func (s *Session) onwardTook() error {
	if w := s.ack.Load(); w != nil {
		return s.waitAck(w)
	}
	return s.sync()
}

// nack tells a peer still waiting for this session's ENDACK, once the
// session ended here (both half closes passed), that it never comes: a
// reset of the sid (its wait ends with ErrReset). True when it sent one.
func (s *Session) nack() bool {
	if !closed(s.done) || !s.peerHalf.Load() || s.peerClosed.Load() && !s.finSent.Load() || !s.m.c.endAcks() {
		return false
	}
	return s.sendNack()
}

// sendNack sends the reset that ends the peer's wait for this session's
// ENDACK, unless the end was acknowledged (or refused) already.
func (s *Session) sendNack() bool {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	if s.acked {
		return false
	}
	s.acked = true
	if s.m.c.sendResetFrame(s.id) == nil {
		_ = s.m.c.flush()
	}
	return true
}
