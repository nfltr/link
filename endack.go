// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

// FeatureEndAck is feature bit 2 (WIRE.md sections 6 and 13): the side
// reads ENDACK frames and sends one when its reader takes the peer's half
// close, so the peer's CloseWriteSync returns once the far end has the
// session's whole stream, through any joins (spec/RelayedClose.tla).
const FeatureEndAck uint32 = 1 << 2

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

// waitAck waits for w: nil at the ENDACK, even when the connection ended
// since; else the session's or the connection's end.
func (s *Session) waitAck(w *ackWait) error {
	c := s.m.c
	select {
	case <-w.done:
		return w.err
	case <-c.Done():
		select {
		case <-w.done:
			return w.err
		default:
		}
		return syncEnded(c)
	}
}

// HoldEndAck makes the caller send the session's ENDACK (AckEnd) once it
// passed the peer's end on: a join (peerlink.Join) on a hop (Config.Hop)
// acknowledges only what its onward peer acknowledged.
func (s *Session) HoldEndAck() { s.heldAck.Store(true) }

// AckEnd sends the session's ENDACK once: the peer's half close was
// received (a far end), passed on (a join's, HoldEndAck), or taken by
// this side's reader. Without FeatureEndAck, or before the peer's half
// close, it sends nothing.
func (s *Session) AckEnd() {
	if !s.peerHalf.Load() || s.err.Load() != nil || !s.m.c.endAcks() {
		return
	}
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	if s.acked {
		return
	}
	s.acked = true
	if s.m.c.SendEndAck(s.id) == nil {
		_ = s.m.c.Flush()
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

// closeAckErr is how the peer's whole close ends this side's wait: a
// clean one (the peer, or the far end a hop passed it from, closed the
// session: it takes no more, as a TCP peer's close) nil; a reset or an
// abort its error.
func closeAckErr(f Frame) error {
	switch {
	case f.Abort != nil:
		return f.Abort
	case f.Reset:
		return ErrReset
	}
	return nil
}

// onwardTook waits until the peer took this side's half close: its
// ENDACK, or without FeatureEndAck the pong of a ping after the close.
func (s *Session) onwardTook() error {
	if w := s.ack.Load(); w != nil {
		return s.waitAck(w)
	}
	return s.Sync()
}

// nack tells a peer still waiting for this session's ENDACK, once the
// session ended here (both half closes passed), that it never comes: a
// reset of the sid (its wait ends with ErrReset). True when it sent one.
func (s *Session) nack() bool {
	if !closed(s.done) || !s.peerHalf.Load() || s.peerClosed.Load() && !s.finSent.Load() || !s.m.c.endAcks() {
		return false
	}
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	if s.acked {
		return false
	}
	s.acked = true
	if s.m.c.SendReset(s.id) == nil {
		_ = s.m.c.Flush()
	}
	return true
}
