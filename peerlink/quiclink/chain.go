// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"unicode/utf8"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	quic "github.com/nfltr/quic-go"
)

// On an ALPNChain connection (since v0.4.0) a session's stream starts
// with the open's chain, hops(1) and trace(16), before the preamble; the
// accepter's stream starts with its answer, a reverse preamble: accept
// kind(1)=0 hops(1), or refuse kind(1)=1 hops(1) code(4) msg_len(2) msg
// (at most link.MaxAbortMsg bytes of UTF-8) followed by FIN. A reset
// (Abort) carries no message: its code and hops are packed in the
// stream error code, chainReset | hops<<32 | code.

// chainReset marks a stream error code that carries an Abort.
const chainReset quic.StreamErrorCode = 1 << 61

var _ peerlink.ChainSession = (*Session)(nil)

// readChain reads an open's chain (ALPNChain).
func readChain(r io.Reader, ch *link.Chain) error {
	var b [17]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	ch.Hops = b[0]
	copy(ch.Trace[:], b[1:])
	return nil
}

// appendAnswer appends a refusal's reverse preamble (a clipped: its
// message valid UTF-8 of at most link.MaxAbortMsg bytes).
func appendAnswer(b []byte, a *link.Abort) []byte {
	b = append(b, 1, a.Hops)
	b = binary.BigEndian.AppendUint32(b, a.Code)
	b = binary.BigEndian.AppendUint16(b, uint16(len(a.Msg))) // #nosec G115 -- clipped
	return append(b, a.Msg...)
}

// errAnswer is a malformed answer.
var errAnswer = errors.New("quiclink: malformed answer to an open")

// parseAnswer reads an answer from b: done once it is whole (nil: an
// accept with its hops; else the refusal), need the bytes still missing.
func parseAnswer(b []byte) (a *link.Abort, hops uint8, need int, err error) {
	switch {
	case len(b) < 2:
		return nil, 0, 2 - len(b), nil
	case b[0] == 0:
		return nil, b[1], 0, nil
	case b[0] != 1:
		return nil, 0, 0, errAnswer
	case len(b) < 8:
		return nil, 0, 8 - len(b), nil
	}
	n := int(binary.BigEndian.Uint16(b[6:]))
	switch {
	case n > link.MaxAbortMsg:
		return nil, 0, 0, errAnswer
	case len(b) < 8+n:
		return nil, 0, 8 + n - len(b), nil
	case !utf8.Valid(b[8 : 8+n]):
		return nil, 0, 0, errAnswer
	}
	return &link.Abort{Refused: true, Code: binary.BigEndian.Uint32(b[2:]), Hops: b[1], Msg: string(b[8 : 8+n])}, 0, 0, nil
}

// answer reads the opener's answer once (a read deadline that passes
// meanwhile returns its error and keeps what was read).
func (s *Session) answer() error {
	s.amu.Lock()
	defer s.amu.Unlock()
	if s.aDone {
		return s.aErr
	}
	for {
		a, hops, need, err := parseAnswer(s.abuf)
		if err != nil || need == 0 {
			s.aDone, s.pathLen = true, hops
			switch {
			case err != nil:
				s.aErr = err
			case a != nil:
				s.aErr = a
				s.Stream.CancelRead(CodeClosed)
			}
			return s.aErr
		}
		b := make([]byte, need)
		k, err := s.Stream.Read(b)
		s.abuf = append(s.abuf, b[:k]...)
		if _, _, need, perr := parseAnswer(s.abuf); err != nil && need == 0 && perr == nil {
			continue // whole, with the FIN (a refusal's)
		}
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return err
			}
			if errors.Is(err, io.EOF) {
				err = errAnswer // FIN before an answer
			}
			s.aDone, s.aErr = true, s.mapErr(err)
			return s.aErr
		}
	}
}

// mapErr turns a stream error carrying an Abort (ALPNChain) into it, a
// write refused by the peer's Close into io.ErrClosedPipe (wrapping it),
// and this Node's own Drain deadline closing the connection into
// link.ErrDrainDeadline (wrapping it: a splice sends it on as
// link.CodeDrainDeadline, as on a link.Mux).
func (s *Session) mapErr(err error) error {
	var ae *quic.ApplicationError
	if errors.As(err, &ae) && !ae.Remote && ae.ErrorCode == CodeDrainDeadline {
		return fmt.Errorf("%w: %w", err, link.ErrDrainDeadline)
	}
	var se *quic.StreamError
	if err == nil || !errors.As(err, &se) {
		return err
	}
	switch c := se.ErrorCode; {
	case s.c.chain && c&chainReset != 0:
		return &link.Abort{Code: uint32(c), Hops: uint8(c >> 32)} // #nosec G115 -- packed by abortCode
	case se.Remote && c == CodeClosed:
		return fmt.Errorf("%w: %w", io.ErrClosedPipe, err)
	}
	return err
}

// abortCode packs a reset's code and hops into a stream error code.
func abortCode(a *link.Abort) quic.StreamErrorCode {
	return chainReset | quic.StreamErrorCode(a.Hops)<<32 | quic.StreamErrorCode(a.Code)
}

// Read reads the peer's bytes; on an ALPNChain connection the opener
// first reads the answer (a refusal's *link.Abort is then every Read's
// error).
func (s *Session) Read(b []byte) (int, error) {
	if s.c.chain && !s.accepter {
		if err := s.answer(); err != nil {
			return 0, err
		}
	}
	n, err := s.Stream.Read(b)
	return n, s.mapErr(err)
}

// Write sends b; the accepter's first Write answers the open (accepted).
func (s *Session) Write(b []byte) (int, error) {
	s.autoAccept()
	n, err := s.Stream.Write(b)
	return n, s.mapErr(err)
}

// Answers reports whether the connection negotiated ALPNChain.
func (s *Session) Answers() bool { return s.c.chain }

// Chain is the open's (zero without ALPNChain).
func (s *Session) Chain() link.Chain { return s.chain }

// PathLen is the accept's hops (link.Session.PathLen).
func (s *Session) PathLen() uint8 {
	s.amu.Lock()
	defer s.amu.Unlock()
	return s.pathLen
}

// Answered waits for the answer to a session this side opened
// (link.Session.Answered): nil once accepted, the refusal's *link.Abort,
// the error that ended the stream first, or ctx's error; link.ErrNoAnswers
// without ALPNChain.
func (s *Session) Answered(ctx context.Context) error {
	switch {
	case !s.c.chain:
		return link.ErrNoAnswers
	case s.accepter:
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- s.answer() }() // ends with the answer or the stream
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Accept answers a session the peer opened: accepted (link.Session.Accept).
func (s *Session) Accept() error { return s.AcceptPath(0) }

// AcceptPath is Accept for a hop with the path's length beyond it.
func (s *Session) AcceptPath(pathLen uint8) error {
	if !s.c.chain || !s.accepter {
		return nil
	}
	var err error
	s.ansOnce.Do(func() {
		defer s.ans.Store(true)
		_, err = s.Stream.Write([]byte{0, pathLen})
	})
	return err
}

func (s *Session) autoAccept() {
	if s.c.chain && s.accepter && !s.ans.Load() {
		_ = s.AcceptPath(0)
	}
}

// Refuse answers a session the peer opened: no, with code and msg; it
// ends the session. Without ALPNChain it is Reset.
func (s *Session) Refuse(code uint32, msg string) error {
	return s.AbortWith(&link.Abort{Refused: true, Code: code, Msg: msg})
}

// Abort resets the session with code (the stream error carries no
// message). Without ALPNChain it is Reset.
func (s *Session) Abort(code uint32, msg string) error {
	return s.AbortWith(&link.Abort{Code: code, Msg: msg})
}

// AbortWith is Refuse (a.Refused) or Abort with a's code and hops.
func (s *Session) AbortWith(a *link.Abort) error {
	if !s.c.chain {
		s.reset.Store(true)
		s.Stream.CancelWrite(CodeReset)
		s.Stream.CancelRead(CodeReset)
		s.ended()
		return nil
	}
	if a.Refused {
		if !s.accepter {
			return errors.New("quiclink: Refuse of a session this side opened (Abort it)")
		}
		refused := false
		s.ansOnce.Do(func() {
			refused = true
			s.ans.Store(true)
			_, _ = s.Stream.Write(appendAnswer(nil, clipped(a)))
			_ = s.Stream.Close()
			s.Stream.CancelRead(CodeRefused)
		})
		if !refused {
			return errors.New("quiclink: Refuse of a session already answered (Abort it)")
		}
		s.reset.Store(true)
		s.ended()
		return nil
	}
	s.reset.Store(true)
	// The bytes written before the abort arrive (RESET_STREAM_AT); once
	// the write side was reset (the peer's STOP_SENDING) it does nothing.
	s.Stream.SetReliableBoundary()
	s.Stream.CancelWrite(abortCode(a))
	s.Stream.CancelRead(abortCode(a))
	s.ended()
	return nil
}

// clipped is a with its message cut to link.MaxAbortMsg at a UTF-8
// boundary (as link's Refuse).
func clipped(a *link.Abort) *link.Abort {
	c := *a
	if len(c.Msg) > link.MaxAbortMsg {
		n := link.MaxAbortMsg
		for n > 0 && !utf8.RuneStart(c.Msg[n]) {
			n--
		}
		c.Msg = c.Msg[:n]
	}
	if !utf8.ValidString(c.Msg) {
		c.Msg = ""
	}
	return &c
}
