// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// The frames (doc.go "Wire"): a type byte, then uvarints.
const (
	fData   = 'D' // D len payload
	fAck    = 'K' // K n
	fOpen   = 'O' // O epoch rcvd flags toklen token
	fResume = 'R' // R n
	fEnd    = 'E' // E n
	fAbort  = 'X' // X
)

const (
	maxFrame  = 16 << 10 // the largest DATA payload
	maxToken  = 256      // the largest OPEN token
	flagAbort = 1        // OPEN: the session is reset (the client lingers)
)

type frame struct {
	t     byte
	n     uint64 // ACK, RESUME, END: the count; OPEN: the client's rcvd
	epoch uint64 // OPEN
	abort bool   // OPEN
	token []byte // OPEN
	data  []byte // DATA: valid until the next readFrame
}

func appendCount(b []byte, t byte, n uint64) []byte {
	return binary.AppendUvarint(append(b, t), n)
}

func appendOpen(b []byte, epoch, rcvd uint64, abort bool, token []byte) []byte {
	b = binary.AppendUvarint(append(b, fOpen), epoch)
	b = binary.AppendUvarint(b, rcvd)
	var fl byte
	if abort {
		fl = flagAbort
	}
	b = binary.AppendUvarint(append(b, fl), uint64(len(token)))
	return append(b, token...)
}

func appendData(b, p []byte) []byte {
	return append(binary.AppendUvarint(append(b, fData), uint64(len(p))), p...)
}

// readFrame reads one frame; a DATA payload goes into *buf.
func readFrame(r *bufio.Reader, buf *[]byte) (frame, error) {
	t, err := r.ReadByte()
	if err != nil {
		return frame{}, err
	}
	f := frame{t: t}
	uv := func() (uint64, error) {
		v, err := binary.ReadUvarint(r)
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return v, err
	}
	switch t {
	case fData:
		n, err := uv()
		if err != nil {
			return f, err
		}
		if n == 0 || n > maxFrame {
			return f, fmt.Errorf("%w: DATA of %d bytes", ErrProtocol, n)
		}
		if cap(*buf) < int(n) {
			*buf = make([]byte, maxFrame)
		}
		f.data = (*buf)[:n]
		if _, err := io.ReadFull(r, f.data); err != nil {
			return f, err
		}
	case fAck, fResume, fEnd:
		if f.n, err = uv(); err != nil {
			return f, err
		}
	case fAbort:
	case fOpen:
		if f.epoch, err = uv(); err != nil {
			return f, err
		}
		if f.n, err = uv(); err != nil {
			return f, err
		}
		fl, err := r.ReadByte()
		if err != nil {
			return f, err
		}
		f.abort = fl&flagAbort != 0
		tl, err := uv()
		if err != nil {
			return f, err
		}
		if tl > maxToken || fl&^flagAbort != 0 {
			return f, fmt.Errorf("%w: OPEN token %d bytes, flags %#x", ErrProtocol, tl, fl)
		}
		f.token = make([]byte, tl)
		if _, err := io.ReadFull(r, f.token); err != nil {
			return f, err
		}
	default:
		return f, fmt.Errorf("%w: frame type %#x", ErrProtocol, t)
	}
	return f, nil
}
