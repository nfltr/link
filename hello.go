// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// Magic opens a link connection, where an HTTP/2 connection starts "PRI ":
// a listener that shares its port can tell the two apart.
const Magic = "NFDP"

// Version is the hello's protocol version.
const Version = 1

// TokenLen is the length of a hello's token: the hex of 16 random bytes.
const TokenLen = 32

// Status bytes of the listener's reply to a hello.
const (
	StatusBound   = 0
	StatusRefused = 1
)

// Window bounds: a side announces a window in [MinWindow, MaxWindow].
// Credit returns in steps of a quarter window; a window of two MaxChunks
// always returns a waiting sender the MaxChunk it asks for.
const (
	MinWindow = 2 * MaxChunk
	MaxWindow = 1 << 30
)

// ErrRefused is the listener's refusal of a hello: an unknown token, or one
// that binds no more connections.
var ErrRefused = errors.New("link: peer refused the connection")

// Hello binds a new connection with token, announcing window, and returns
// the listener's window from its reply.
func Hello(nc net.Conn, token string, window int) (int, error) {
	if len(token) != TokenLen {
		return 0, fmt.Errorf("link: token of %d bytes, want %d", len(token), TokenLen)
	}
	b := append(append([]byte(Magic), Version), token...)
	if _, err := nc.Write(binary.BigEndian.AppendUint32(b, uint32(window))); err != nil { // #nosec G115 -- a window is checked to fit (CheckWindow)
		return 0, err
	}
	var r [5]byte
	if _, err := io.ReadFull(nc, r[:1]); err != nil {
		return 0, err
	}
	if r[0] != StatusBound {
		return 0, ErrRefused
	}
	if _, err := io.ReadFull(nc, r[1:]); err != nil {
		return 0, err
	}
	return CheckWindow(binary.BigEndian.Uint32(r[1:]))
}

// ReadHello reads the rest of a hello, after its Magic, and returns the
// token and the dialer's window.
func ReadHello(r io.Reader) (string, int, error) {
	var b [1 + TokenLen + 4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", 0, err
	}
	if b[0] != Version {
		return "", 0, fmt.Errorf("link: version %d, want %d", b[0], Version)
	}
	w, err := CheckWindow(binary.BigEndian.Uint32(b[1+TokenLen:]))
	return string(b[1 : 1+TokenLen]), w, err
}

// readHelloV1 reads a version-1 hello after its Magic and version byte.
func readHelloV1(r io.Reader) (string, int, error) {
	var b [TokenLen + 4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", 0, err
	}
	w, err := CheckWindow(binary.BigEndian.Uint32(b[TokenLen:]))
	return string(b[:TokenLen]), w, err
}

// CheckWindow returns w as a window, or an error when it is out of range.
func CheckWindow(w uint32) (int, error) {
	if w < MinWindow || w > MaxWindow {
		return 0, fmt.Errorf("link: window %d out of range", w)
	}
	return int(w), nil
}

// Reply answers a hello; the reply of a bound connection announces window.
func Reply(w io.Writer, status byte, window int) error {
	b := []byte{status}
	if status == StatusBound {
		b = binary.BigEndian.AppendUint32(b, uint32(window)) // #nosec G115 -- a window is checked to fit (CheckWindow)
	}
	_, err := w.Write(b)
	return err
}
