// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"errors"
	"fmt"
)

// Magic opens a link connection, where an HTTP/2 connection starts "PRI ":
// a listener that shares its port can tell the two apart.
const Magic = "NFDP"

// Status bytes of the listener's reply to a hello (StatusVersion too).
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

// CheckWindow returns w as a window, or an error when it is out of range.
func CheckWindow(w uint32) (int, error) {
	if w < MinWindow || w > MaxWindow {
		return 0, fmt.Errorf("link: window %d out of range", w)
	}
	return int(w), nil
}
