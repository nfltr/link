// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package peerlink

import "net"

// traceOn is false without the linktrace tag: every trace call sits behind
// `if traceOn` and compiles away (trace_on.go has the recorder).
const traceOn = false

type traceState struct{}

func traceStart(*joint)                 {}
func traceJoin(*joint, string, int)     {}
func traceClient(*joint, net.Conn) bool { return false }
