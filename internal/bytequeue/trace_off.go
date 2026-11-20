// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package bytequeue

// traceOn is false without the linktrace tag: every trace call sits behind
// `if traceOn`, and a Queue's hook has no size (trace_on.go).
const traceOn = false

type traceHook struct{}

func (q *Queue) trace(string, bool, int, int) {}
