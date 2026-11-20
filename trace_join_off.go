// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package link

// joinTrace and joinWTrace are the joint trace's (trace_join_on.go); no
// size here.
type (
	joinTrace  struct{}
	joinWTrace struct{}
)

func traceJoinUp(*joint)                   {}
func (*joinWriter) traceJoinW(*joint, int) {}
func (*joinWriter) traceCopy(error)        {}
func (*joint) traceOver(int, error, error) {}
func (*joint) traceFail(int, bool)         {}
