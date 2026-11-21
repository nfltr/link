// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package quiclink

const traceOn = false // trace_on.go: -tags linktrace

func traceEA(*Session, string, ...string) {}

func traceHopRead(*Session) {}
