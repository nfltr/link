// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linktrace

package pathswitch

type traceState struct{}

func (c *Conn) trace(ev, by string, e, n uint64, x string) {}
func (c *Conn) traceN(ev, by string, e, k uint64)          {}

func tracePair(name string, a, b *Conn) {}
func traceEnv(c *Conn, ev, x string)    {}
