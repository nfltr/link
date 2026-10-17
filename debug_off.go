// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linkdebug

package link

// debugOn is false in a build without the linkdebug tag: every debug hook
// sits behind `if debugOn`, so the compiler drops it and a Session
// carries no debug field of any size (debug_on.go has the hooks).
const debugOn = false

type debugState struct{}

func debugOpened(*Session)                {}
func debugReleased(*Session)              {}
func debugEnter(*Session, debugOp) func() { return nil }

type debugOp uint8

const (
	debugRead debugOp = iota
	debugWrite
)
