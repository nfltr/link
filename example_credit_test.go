// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link_test

import (
	"fmt"
	"time"

	"github.com/nfltr/link"
)

// ExampleNewCredit is link's per-session flow control on a transport of
// the caller's own (gRPC streams, say), without a Conn: the sender takes
// credit before it sends, the receiver charges what arrives and returns
// credit once the application read it, as a window update the caller
// carries back (AppendWindow, ParseWindow). Here the transport is a
// function call, and one session moves twice the window.
func ExampleNewCredit() {
	const window = link.MinWindow
	var sender *link.Credit
	receiver := link.NewCredit(window, window, func(sid string, n int) {
		update := link.AppendWindow(nil, sid, n) // what goes on the wire
		sid, n, err := link.ParseWindow(update)
		if err == nil {
			err = sender.Grant(sid, n)
		}
		if err != nil {
			panic(err)
		}
	})
	sender = link.NewCredit(window, window, func(string, int) {})
	sender.Open("s1")   // the opener, before it sends its open
	receiver.Open("s1") // the other side, when the open arrives

	sent := 0
	for sent < 2*window {
		n := min(link.MaxChunk, 2*window-sent)
		if !sender.Acquire("s1", n) {
			panic("session forgotten")
		}
		// ... the caller sends n bytes of s1; they arrive:
		if err := receiver.Received("s1", n); err != nil {
			panic(err) // the sender broke the credit rules
		}
		receiver.Consumed("s1", n) // the application read them
		sent += n
	}
	fmt.Println("sent", sent, "bytes on a window of", window)
	// Output: sent 262144 bytes on a window of 131072
}

// ExampleIsHello tells a link connection from TLS or HTTP/2 on a shared
// port by its first len(Magic) bytes.
func ExampleIsHello() {
	for _, first := range [][]byte{
		[]byte(link.Magic),
		[]byte("PRI * HTTP/2.0"),
		{0x16, 0x03, 0x01, 0x02}, // a TLS ClientHello's record header
	} {
		fmt.Println(link.IsHello(first[:len(link.Magic)]))
	}
	// Output:
	// true
	// false
	// false
}

// ExampleLiveness_DeadAfter is the bound a caller that watches a peer's
// heartbeats itself waits for, given the interval the peer said it keeps.
func ExampleLiveness_DeadAfter() {
	l := link.LivenessFast // 1.5s + 3s
	fmt.Println(l.DeadAfter(1500 * time.Millisecond))
	fmt.Println(l.DeadAfter(5 * time.Second)) // a slower peer: never under three of its intervals
	fmt.Println(l.DeadAfter(0))               // no stated interval: the transport's keepalive
	// Output:
	// 4.5s
	// 15s
	// 0s
}
