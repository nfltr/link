// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlink_test

import (
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

// emfileListener fails its first Accept with EMFILE (a process at its file
// descriptor limit), closes again at the next, which blocks until Close.
type emfileListener struct {
	mu     sync.Mutex
	calls  int
	again  chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (l *emfileListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	l.calls++
	n := l.calls
	l.mu.Unlock()
	if n == 1 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept", syscall.EMFILE)}
	}
	if n == 2 {
		close(l.again)
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *emfileListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *emfileListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

// TestServeRetriesAcceptErrors: an Accept error other than a closed
// listener (EMFILE) is retried after a backoff, not the end of Serve.
func TestServeRetriesAcceptErrors(t *testing.T) {
	ln := &emfileListener{again: make(chan struct{}), closed: make(chan struct{})}
	n := mustNew(t, peerlink.Config{Listener: ln, Link: link.Config{Auth: link.NameAuth("b")}})
	served := make(chan error, 1)
	go func() { served <- n.Serve(echoAccept) }()
	select {
	case err := <-served:
		t.Fatalf("Serve returned after one EMFILE: %v", err)
	case <-ln.again:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not call Accept again")
	}
	drainNow(n)
	if err := <-served; err != nil {
		t.Fatalf("Serve after Drain = %v, want nil", err)
	}
}
