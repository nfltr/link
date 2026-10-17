// Package peerlink is a stub of github.com/nfltr/link/peerlink for the tests.
package peerlink

import (
	"context"
	"net"
)

type Session interface {
	net.Conn
	CloseWrite() error
	Reset() error
}

type Node struct{}

func (n *Node) Open(ctx context.Context, peer string, header []byte) (Session, error) {
	return nil, nil
}
func (n *Node) Serve(accept func(peer string, header []byte, s Session)) error { return nil }
