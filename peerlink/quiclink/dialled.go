// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import (
	"context"
	"errors"
	"slices"

	"github.com/nfltr/link/peerlink"
)

// peerConns are the connections this Node dialled to one peer, at most
// Config.Conns.
type peerConns struct {
	sem   chan struct{} // held while picking (and dialling)
	cs    []*conn       // n.mu
	picks int           // pick calls using it (n.mu)
}

// forgetLocked drops peer's p once nothing uses it (no connection, no
// pick) unless Serve keeps peer connected: a peer named by an address
// that changes leaves nothing behind. n.mu is held.
func (n *Node) forgetLocked(peer string, p *peerConns) {
	if p.picks == 0 && len(p.cs) == 0 && n.dialled[peer] == p && !slices.Contains(n.cfg.Connect, peer) {
		delete(n.dialled, peer)
	}
}

// pick returns a usable connection to peer, a session counted on it when
// add: an idle one this Node dialled, else a new one while it has fewer
// than Config.Conns, else the least loaded (dialled at the first Open,
// again once one closed or the peer sent GOAWAY; a further connection
// that fails to dial leaves the session on the least loaded). Picks toward
// one peer are one at a time, so a count is taken before the next pick
// looks.
func (n *Node) pick(ctx context.Context, peer string, add bool) (*conn, error) {
	if n.cfg.Dial == nil {
		return n.pickServed(ctx, peer, add)
	}
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return nil, peerlink.ErrDraining
	}
	p := n.dialled[peer]
	if p == nil {
		p = &peerConns{sem: make(chan struct{}, 1)}
		n.dialled[peer] = p
	}
	p.picks++
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		p.picks--
		n.forgetLocked(peer, p)
		n.mu.Unlock()
	}()
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-p.sem }()
	n.mu.Lock()
	var best *conn
	usable := 0
	for _, c := range p.cs {
		if c.usable() {
			usable++
			if best == nil || c.active < best.active {
				best = c
			}
		}
	}
	if best != nil && (best.active == 0 || usable >= max(n.cfg.Conns, 1)) {
		ok := !add || best.addLocked()
		n.mu.Unlock()
		if !ok {
			return nil, peerlink.ErrDraining
		}
		return best, nil
	}
	n.mu.Unlock()
	c, err := n.dial(ctx, peer, p, add)
	if err != nil && best != nil && ctx.Err() == nil && !errors.Is(err, peerlink.ErrDraining) {
		n.mu.Lock() // another connection failed to dial: the one there is serves
		ok := best.usable() && (!add || best.addLocked())
		n.mu.Unlock()
		if ok {
			return best, nil
		}
	}
	return c, err
}

// dial dials peer for p, a session counted on the connection when add,
// and starts serving it. A Drain that starts meanwhile waits for it, then
// drains its connection with the others: the peer may have opened
// sessions on it already, so it gets GOAWAY, never a bare close.
func (n *Node) dial(ctx context.Context, peer string, p *peerConns, add bool) (*conn, error) {
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		return nil, peerlink.ErrDraining
	}
	n.dialling++
	n.mu.Unlock()
	q, err := n.cfg.Dial(ctx, peer, n.qc.Clone())
	if err == nil {
		err = refuseWithoutAckFrequency(q)
	}
	var c *conn
	if err == nil {
		c = n.newConn(q, peer)
	}
	n.mu.Lock()
	n.dialling--
	close(n.changed) // wakes a Drain waiting for this dial
	n.changed = make(chan struct{})
	draining, cut := n.draining, n.cut
	if c != nil && !cut {
		p.cs = append(p.cs, c)
		if add && !draining {
			c.addLocked()
		}
	}
	n.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if cut {
		_ = q.CloseWithError(CodeDrainDeadline, "drain deadline")
		return nil, peerlink.ErrDraining
	}
	go func() { // #nosec G118 -- the connection outlives the Open that dialled it
		c.run()
		n.mu.Lock()
		p.cs = slices.DeleteFunc(p.cs, func(x *conn) bool { return x == c })
		n.forgetLocked(peer, p)
		n.mu.Unlock()
	}()
	if draining {
		return nil, peerlink.ErrDraining // Drain sends GOAWAY on c
	}
	return c, nil
}
