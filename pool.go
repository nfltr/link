// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"time"
)

// PoolConfig is a Pool's settings. Every policy is the caller's.
type PoolConfig struct {
	// Link is each connection's Config; a Pool speaks version 2 (Version
	// 0 or 2).
	Link Config
	// Dial opens a connection to the peer, its security done (TLS, a
	// proxy): the caller's.
	Dial func(ctx context.Context) (net.Conn, error)
	// Min connections are kept open, redialled with Backoff when one ends;
	// Max (0 is 1) is the most open at once.
	Min, Max int
	// Grow, when set, says whether Open dials one more connection (below
	// Max) although one could take the session; nil grows only when none
	// can.
	Grow func(PoolStats) bool
	// Backoff is how long to wait before redialling after attempt failed
	// dials (from 1). nil: Open fails with the dial's error, and Min is
	// redialled once a second.
	Backoff func(attempt int) time.Duration
}

// PoolStats is a Pool's load: open connections that take sessions, and
// their sessions.
type PoolStats struct{ Conns, Sessions int }

// Pool keeps connections to one peer and opens sessions on them: on the
// one with the fewest sessions, dialling more as Grow says. A connection
// the peer sends GOAWAY on takes no more sessions; its open ones run on.
// Drain replaces every connection make-before-break. Sessions the peer
// opens toward a Pool are refused.
type Pool struct {
	cfg    PoolConfig
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	conns   []*Mux
	dials   int           // in flight
	changed chan struct{} // closed when conns change
}

// NewPool returns a Pool; it dials its Min connections in the background.
func NewPool(cfg PoolConfig) *Pool {
	cfg.Max = max(cfg.Max, cfg.Min, 1)
	p := &Pool{cfg: cfg, changed: make(chan struct{})}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	if cfg.Min > 0 {
		go p.maintain()
	}
	return p
}

// takes reports whether m takes a new session.
func takes(m *Mux) bool {
	select {
	case <-m.c.Done():
		return false
	default:
	}
	return !m.c.GoingAway() && m.Sessions() < min(MaxSessions, m.c.PeerMaxSessions())
}

// Stats is the Pool's load.
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statsLocked()
}

func (p *Pool) statsLocked() PoolStats {
	var st PoolStats
	for _, m := range p.conns {
		if takes(m) {
			st.Conns++
			st.Sessions += m.Sessions()
		}
	}
	return st
}

// pick returns the connection with the fewest sessions that takes one
// (nil when none), and whether to dial another.
func (p *Pool) pick() (*Mux, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return nil, false, net.ErrClosed
	}
	var best *Mux
	for _, m := range p.conns {
		if takes(m) && (best == nil || m.Sessions() < best.Sessions()) {
			best = m
		}
	}
	room := len(p.conns)+p.dials < p.cfg.Max
	grow := room && (best == nil || p.cfg.Grow != nil && p.cfg.Grow(p.statsLocked()))
	if best == nil && !room && len(p.conns) > 0 {
		return nil, false, ErrFull
	}
	return best, grow, nil
}

// Open opens a session with header on one of the Pool's connections.
func (p *Pool) Open(ctx context.Context, header []byte) (*Session, error) {
	for attempt := 0; ; {
		m, grow, err := p.pick()
		if err != nil {
			return nil, err
		}
		if grow {
			nm, err := p.dial(ctx)
			switch {
			case err == nil:
				m, attempt = nm, 0
			case m == nil:
				attempt++
				if p.cfg.Backoff == nil {
					return nil, err
				}
				if err := p.sleep(ctx, p.cfg.Backoff(attempt)); err != nil {
					return nil, err
				}
				continue
			}
		}
		if m == nil { // dials in flight hold the room: wait for one
			if err := p.waitChange(ctx); err != nil {
				return nil, err
			}
			continue
		}
		s, err := m.Open(func(string) ([]byte, error) { return header, nil })
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, ErrGoAway) && !errors.Is(err, ErrFull) && !errors.Is(err, net.ErrClosed) {
			return nil, err
		}
		// m stopped taking sessions meanwhile: pick again.
	}
}

// dial opens one more connection and adds it to the Pool.
func (p *Pool) dial(ctx context.Context) (*Mux, error) {
	p.mu.Lock()
	p.dials++
	p.mu.Unlock()
	m, err := p.connect(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dials--
	if err == nil && p.ctx.Err() != nil {
		_ = m.Close()
		err = net.ErrClosed
	}
	if err != nil {
		return nil, err
	}
	p.conns = append(p.conns, m)
	p.changedLocked()
	go func() {
		_ = m.Serve(nil)
		p.mu.Lock()
		p.conns = slices.DeleteFunc(p.conns, func(x *Mux) bool { return x == m })
		p.changedLocked()
		p.mu.Unlock()
	}()
	return m, nil
}

func (p *Pool) connect(ctx context.Context) (*Mux, error) {
	if p.cfg.Link.Version == 1 {
		return nil, errV1
	}
	nc, err := p.cfg.Dial(ctx)
	if err != nil {
		return nil, err
	}
	c, err := Client(ctx, nc, p.cfg.Link)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	return NewMux(c, nil), nil
}

func (p *Pool) changedLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *Pool) waitChange(ctx context.Context) error {
	p.mu.Lock()
	ch := p.changed
	p.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return net.ErrClosed
	}
}

func (p *Pool) sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return net.ErrClosed
	}
}

// maintain keeps Min connections that take sessions open.
func (p *Pool) maintain() {
	for attempt := 0; p.ctx.Err() == nil; {
		p.mu.Lock()
		need := p.statsLocked().Conns+p.dials < p.cfg.Min && len(p.conns)+p.dials < p.cfg.Max
		ch := p.changed
		p.mu.Unlock()
		if !need {
			select {
			case <-ch:
			case <-p.ctx.Done():
			}
			continue
		}
		if _, err := p.dial(p.ctx); err != nil {
			attempt++
			d := time.Second
			if p.cfg.Backoff != nil {
				d = p.cfg.Backoff(attempt)
			}
			_ = p.sleep(p.ctx, d)
			continue
		}
		attempt = 0
	}
}

// Drain replaces the Pool's connections make-before-break: it dials a
// replacement (Min of them, at least one) first, then sends GOAWAY on
// every old connection, which then takes no new session and closes once
// its open sessions ended, at ctx's deadline at the latest. It returns
// once they all closed. A replacement that cannot be dialled leaves the
// old connections as they were.
func (p *Pool) Drain(ctx context.Context) error {
	p.mu.Lock()
	old := slices.Clone(p.conns)
	p.mu.Unlock()
	if len(old) == 0 {
		return nil
	}
	for range max(p.cfg.Min, 1) {
		if _, err := p.dial(ctx); err != nil { // past Max: the old ones are about to go
			return err
		}
	}
	deadline, _ := ctx.Deadline()
	for _, m := range old {
		_ = m.GoAway(deadline)
	}
	for _, m := range old {
		select {
		case <-m.c.Done():
		case <-ctx.Done():
			for _, m := range old {
				_ = m.Close()
			}
			return ctx.Err()
		}
	}
	return nil
}

// Close closes every connection; Open fails from then on.
func (p *Pool) Close() error {
	p.cancel()
	p.mu.Lock()
	conns := slices.Clone(p.conns)
	p.mu.Unlock()
	for _, m := range conns {
		_ = m.Close()
	}
	return nil
}
