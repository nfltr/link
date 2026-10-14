// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"
)

// PoolConfig is a Pool's settings. Every policy is the caller's.
type PoolConfig struct {
	// Link is each connection's Config.
	Link Config
	// Dial opens a connection to the peer, its security done (TLS, a
	// proxy): the caller's.
	Dial func(ctx context.Context) (net.Conn, error)
	// Min connections are kept open, redialled with Backoff when one ends;
	// Max (0 is 1) is the most open at once, not counting those going away
	// (GOAWAY), which take no new session.
	Min, Max int
	// Grow, when set, says whether Open dials one more connection (below
	// Max) although one could take the session: in the background, one
	// such dial at a time. nil grows only when none can.
	Grow func(PoolStats) bool
	// GrowWait is how long the Open that started a growth dial waits for
	// it: the session goes on the new connection when it is up by then,
	// else on one that is up (the dial goes on). 0: it does not wait.
	// Sessions opened in a burst spread over the connections only when
	// the Open waits.
	GrowWait time.Duration
	// Backoff is how long to wait before redialling after attempt failed
	// dials (from 1). nil: Open fails with the dial's error, and Min is
	// redialled once a second.
	Backoff func(attempt int) time.Duration
	// Accept, when set, takes the sessions the peer opens on the Pool's
	// connections (Mux.Serve's accept: it owns the session and closes
	// it). nil: they are refused.
	Accept func(open []byte, s *Session)
}

// PoolStats is a Pool's load: open connections that take sessions, and
// their sessions.
type PoolStats struct{ Conns, Sessions int }

// Pool keeps connections to one peer and opens sessions on them: on the
// one with the fewest sessions, dialling more as Grow says. A connection
// the peer sends GOAWAY on takes no more sessions; its open ones run on.
// Drain replaces every connection make-before-break. Sessions the peer
// opens on the Pool's connections go to Accept (nil: refused).
type Pool struct {
	cfg    PoolConfig
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	conns   []*Mux
	dials   int           // in flight
	growing bool          // a background growth dial is in flight
	changed chan struct{} // closed when conns change
	// refused counts the connections in a row the peer refused: they
	// ended carrying no session after its GOAWAY, or within their
	// liveness interval (a peer shutting down that still accepts). Min
	// is redialled after Backoff(refused), never in a tight loop.
	refused int
}

// NewPool returns a Pool; it dials its Min connections in the background.
// It refuses a PoolConfig that Validate refuses.
func NewPool(cfg PoolConfig) (*Pool, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.Max = max(cfg.Max, cfg.Min, 1)
	p := &Pool{cfg: cfg, changed: make(chan struct{})}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	if cfg.Min > 0 {
		go p.maintain()
	}
	return p, nil
}

// Validate reports the first field of cfg that NewPool refuses, naming it
// and what to set: Dial is required; Min and Max are not negative, and Min
// is at most a Max that is set; Link is valid (Config.Validate).
func (cfg PoolConfig) Validate() error {
	switch {
	case cfg.Dial == nil:
		return errors.New("link: PoolConfig.Dial is nil: set the function that dials the peer (its TLS, if any, done)")
	case cfg.Min < 0 || cfg.Max < 0:
		return fmt.Errorf("link: PoolConfig.Min %d, Max %d: neither is negative", cfg.Min, cfg.Max)
	case cfg.Max > 0 && cfg.Min > cfg.Max:
		return fmt.Errorf("link: PoolConfig.Min %d is over Max %d: lower Min or raise Max", cfg.Min, cfg.Max)
	}
	if err := cfg.Link.Validate(); err != nil {
		return fmt.Errorf("PoolConfig.Link: %w", err)
	}
	return nil
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

// staying is how many connections count against Max: those open and not
// going away, and the dials in flight.
func (p *Pool) stayingLocked() int {
	n := p.dials
	for _, m := range p.conns {
		if !m.c.GoingAway() && !closed(m.c.Done()) {
			n++
		}
	}
	return n
}

// pick returns the connection with the fewest sessions that takes one
// (nil when none), and whether to dial another: synchronously when none
// takes the session, else in the background (growBG).
func (p *Pool) pick() (best *Mux, grow, growBG bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return nil, false, false, net.ErrClosed
	}
	for _, m := range p.conns {
		if takes(m) && (best == nil || m.Sessions() < best.Sessions()) {
			best = m
		}
	}
	staying := p.stayingLocked()
	room := staying < p.cfg.Max
	if best == nil {
		if !room && staying > p.dials {
			return nil, false, false, ErrFull // every connection carries its most sessions
		}
		return nil, room, false, nil
	}
	growBG = room && !p.growing && p.cfg.Grow != nil && p.cfg.Grow(p.statsLocked())
	p.growing = p.growing || growBG
	return best, false, growBG, nil
}

// Open opens a session with header on one of the Pool's connections.
func (p *Pool) Open(ctx context.Context, header []byte) (*Session, error) {
	for attempt := 0; ; {
		m, grow, growBG, err := p.pick()
		if err != nil {
			return nil, err
		}
		if growBG {
			grown := make(chan *Mux, 1)
			go func() { grown <- p.grow(ctx) }()
			if p.cfg.GrowWait > 0 {
				t := time.NewTimer(p.cfg.GrowWait)
				select {
				case nm := <-grown:
					if nm != nil {
						m = nm
					}
				case <-t.C:
				case <-ctx.Done():
				}
				t.Stop()
			}
		}
		if grow { // none takes the session: Open waits for this dial
			nm, err := p.dial(ctx)
			if err != nil {
				attempt++
				if p.cfg.Backoff == nil {
					return nil, err
				}
				if err := p.sleep(ctx, p.cfg.Backoff(attempt)); err != nil {
					return nil, err
				}
				continue
			}
			m, attempt = nm, 0
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

// grow dials one more connection in the background, bounded by the
// deadline of the Open that asked for it (not its cancel: Open returns
// first); a failure leaves the next Open to ask again.
func (p *Pool) grow(ctx context.Context) *Mux {
	dctx := p.ctx
	if dl, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		dctx, cancel = context.WithDeadline(p.ctx, dl)
		defer cancel()
	}
	m, _ := p.dial(dctx)
	p.mu.Lock()
	p.growing = false
	p.mu.Unlock()
	return m
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
		up := time.Now()
		_ = m.Serve(p.cfg.Accept)
		p.mu.Lock()
		if !m.used.Load() && (closed(m.c.PeerGoAway()) || time.Since(up) < m.c.live.Interval) {
			p.refused++
		} else {
			p.refused = 0
		}
		p.conns = slices.DeleteFunc(p.conns, func(x *Mux) bool { return x == m })
		p.changedLocked()
		p.mu.Unlock()
	}()
	return m, nil
}

func (p *Pool) connect(ctx context.Context) (*Mux, error) {
	nc, err := p.cfg.Dial(ctx)
	if err != nil {
		return nil, err
	}
	c, err := Client(ctx, nc, p.cfg.Link)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	return NewMux(c), nil
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
		need := p.statsLocked().Conns+p.dials < p.cfg.Min && p.stayingLocked() < p.cfg.Max
		ch, refused := p.changed, p.refused
		p.mu.Unlock()
		if !need {
			select {
			case <-ch:
			case <-p.ctx.Done():
			}
			continue
		}
		if refused > 0 && attempt == 0 { // the last ones were refused: as a failed dial
			_ = p.sleep(p.ctx, p.backoff(refused))
		}
		if _, err := p.dial(p.ctx); err != nil {
			attempt++
			_ = p.sleep(p.ctx, p.backoff(attempt))
			continue
		}
		attempt = 0
	}
}

// backoff is how long maintain waits after attempt failed dials.
func (p *Pool) backoff(attempt int) time.Duration {
	if p.cfg.Backoff == nil {
		return time.Second
	}
	return p.cfg.Backoff(attempt)
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
