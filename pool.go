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
	ended   ConnStats      // the counters of its connections that ended
	tr      poolTraceState // linktrace builds only (trace_pool_on.go)

	drainSnapped func() // tests: runs once Drain took its snapshot
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
	if traceOn {
		tracePool(p)
	}
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
	return !ended(m) && !m.c.GoingAway() && m.Sessions() < min(MaxSessions, m.c.PeerMaxSessions())
}

// Stats is the Pool's load.
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statsLocked()
}

// ConnStats sums the counters of every connection the Pool had, those
// that ended included, so they never go back; QueuedBytes is its open
// connections'.
func (p *Pool) ConnStats() ConnStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.ended
	for _, m := range p.conns {
		cs := m.c.Stats()
		st.add(cs)
		st.QueuedBytes += cs.QueuedBytes
	}
	return st
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

// ended reports whether m closed, or its writes failed (the peer reset
// it; its reader has yet to see the end).
func ended(m *Mux) bool {
	return closed(m.c.Done()) || m.c.writeFailed()
}

// staying is how many connections count against Max: those open and not
// going away, and the dials in flight.
func (p *Pool) stayingLocked() int {
	if traceOn {
		tracePoolSeen(p)
	}
	n := p.dials
	for _, m := range p.conns {
		if !m.c.GoingAway() && !ended(m) {
			n++
		}
	}
	return n
}

// pick returns the connection with the fewest sessions that takes one
// (nil when none), and whether to dial another: synchronously when none
// takes the session, else in the background (growBG). A dial it asks for
// is reserved (counted against Max) before it returns, so concurrent
// Opens never dial past Max; the caller makes it with dial. changed is
// p.changed as pick read it: with neither a connection nor a dial, the
// caller waits on it, so a dial ending after pick wakes it (spec/Pool.tla,
// H1).
// o numbers the Open in a trace (tracePoolOpen).
func (p *Pool) pick(o int) (best *Mux, grow, growBG bool, changed <-chan struct{}, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if traceOn {
		defer func() {
			t := map[bool]string{true: "dial", false: "wait"}[grow]
			switch {
			case err != nil:
				t = map[bool]string{true: "closed", false: "full"}[err == net.ErrClosed]
			case best != nil:
				t = map[bool]string{true: "grow", false: "place"}[growBG]
			}
			tracePoolEv(p, "Pick", t, o, best, 0)
		}()
	}
	if traceOn {
		tracePoolSeen(p)
	}
	if p.ctx.Err() != nil {
		return nil, false, false, nil, net.ErrClosed
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
			return nil, false, false, nil, ErrFull // every connection carries its most sessions
		}
		if room {
			p.dials++
		}
		return nil, room, false, p.changed, nil
	}
	growBG = room && !p.growing && p.cfg.Grow != nil && p.cfg.Grow(p.statsLocked())
	if growBG {
		p.growing = true
		p.dials++
	}
	return best, false, growBG, nil, nil
}

// Open opens a session with header on one of the Pool's connections,
// with the Chain ctx carries (ContextWithChain; Mux.OpenChain) when it
// carries one.
func (p *Pool) Open(ctx context.Context, header []byte) (s *Session, err error) {
	o := 0
	if traceOn {
		if o = tracePoolOpen(p); o != 0 {
			p.traceEv("Start", o, nil, 0)
			defer func() {
				if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, ErrFull) {
					p.traceEv("OpenErr", o, nil, 0)
				}
			}()
		}
	}
	for attempt := 0; ; {
		m, grow, growBG, changed, err := p.pick(o)
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
			nm, err := p.dial(ctx, false)
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
			if err := p.waitChange(ctx, changed); err != nil {
				return nil, err
			}
			continue
		}
		ch, _ := ChainFromContext(ctx)
		s, err := m.OpenChain(header, ch)
		if traceOn && o != 0 {
			p.traceEv("Place", o, m, map[bool]int{true: 1}[err == nil])
			if err == nil {
				go func() { <-s.Done(); p.traceEv("End", o, m, 0) }()
			}
		}
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, ErrGoAway) && !errors.Is(err, ErrFull) && !errors.Is(err, net.ErrClosed) && !m.c.writeFailed() {
			return nil, err
		}
		// m stopped taking sessions meanwhile, or its writes failed: pick
		// again (pick passes it).
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
	m, _ := p.dial(dctx, false)
	p.mu.Lock()
	p.growing = false
	p.mu.Unlock()
	return m
}

// dial opens one more connection and adds it to the Pool; its caller
// reserved it (pick, maintain, Drain), and dial ends the reservation. next:
// once it is up, reserve the caller's next dial in its place (Drain's
// replacements, one at a time: no one takes the room in between).
func (p *Pool) dial(ctx context.Context, next bool) (*Mux, error) {
	m, err := p.connect(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dials--
	p.changedLocked() // its end, failed too: a waiter whose room it held looks again (spec/Pool.tla, H2)
	if traceOn {
		cap := 0
		if m != nil {
			cap = min(MaxSessions, m.c.PeerMaxSessions())
		}
		tracePoolEv(p, "Dial", "", 0, m, cap)
	}
	if err == nil && p.ctx.Err() != nil {
		_ = m.Close()
		err = net.ErrClosed
		if traceOn {
			tracePoolEv(p, "Remove", "", 0, m, 0)
		}
	}
	if err != nil {
		return nil, err
	}
	if next {
		p.dials++
	}
	p.conns = append(p.conns, m)
	go func() { // it stops counting toward Min: maintain replaces it now (spec/Pool.tla, P1)
		select {
		case <-m.c.PeerGoAway():
			p.mu.Lock()
			p.changedLocked()
			if traceOn {
				tracePoolEv(p, "PeerGoAway", "", 0, m, 0)
			}
			p.mu.Unlock()
		case <-m.c.Done():
		}
	}()
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
		if traceOn {
			tracePoolEv(p, "Down", "", 0, m, 0)
			tracePoolEv(p, "Remove", "", 0, m, 0)
		}
		p.ended.add(m.c.Stats())
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

func (p *Pool) waitChange(ctx context.Context, ch <-chan struct{}) error {
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
		if traceOn && !need {
			tracePoolEv(p, "MaintainWait", "", 0, nil, 0)
		}
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
		p.mu.Lock() // Opens may have dialled meanwhile
		need = p.statsLocked().Conns+p.dials < p.cfg.Min && p.stayingLocked() < p.cfg.Max
		if need {
			p.dials++
			if traceOn {
				tracePoolEv(p, "MaintainDial", "", 0, nil, 0)
			}
		}
		p.mu.Unlock()
		if !need {
			continue
		}
		if _, err := p.dial(p.ctx, false); err != nil {
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

// Drain replaces the Pool's connections make-before-break: it dials
// replacements first (Min of them, at least one, but no more than the old
// connections that count against Max, full ones included: one already
// going away counts against nothing), then sends GOAWAY on every old
// connection, which then takes no new session and closes once its open
// sessions ended, at ctx's deadline at the latest. It returns once they
// all closed. A replacement that cannot be dialled leaves the old
// connections as they were and sends GOAWAY on the replacements already
// dialled, so the Pool is back within Max. Call one Drain at a time: a
// second one while the first runs replaces the same connections again,
// and the Pool holds both Drains' replacements beside them meanwhile.
func (p *Pool) Drain(ctx context.Context) error {
	p.mu.Lock()
	if traceOn {
		tracePoolSeen(p)
	}
	old := slices.Clone(p.conns)
	staying := 0
	for _, m := range old {
		if !m.c.GoingAway() && !ended(m) { // full ones too (spec/Pool.tla, M1)
			staying++
		}
	}
	n := min(max(p.cfg.Min, 1), staying) // past Max only by what the GOAWAYs free (spec/Pool.tla, P2)
	if len(old) > 0 && n > 0 {
		// The first replacement counts against Max at the snapshot that
		// planned it: a connection that ends meanwhile is not replaced by
		// maintain too (spec/Pool.tla DrainStart, finding C6).
		p.dials++
	}
	if traceOn && len(old) > 0 {
		tracePoolEv(p, "DrainStart", "", 0, nil, n)
	}
	p.mu.Unlock()
	if p.drainSnapped != nil {
		p.drainSnapped()
	}
	if len(old) == 0 {
		return nil
	}
	deadline, _ := ctx.Deadline()
	var reps []*Mux
	for i := range n {
		m, err := p.dial(ctx, i < n-1) // past Max: the old ones are about to go
		if err != nil {
			p.goAway(reps, deadline) // back within Max (spec/Pool.tla, M2)
			return err
		}
		reps = append(reps, m)
	}
	p.goAway(old, deadline)
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
	return lateErr(ctx)
}

// goAway sends GOAWAY on ms; they stop counting toward Min and Max.
func (p *Pool) goAway(ms []*Mux, deadline time.Time) {
	// Leaving under p.mu, before the frames: a pick sees each of them
	// leaving or not as Drain does (spec/Pool.tla DrainGoAway, finding C10).
	p.mu.Lock()
	for _, m := range ms {
		m.c.goingAway.Store(true)
	}
	p.changedLocked()
	if traceOn {
		tracePoolEv(p, "GoAway", "", 0, nil, 0)
	}
	p.mu.Unlock()
	for _, m := range ms {
		_ = m.goAway(deadline)
	}
}

// Close closes every connection; Open fails from then on. A connection
// that carries no session closes once the frames already written on it
// went out (the tails and closes of the sessions that ended there reach
// the peer), the others at once.
func (p *Pool) Close() error {
	p.mu.Lock()
	p.cancel() // under p.mu: a pick or dial sees the Pool open or closed, not both (spec/Pool.tla CloseStart)
	if traceOn {
		tracePoolEv(p, "CloseStart", "", 0, nil, 0)
	}
	conns := slices.Clone(p.conns)
	p.mu.Unlock()
	for _, m := range conns {
		m.closeIdle()
	}
	if traceOn {
		p.traceEv("CloseEnd", 0, nil, 0)
	}
	return nil
}
