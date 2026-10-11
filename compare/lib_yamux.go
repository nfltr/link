// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

// The thin layer yamux needs to stand in for what the replica link takes
// from link: a pool (grow one connection per 2 sessions, least-loaded pick,
// GOAWAY'd and dead connections skipped), a session header, a listener
// GOAWAY that closes each connection once its streams end, a hello with a
// version and a credential checked before yamux starts, liveness from the
// same preset and dead-peer events. Its line count is part of the
// comparison (aspect 10).

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

type yamuxLib struct {
	live  liveness
	ev    *events
	token []byte // hello credential; empty = none

	mu   sync.Mutex
	sess map[*yamux.Session]struct{}
}

// deadLog counts yamux's keepalive failures: it reports them only as log lines.
type deadLog struct{ ev *events }

func (d deadLog) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "keepalive failed") {
		d.ev.add("keepalive-failed", errors.New(strings.TrimSpace(string(p))), true)
	}
	return len(p), nil
}

func (y *yamuxLib) config() *yamux.Config {
	c := yamux.DefaultConfig()
	c.MaxStreamWindowSize = window
	c.KeepAliveInterval = y.live.interval
	c.ConnectionWriteTimeout = y.live.timeout
	c.LogOutput = deadLog{y.ev}
	return c
}

// hello: "YMX" version(1) token_len(1) token; reply status(1), 0 = ok.
func (y *yamuxLib) hello(nc net.Conn) error {
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	defer nc.SetDeadline(time.Time{})
	if _, err := nc.Write(append([]byte{'Y', 'M', 'X', 1, byte(len(y.token))}, y.token...)); err != nil {
		return err
	}
	var st [1]byte
	if _, err := io.ReadFull(nc, st[:]); err != nil || st[0] != 0 {
		return fmt.Errorf("yamux hello refused: %v %d", err, st[0])
	}
	return nil
}

func (y *yamuxLib) verify(nc net.Conn) error {
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	defer nc.SetDeadline(time.Time{})
	var h [5]byte
	if _, err := io.ReadFull(nc, h[:]); err != nil {
		return err
	}
	tok := make([]byte, h[4])
	if _, err := io.ReadFull(nc, tok); err != nil {
		return err
	}
	if string(h[:3]) != "YMX" || h[3] != 1 || subtle.ConstantTimeCompare(tok, y.token) != 1 {
		_, _ = nc.Write([]byte{1})
		return errors.New("yamux hello: bad version or credential")
	}
	_, err := nc.Write([]byte{0})
	return err
}

type ypool struct {
	y     *yamuxLib
	dial  func(context.Context) (net.Conn, error)
	max   int
	mu    sync.Mutex
	conns []*ysess
	dials int
	shut  bool
}

type ysess struct {
	s    *yamux.Session
	away bool // the peer sent GOAWAY (seen at OpenStream); guarded by ypool.mu
}

func (y *yamuxLib) client(dial func(context.Context) (net.Conn, error), maxConns int) client {
	return &ypool{y: y, dial: dial, max: max(maxConns, 1)}
}

// pick returns the least-loaded connection that takes streams and whether
// to dial one more (fewer than max, and none takes streams or the open
// ones carry 2 sessions each).
func (p *ypool) pick() (best *ysess, grow bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.shut {
		return nil, false, net.ErrClosed
	}
	live, streams := 0, 0
	for _, c := range p.conns {
		if c.away || c.s.IsClosed() {
			continue
		}
		live++
		streams += c.s.NumStreams()
		if best == nil || c.s.NumStreams() < best.s.NumStreams() {
			best = c
		}
	}
	grow = len(p.conns)+p.dials < p.max && (best == nil || streams >= 2*live)
	if grow {
		p.dials++
	}
	return best, grow, nil
}

func (p *ypool) add(ctx context.Context) (*ysess, error) {
	defer func() { p.mu.Lock(); p.dials--; p.mu.Unlock() }()
	nc, err := p.dial(ctx)
	if err == nil {
		if err = p.y.hello(nc); err != nil {
			_ = nc.Close()
		}
	}
	if err != nil {
		return nil, err
	}
	s, err := yamux.Client(nc, p.y.config())
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	c := &ysess{s: s}
	p.mu.Lock()
	p.conns = append(p.conns, c)
	p.mu.Unlock()
	go func() { // refuse streams the peer opens; drop the connection when it ends
		for {
			st, err := s.AcceptStream()
			if err != nil {
				p.y.ev.add("down", err, false)
				break
			}
			_ = st.Close()
		}
		p.mu.Lock()
		for i, x := range p.conns {
			if x == c {
				p.conns = append(p.conns[:i], p.conns[i+1:]...)
				break
			}
		}
		p.mu.Unlock()
	}()
	return c, nil
}

func (p *ypool) Open(ctx context.Context, hdr []byte) (net.Conn, error) {
	for {
		c, grow, err := p.pick()
		if err != nil {
			return nil, err
		}
		if grow {
			nc, err := p.add(ctx)
			if err == nil {
				c = nc
			} else if c == nil {
				return nil, err
			}
		}
		if c == nil { // every connection is going away and the pool is full: wait for one to close
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		st, err := c.s.OpenStream()
		if errors.Is(err, yamux.ErrRemoteGoAway) {
			p.mu.Lock()
			c.away = true
			p.mu.Unlock()
			continue
		}
		if errors.Is(err, yamux.ErrSessionShutdown) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := writeHeader(st, hdr); err != nil {
			_ = st.Close()
			return nil, err
		}
		return ystream{st}, nil
	}
}

func (p *ypool) Close() error {
	p.mu.Lock()
	p.shut = true
	conns := append([]*ysess(nil), p.conns...)
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.s.Close()
	}
	return nil
}

// ystream gives a stream net.Conn's half-close: yamux's Close is a
// half-close (the peer reads EOF; this side still reads), full once both
// sides closed or after StreamCloseTimeout.
type ystream struct{ *yamux.Stream }

func (s ystream) CloseWrite() error { return s.Stream.Close() }

// The session header: length(2) bytes, the stream's first bytes.
func writeHeader(w io.Writer, hdr []byte) error {
	_, err := w.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(hdr))), hdr...)) // #nosec G115 -- headers are short
	return err
}

func readHeader(r io.Reader) ([]byte, error) {
	var n [2]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	hdr := make([]byte, binary.BigEndian.Uint16(n[:]))
	_, err := io.ReadFull(r, hdr)
	return hdr, err
}

func (y *yamuxLib) serve(nc net.Conn, handle func([]byte, net.Conn)) {
	if err := y.verify(nc); err != nil {
		_ = nc.Close()
		return
	}
	s, err := yamux.Server(nc, y.config())
	if err != nil {
		_ = nc.Close()
		return
	}
	y.mu.Lock()
	if y.sess == nil {
		y.sess = map[*yamux.Session]struct{}{}
	}
	y.sess[s] = struct{}{}
	y.mu.Unlock()
	for {
		st, err := s.AcceptStream()
		if err != nil {
			y.ev.add("down", err, false)
			break
		}
		go func() {
			hdr, err := readHeader(st)
			if err != nil {
				_ = st.Close()
				return
			}
			handle(hdr, ystream{st})
		}()
	}
	_ = s.Close()
	y.mu.Lock()
	delete(y.sess, s)
	y.mu.Unlock()
}

// goAwayAll sends GOAWAY and closes each connection once its streams
// ended, at deadline at the latest (yamux leaves both to the caller).
func (y *yamuxLib) goAwayAll(deadline time.Time) {
	y.mu.Lock()
	defer y.mu.Unlock()
	for s := range y.sess {
		_ = s.GoAway()
		go func() {
			// ponytail: polls NumStreams; yamux has no stream-count notification
			for s.NumStreams() > 0 && time.Now().Before(deadline) && !s.IsClosed() {
				time.Sleep(50 * time.Millisecond)
			}
			_ = s.Close()
		}()
	}
}

func (y *yamuxLib) served() int {
	y.mu.Lock()
	defer y.mu.Unlock()
	return len(y.sess)
}
