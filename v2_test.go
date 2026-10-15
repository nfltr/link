// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tokenAuth proves a dialer with a fixed token.
type tokenAuth string

func (a tokenAuth) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return MethodToken, []byte(a), nil
}

func (a tokenAuth) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (Identity, error) {
	if method != MethodToken || string(payload) != string(a) {
		return Identity{}, errors.New("unknown token")
	}
	return Identity{Method: "token", Subject: "agent-1"}, nil
}

var testToken = tokenAuth(bytes.Repeat([]byte("ab"), 16))

// countObserver counts events.
type countObserver struct {
	NopObserver
	up, down, open, closed, rtt, goaway atomic.Int32
	downErr                             atomic.Value
}

func (o *countObserver) ConnUp(Identity) { o.up.Add(1) }
func (o *countObserver) ConnDown(_ Identity, err error) {
	o.down.Add(1)
	if err != nil {
		o.downErr.Store(err)
	}
}
func (o *countObserver) SessionOpen()          { o.open.Add(1) }
func (o *countObserver) SessionClose(error)    { o.closed.Add(1) }
func (o *countObserver) PingRTT(time.Duration) { o.rtt.Add(1) }
func (o *countObserver) GoAway(Identity)       { o.goaway.Add(1) }

// v2pair binds a dialer and a listener over a TCP loopback connection
// (Conn writes block on an unbuffered net.Pipe until the peer reads).
func v2pair(t *testing.T, dialer, listener Config) (*Conn, *Conn) {
	t.Helper()
	a, b := tcpPair(t)
	type res struct {
		c   *Conn
		err error
	}
	sc := make(chan res, 1)
	go func() {
		c, _, err := Server(context.Background(), b, listener)
		sc <- res{c, err}
	}()
	cc, err := Client(context.Background(), a, dialer)
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	s := <-sc
	if s.err != nil {
		t.Fatalf("Server: %v", s.err)
	}
	t.Cleanup(func() { _ = cc.Close(); _ = s.c.Close() })
	return cc, s.c
}

func tcpPair(t testing.TB) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	ac := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ac <- c
	}()
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b := <-ac
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return a, b
}

var v2cfg = Config{Window: testWindow, Auth: testToken}

func TestHandshakeV2(t *testing.T) {
	obs := &countObserver{}
	a, b := tcpPair(t)
	go func() {
		_, _ = Client(context.Background(), a, Config{Window: testWindow, MaxSessions: 7, Liveness: LivenessFast, Auth: testToken})
	}()
	c, id, err := Server(context.Background(), b, Config{Window: 2 * testWindow, Auth: testToken, Observer: obs})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	if id != (Identity{Method: "token", Subject: "agent-1"}) || c.Identity() != id {
		t.Fatalf("identity %+v", id)
	}
	if c.Version() != 2 || c.Credit().Window() != 2*testWindow || c.Credit().PeerWindow() != testWindow || c.PeerMaxSessions() != 7 {
		t.Fatalf("version %d window %d peer window %d peer max %d", c.Version(), c.Credit().Window(), c.Credit().PeerWindow(), c.PeerMaxSessions())
	}
	if c.live != LivenessFast {
		t.Fatalf("listener liveness %v, want the dialer's", c.live)
	}
	if obs.up.Load() != 1 {
		t.Fatal("ConnUp not observed")
	}
}

func TestHandshakeVersionNegotiation(t *testing.T) {
	// A version-1 hello (a dialer of link v0.4 or older) gets the version
	// reply, which such a dialer reads as a refusal (its status is not
	// bound), and Server returns ErrVersion.
	v1 := append([]byte(Magic+"\x01"), bytes.Repeat([]byte("ab"), 16)...)
	v1 = append(v1, 0, 4, 0, 0)
	var out bytes.Buffer
	if _, _, err := Server(context.Background(), bufConn{r: bytes.NewReader(v1), w: &out}, v2cfg); !errors.Is(err, ErrVersion) {
		t.Fatalf("v1 hello: %v, want ErrVersion", err)
	}
	if !bytes.Equal(out.Bytes(), []byte{statusVersion, MaxVersion}) {
		t.Fatalf("v1 hello: reply %x, want the version reply", out.Bytes())
	}
	// A hello of a version past this package's gets the version reply too.
	v3 := append([]byte(Magic+"\x03"), make([]byte, helloV2Fixed)...)
	out.Reset()
	if _, _, err := Server(context.Background(), bufConn{r: bytes.NewReader(v3), w: &out}, v2cfg); !errors.Is(err, ErrVersion) {
		t.Fatalf("v3 hello: %v", err)
	}
	var ve *VersionError
	_, err := Client(context.Background(), bufConn{r: bytes.NewReader(out.Bytes()), w: io.Discard}, Config{Auth: testToken})
	if !errors.As(err, &ve) || ve.Max != MaxVersion {
		t.Fatalf("version reply to the dialer: %v", err)
	}
}

func TestHandshakeRefusals(t *testing.T) {
	a, b := tcpPair(t)
	errc := make(chan error, 1)
	go func() {
		_, _, err := Server(context.Background(), b, Config{Auth: testToken})
		errc <- err
	}()
	if _, err := Client(context.Background(), a, Config{Auth: tokenAuth("x")}); !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "Config.Auth") {
		t.Fatalf("client: %v", err)
	}
	if err := <-errc; !errors.Is(err, ErrAuth) {
		t.Fatalf("server: %v", err)
	}
	if _, _, err := Server(context.Background(), bufConn{}, Config{}); err == nil {
		t.Fatal("Server without an Authenticator")
	}
	if _, _, err := Server(context.Background(), bufConn{r: bytes.NewReader([]byte("PRI * HTTP/2.0"))}, Config{Auth: NoAuth}); !errors.Is(err, ErrFrame) {
		t.Fatalf("not a hello: %v", err)
	}
	// ctx bounds the handshake: a listener that never answers.
	a, _ = tcpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := Client(ctx, a, v2cfg); err == nil {
		t.Fatal("handshake outlived its ctx")
	}
}

func TestIsHello(t *testing.T) {
	if !IsHello([]byte("NFDP\x02")) || IsHello([]byte("NFD")) || IsHello([]byte("PRI *")) {
		t.Fatal("IsHello")
	}
}

// v2muxPair returns a dialer's and a listener's Mux, both accepting with
// accept.
func v2muxPair(t *testing.T, accept func([]byte, *Session), dialer, listener Config) (*Mux, *Mux) {
	t.Helper()
	c, s := v2pair(t, dialer, listener)
	dm, lm := NewMux(c), NewMux(s)
	go func() { _ = dm.Serve(accept) }()
	go func() { _ = lm.Serve(accept) }()
	return dm, lm
}

func v2echo(header []byte, s *Session) {
	defer s.Close() //nolint:errcheck
	if string(header) == "nope" {
		return
	}
	_, _ = io.Copy(s, s)
}

func openV2(m *Mux, header string) (*Session, error) {
	return m.Open([]byte(header))
}

func TestMuxV2Sessions(t *testing.T) {
	obs := &countObserver{}
	lcfg := v2cfg
	lcfg.Observer = obs
	dm, lm := v2muxPair(t, v2echo, v2cfg, lcfg)
	var wg sync.WaitGroup
	for i, m := range []*Mux{dm, lm, dm, lm} { // both sides open
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := openV2(m, "target")
			if err != nil {
				t.Error(err)
				return
			}
			if string(s.Header()) != "target" {
				t.Error("header")
			}
			msg := bytes.Repeat([]byte{byte(i)}, 3*testWindow) // past a window: credit returns
			go func() { _, _ = s.Write(msg); _ = s.CloseWrite() }()
			got, err := io.ReadAll(s)
			if err != nil || !bytes.Equal(got, msg) {
				t.Errorf("echo %d: %d bytes, %v", i, len(got), err)
			}
			_ = s.Close()
		}()
	}
	wg.Wait()
	// Ids: odd from the dialer, even from the listener.
	if sid := dm.nextSID(); sid[0]%2 != 1 {
		t.Fatalf("dialer sid %x", sid)
	}
	if sid := lm.nextSID(); sid[0]%2 != 0 {
		t.Fatalf("listener sid %x", sid)
	}
	waitUntil(t, func() bool { return dm.Sessions() == 0 && lm.Sessions() == 0 })
	waitUntil(t, func() bool { return obs.open.Load() == 4 && obs.closed.Load() == 4 })
	// A refused session ends at once.
	s, err := openV2(dm, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("refused session read %d %v", n, err)
	}
}

// Sessions open when their connection ends are reported closed, with
// ConnDown's error, before ConnDown: an observer's open sessions return to
// zero (v0.9.4; before, SessionClose came only for a session's own end).
func TestObserverSessionsClosedWithConn(t *testing.T) {
	obs := &countObserver{}
	lcfg := v2cfg
	lcfg.Observer = obs
	hold := make(chan struct{})
	defer close(hold)
	dm, lm := v2muxPair(t, func(_ []byte, s *Session) { <-hold; _ = s.Close() }, v2cfg, lcfg)
	for _, m := range []*Mux{dm, lm, dm} { // both sides open
		if _, err := openV2(m, "target"); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, func() bool { return obs.open.Load() == 3 })
	if obs.closed.Load() != 0 {
		t.Fatalf("%d sessions closed while open", obs.closed.Load())
	}
	_ = dm.Conn().nc.Close() // the network cuts the connection
	waitUntil(t, func() bool { return obs.down.Load() == 1 })
	if o, c := obs.open.Load(), obs.closed.Load(); o != 3 || c != 3 {
		t.Fatalf("after the connection died: %d opened, %d closed; want 3, 3", o, c)
	}
}

func waitUntil(t testing.TB, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in 5s")
		}
	}
}

// TestMuxV2StuckReaderIsolation: a session whose reader takes nothing
// holds only its window; 8 other sessions on the same connection move
// 1 MiB each meanwhile.
func TestMuxV2StuckReaderIsolation(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)
	accept := func(h []byte, s *Session) {
		defer s.Close() //nolint:errcheck
		if string(h) == "stuck" {
			<-stuck
			return
		}
		_, _ = io.Copy(io.Discard, s)
	}
	dm, _ := v2muxPair(t, accept, v2cfg, v2cfg)
	st, err := openV2(dm, "stuck")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write(make([]byte, 8*testWindow)) }() // stops at the window
	time.Sleep(50 * time.Millisecond)
	var done atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := openV2(dm, "sink")
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := s.Write(make([]byte, 1<<20)); err == nil {
				done.Add(1)
			}
			_ = s.Close()
		}()
	}
	ok := make(chan struct{})
	go func() { wg.Wait(); close(ok) }()
	select {
	case <-ok:
	case <-time.After(10 * time.Second):
	}
	if n := done.Load(); n != 8 {
		t.Fatalf("%d/8 sessions moved 1 MiB beside a stuck one", n)
	}
}

func TestMuxV2MaxSessions(t *testing.T) {
	dcfg, lcfg := v2cfg, v2cfg
	lcfg.MaxSessions = 2
	dm, _ := v2muxPair(t, func(_ []byte, s *Session) { _, _ = io.Copy(io.Discard, s) }, dcfg, lcfg)
	for range 2 {
		if _, err := openV2(dm, "x"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := openV2(dm, "x"); !errors.Is(err, ErrFull) {
		t.Fatalf("third session: %v", err)
	}
}

func TestMuxV2GoAway(t *testing.T) {
	obs := &countObserver{}
	dcfg := v2cfg
	dcfg.Observer = obs
	dm, lm := v2muxPair(t, v2echo, dcfg, v2cfg)
	s, err := openV2(dm, "a")
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return lm.Sessions() == 1 })
	if err := lm.goAway(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dm.c.PeerGoAway():
	case <-time.After(5 * time.Second):
		t.Fatal("GOAWAY not received")
	}
	if _, err := openV2(dm, "b"); !errors.Is(err, ErrGoAway) {
		t.Fatalf("open after GOAWAY: %v", err)
	}
	if _, err := openV2(lm, "b"); !errors.Is(err, ErrGoAway) {
		t.Fatalf("open after own GOAWAY: %v", err)
	}
	// The open session runs on; the connection closes once it ends.
	if _, err := s.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(s, buf); err != nil || string(buf) != "hi" {
		t.Fatalf("session after GOAWAY: %q %v", buf, err)
	}
	_ = s.Close()
	select {
	case <-lm.c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection not closed after its last session")
	}
	if obs.goaway.Load() != 1 {
		t.Fatal("GoAway not observed")
	}
}

func TestPingRTTObserved(t *testing.T) {
	obs := &countObserver{}
	dcfg := v2cfg
	dcfg.Observer, dcfg.Liveness = obs, LivenessFast
	c, s := v2pair(t, dcfg, v2cfg)
	go func() { _, _ = c.Recv() }()
	go func() { _, _ = s.Recv() }()
	waitUntil(t, func() bool { return obs.rtt.Load() > 0 })
}

func TestCreditOverACallersTransport(t *testing.T) {
	// A caller with a transport of its own carries the window updates.
	var updates [][]byte
	var mu sync.Mutex
	send := NewCredit(testWindow, testWindow, nil)
	recv := NewCredit(testWindow, testWindow, func(sid string, n int) {
		mu.Lock()
		updates = append(updates, AppendWindow(nil, sid, n))
		mu.Unlock()
	})
	send.Open("s")
	recv.Open("s")
	if !send.Acquire("s", testWindow) {
		t.Fatal("first window")
	}
	if err := recv.Received("s", testWindow); err != nil {
		t.Fatal(err)
	}
	if err := recv.Received("s", 1); !errors.Is(err, ErrCredit) {
		t.Fatalf("past the window: %v", err)
	}
	recv.Consumed("s", testWindow)
	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 1 {
		t.Fatalf("%d updates", len(updates))
	}
	sid, n, err := ParseWindow(updates[0])
	if err != nil || sid != "s" || n != testWindow {
		t.Fatalf("update %q %d %v", sid, n, err)
	}
	if err := send.Grant(sid, n); err != nil || !send.Acquire("s", testWindow) {
		t.Fatalf("grant: %v", err)
	}
	if err := send.Grant("s", testWindow+1); !errors.Is(err, ErrCredit) {
		t.Fatalf("grant past the window: %v", err)
	}
	if _, _, err := ParseWindow([]byte{3, 'a'}); err == nil {
		t.Fatal("short update parsed")
	}
}

func TestPool(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	var accepted atomic.Int32
	var listeners sync.Map // *Mux -> struct{}
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				c, _, err := Server(context.Background(), nc, v2cfg)
				if err != nil {
					_ = nc.Close()
					return
				}
				accepted.Add(1)
				m := NewMux(c)
				listeners.Store(m, struct{}{})
				_ = m.Serve(v2echo)
			}()
		}
	}()
	var dials atomic.Int32
	failDials := atomic.Int32{}
	p := mustPool(t, PoolConfig{
		Link: v2cfg,
		Dial: func(ctx context.Context) (net.Conn, error) {
			dials.Add(1)
			if failDials.Load() > 0 {
				failDials.Add(-1)
				return nil, errors.New("refused")
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
		},
		Max:      3,
		Grow:     func(st PoolStats) bool { return st.Sessions >= 2*st.Conns },
		GrowWait: 5 * time.Second,
		Backoff:  func(int) time.Duration { return 10 * time.Millisecond },
	})
	defer p.Close() //nolint:errcheck
	ctx := context.Background()
	echo := func(s *Session) {
		t.Helper()
		if _, err := s.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		b := make([]byte, 4)
		if _, err := io.ReadFull(s, b); err != nil || string(b) != "ping" {
			t.Fatalf("echo %q %v", b, err)
		}
	}

	// Growth: a connection per two sessions, at most 3 (GrowWait: each
	// Open that grows takes the new connection).
	var held []*Session
	for range 7 {
		s, err := p.Open(ctx, []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		echo(s)
		held = append(held, s)
	}
	if st := p.Stats(); st.Conns != 3 || st.Sessions != 7 {
		t.Fatalf("stats %+v, want 3 conns 7 sessions", st)
	}

	// Drain: replacement first, new sessions there, old ones finish.
	drained := make(chan error, 1)
	go func() { drained <- p.Drain(ctx) }()
	waitUntil(t, func() bool { return accepted.Load() == 4 })
	s, err := p.Open(ctx, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	echo(s)
	for _, h := range held {
		echo(h) // still running on the old connections
	}
	select {
	case err := <-drained:
		t.Fatalf("drained with sessions open: %v", err)
	default:
	}
	for _, h := range held {
		_ = h.Close()
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return p.Stats().Conns == 1 })

	// A peer's GOAWAY: sessions go elsewhere.
	listeners.Range(func(k, _ any) bool {
		m := k.(*Mux)
		select {
		case <-m.c.Done():
		default:
			_ = m.goAway(time.Now().Add(5 * time.Second))
		}
		return true
	})
	waitUntil(t, func() bool { return p.Stats().Conns == 0 }) // it takes no more sessions
	s2, err := p.Open(ctx, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	echo(s2)
	if accepted.Load() != 5 {
		t.Fatalf("%d connections accepted, want a new one after GOAWAY", accepted.Load())
	}

	// Backoff: failed dials are retried.
	_ = s.Close()
	_ = s2.Close()
	waitUntil(t, func() bool { return p.Stats().Sessions == 0 })
	failDials.Store(2)
	before := dials.Load()
	for range 2 * 3 { // fill past what one connection's Grow allows
		s, err := p.Open(ctx, []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close() //nolint:errcheck
	}
	if dials.Load()-before < 3 {
		t.Fatalf("%d dials, want the failed ones retried", dials.Load()-before)
	}
}

func TestPoolMin(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	conns := make(chan *Conn, 4)
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				c, _, err := Server(context.Background(), nc, v2cfg)
				if err == nil {
					conns <- c
					_ = NewMux(c).Serve(nil)
				}
			}()
		}
	}()
	p := mustPool(t, PoolConfig{Link: v2cfg, Min: 1, Dial: func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
	}, Backoff: func(int) time.Duration { return 10 * time.Millisecond }})
	defer p.Close() //nolint:errcheck
	first := <-conns
	_ = first.Close() // the peer drops it: redialled
	select {
	case <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("Min not kept")
	}
}

// TestPoolAccept: sessions the peer opens on a Pool's connection go to
// Accept; without Accept they are refused (a reset).
func TestPoolAccept(t *testing.T) {
	for _, withAccept := range []bool{true, false} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		served := make(chan *Mux, 1)
		go func() {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			c, _, err := Server(context.Background(), nc, v2cfg)
			if err != nil {
				return
			}
			m := NewMux(c)
			served <- m
			_ = m.Serve(nil)
		}()
		pc := PoolConfig{Link: v2cfg, Min: 1, Dial: func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
		}}
		if withAccept {
			pc.Accept = v2echo
		}
		p := mustPool(t, pc)
		m := <-served
		s, err := m.Open([]byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = s.Write([]byte("ping"))
		b := make([]byte, 4)
		_, err = io.ReadFull(s, b)
		if withAccept && (err != nil || string(b) != "ping") {
			t.Errorf("with Accept: echo %q %v", b, err)
		}
		if !withAccept && !errors.Is(err, ErrReset) {
			t.Errorf("without Accept: read %v, want ErrReset", err)
		}
		_ = s.Close()
		_ = p.Close()
		_ = ln.Close()
	}
}

// TestV2RejectsV1Peers: version-2 frame types are malformed on a
// version-1 connection.
func TestSendOpenLimits(t *testing.T) {
	c, _ := v2pair(t, v2cfg, v2cfg)
	if err := c.SendOpen(string(make([]byte, MaxSID+1)), nil); !errors.Is(err, errSID) {
		t.Fatal(err)
	}
	h := make([]byte, maxHeader+1)
	_, _ = rand.Read(h)
	if err := c.SendOpen("a", h); !errors.Is(err, errHeader) {
		t.Fatal(err)
	}
}

// SetDeadline: no-op (handshakes set one).
func (bufConn) SetDeadline(time.Time) error { return nil }

func mustPool(t testing.TB, cfg PoolConfig) *Pool {
	t.Helper()
	p, err := NewPool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
