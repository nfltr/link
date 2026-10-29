// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"github.com/nfltr/link/peerlink/quiclink"
)

// faultLink is a pathLink whose traffic crosses a proxy (TCP or UDP),
// so that it can be partitioned.
type faultLink struct {
	pathLink
	blackHole func()
}

func proxiedPair(t *testing.T, quic bool) faultLink {
	if quic {
		pc := listenUDP(t)
		px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
		b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
		a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", px.Addr()), Liveness: link.LivenessFast})
		return faultLink{pathLink{a: a, b: b}, px.BlackHole}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	px := peerlinktest.NewProxy(t, ln.Addr().String())
	b, err := peerlink.New(peerlink.Config{Listener: ln, Link: link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth("b")}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := peerlink.New(peerlink.Config{Link: link.Config{Liveness: link.LivenessFast, Auth: link.NameAuth("a")},
		Pool: link.PoolConfig{Backoff: func(int) time.Duration { return 50 * time.Millisecond }},
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", px.Addr())
		}})
	if err != nil {
		t.Fatal(err)
	}
	return faultLink{pathLink{a: a, b: b}, px.BlackHole}
}

// hopLog keeps a path's HopEnded events by trace, for a failure's log.
type hopLog struct {
	mu   sync.Mutex
	ends map[link.Trace][]string
}

func (l *hopLog) observer(hop int) link.Observer { return hopObs{hop: hop, l: l} }

func (l *hopLog) of(tr link.Trace) []string {
	time.Sleep(200 * time.Millisecond) // the hops' ends
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ends[tr]
}

type hopObs struct {
	link.NopObserver
	hop int
	l   *hopLog
}

func (o hopObs) Hop(e link.HopEvent) {
	if e.Kind != link.HopEnded {
		return
	}
	o.l.mu.Lock()
	o.l.ends[e.Chain.Trace] = append(o.l.ends[e.Chain.Trace], fmt.Sprintf("hop %d: in %d out %d, %v", o.hop, e.In, e.Out, e.Err))
	o.l.mu.Unlock()
}

// faultPath is newPath's path of 6 links (5 hops), each over QUIC or TCP
// as quic says, with an echo at the far end.
func faultPath(t *testing.T, quic []bool, log *hopLog) []faultLink {
	links := make([]faultLink, len(quic))
	for i, q := range quic {
		links[i] = proxiedPair(t, q)
	}
	for i, l := range links {
		accept := farEcho
		if i < len(links)-1 {
			next := links[i+1]
			accept = peerlink.Forward(func(ctx context.Context, _ string, header []byte, _ peerlink.Session) (peerlink.Session, error) {
				return next.a.Open(ctx, "b", header)
			}, link.ForwardConfig{Observer: log.observer(i + 1)})
		}
		go func() { _ = l.b.Serve(accept) }()
		go func() { _ = l.a.Serve(func(_ string, _ []byte, s peerlink.Session) { _ = s.Reset() }) }()
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, l := range links {
			_ = l.a.Drain(ctx)
			_ = l.b.Drain(ctx)
		}
	})
	return links
}

// echoIntact is the chain soak's echo (soak/chainsoak.EchoOnce) on
// links[0]: intact false with err nil is an integrity failure (the
// session ended cleanly with other bytes).
func echoIntact(l peerlink.Link, n int) (tr link.Trace, got int64, intact bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // a hop with no route waits while its origin does
	defer cancel()
	s, err := l.Open(ctx, "b", []byte("echo"))
	if err != nil {
		return tr, 0, true, err
	}
	defer s.Close() //nolint:errcheck
	tr = s.(peerlink.ChainSession).Chain().Trace
	if err := s.(peerlink.ChainSession).Answered(ctx); err != nil {
		return tr, 0, true, err
	}
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(rand.Uint32())
	}
	werr := make(chan error, 1)
	go func() {
		_, err := s.Write(data)
		if err == nil {
			err = s.(interface{ CloseWrite() error }).CloseWrite()
		}
		werr <- err
	}()
	h := sha256.New()
	got, rerr := io.Copy(h, s)
	if err := <-werr; err != nil {
		return tr, got, true, err
	}
	if rerr != nil {
		return tr, got, true, rerr
	}
	want := sha256.Sum256(data)
	return tr, got, got == int64(n) && bytes.Equal(h.Sum(nil), want[:]), nil
}

// TestChainMiddleHopFaultsMixed: link's TestChainMiddleHopFaultsIntegrity
// over peerlink.Forward hops whose links are QUIC or TCP at random (a
// hop splices a link.Session to a quiclink session, or two of a kind):
// the middle hop (3 of 5) is killed, drained or partitioned with echoes
// of up to 4 MiB in flight; every echo either fails with an error or
// comes back intact, none ends cleanly with other bytes.
func TestChainMiddleHopFaultsMixed(t *testing.T) {
	if testing.Short() {
		t.Skip("a liveness bound per partition")
	}
	faults := map[string]func(links []faultLink){
		"kill": func(links []faultLink) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = links[2].b.Drain(ctx)
			_ = links[3].a.Drain(ctx)
		},
		"drain": func(links []faultLink) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.IntN(40))*time.Millisecond)
			defer cancel()
			go func() { _ = links[3].a.Drain(ctx) }()
			_ = links[2].b.Drain(ctx)
		},
		"partition": func(links []faultLink) {
			links[2].blackHole()
			links[3].blackHole()
		},
	}
	runs := map[string]int{"kill": 5, "drain": 5, "partition": 3}
	for name, fault := range faults {
		t.Run(name, func(t *testing.T) {
			var bad, failed, ok atomic.Int64
			for run := range runs[name] {
				quic := make([]bool, 6)
				for i := range quic {
					quic[i] = rand.IntN(2) == 0
				}
				var mix strings.Builder
				for _, q := range quic {
					mix.WriteString(map[bool]string{true: "q", false: "t"}[q])
				}
				t.Run(fmt.Sprintf("%d-%s", run, mix.String()), func(t *testing.T) {
					log := &hopLog{ends: map[link.Trace][]string{}}
					links := faultPath(t, quic, log)
					var stop atomic.Bool
					var wg sync.WaitGroup
					for range 6 {
						wg.Go(func() {
							for !stop.Load() {
								size := rand.IntN(1 << 10)
								switch p := rand.IntN(10); {
								case p >= 8:
									size = rand.IntN(4 << 20)
								case p >= 4:
									size = rand.IntN(64 << 10)
								}
								tr, got, intact, err := echoIntact(links[0].a, size)
								switch {
								case err != nil:
									failed.Add(1)
									time.Sleep(10 * time.Millisecond)
								case !intact:
									bad.Add(1)
									t.Errorf("an echo %x of %d bytes ended cleanly after %d: %q", tr[:4], size, got, log.of(tr))
								default:
									ok.Add(1)
								}
							}
						})
					}
					time.Sleep(time.Duration(50+rand.IntN(150)) * time.Millisecond)
					fault(links)
					time.Sleep(50 * time.Millisecond)
					stop.Store(true)
					wg.Wait()
				})
			}
			t.Logf("%d echoes intact, %d failed, %d integrity failures", ok.Load(), failed.Load(), bad.Load())
		})
	}
}

// TestAbortAfterPeerAbort: a session aborted by its peer (STOP_SENDING
// reached its writes) and then aborted here (a hop passing the other
// side's cut on, as both hops beside a dead one do) neither panics nor
// leaks: quic-go's SetReliableBoundary after the stream's reset counted
// its frames again (panic: numOutStandingFrames negative).
func TestAbortAfterPeerAbort(t *testing.T) {
	pc := listenUDP(t)
	b := mustNew(t, quiclink.Config{PacketConn: pc, TLS: serverTLS("b"), Liveness: link.LivenessFast})
	a := mustNew(t, quiclink.Config{Dial: dialer("a", "b", pc.LocalAddr().String()), Liveness: link.LivenessFast})
	go func() {
		_ = b.Serve(func(_ string, _ []byte, s peerlink.Session) {
			cs := s.(peerlink.ChainSession)
			_ = cs.Accept()
			_, _ = io.CopyN(io.Discard, s, 64<<10)
			_ = cs.Abort(link.CodeApp, "no")
		})
	}()
	go func() { _ = a.Serve(nil) }()
	buf := make([]byte, 256<<10)
	for range 300 {
		s, err := a.Open(context.Background(), "b", nil)
		if err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := s.Write(buf); err != nil {
				break // the peer's STOP_SENDING
			}
		}
		_ = s.(peerlink.ChainSession).Abort(link.CodeApp+1, "passed on")
		_ = s.Close()
	}
}
