// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package peerlinktest

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// UDPProxy relays UDP datagrams to a target and injects the faults a Pair
// needs, for implementations over UDP (QUIC, say): an implementation's
// test points its dialer at Addr and sets Pair.BlackHole, Cut and Throttle
// to the UDPProxy's methods. Each source address it hears from is a flow
// with a socket of its own toward the target.
type UDPProxy struct {
	pc     *net.UDPConn
	target *net.UDPAddr
	hole   atomic.Bool
	rate   atomic.Int64 // bytes per second each way; 0: unthrottled

	mu    sync.Mutex
	flows map[string]*udpFlow // by source address
	dead  map[string]bool     // source addresses Cut dropped
	tos   *udpLimiter         // toward the target
	back  *udpLimiter         // toward the sources
}

type udpFlow struct {
	src *net.UDPAddr
	up  *net.UDPConn // under UDPProxy.mu
}

// NewUDPProxy listens on loopback and relays each source's datagrams to
// target; the test's cleanup closes it.
func NewUDPProxy(t testing.TB, target string) *UDPProxy {
	t.Helper()
	ta, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	p := &UDPProxy{pc: pc, target: ta, flows: map[string]*udpFlow{}, dead: map[string]bool{},
		tos: &udpLimiter{}, back: &udpLimiter{}}
	go p.relay()
	t.Cleanup(func() {
		_ = pc.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, f := range p.flows {
			_ = f.up.Close()
		}
	})
	return p
}

// Addr is where to send to the target through the UDPProxy.
func (p *UDPProxy) Addr() string { return p.pc.LocalAddr().String() }

// BlackHole drops every datagram, now and later, either way.
func (p *UDPProxy) BlackHole() { p.hole.Store(true) }

// Cut drops the flows relayed so far, for good: UDP has no reset to send,
// so their endpoints see a path gone silent, as with BlackHole. A new
// source address (a new socket of the dialer's) is relayed.
func (p *UDPProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, f := range p.flows {
		p.dead[k] = true
		_ = f.up.Close()
		delete(p.flows, k)
	}
}

// Throttle relays at most bytesPerSecond each way; what arrives faster
// waits in a small queue (64 KiB each way) and beyond it is dropped, as at
// a router.
func (p *UDPProxy) Throttle(bytesPerSecond int) { p.rate.Store(int64(bytesPerSecond)) }

// Rebind gives every flow a new socket toward the target, as a NAT
// rebinding its port or a dialer whose address changed would: the target
// sees the flow's datagrams from a new address, and its replies to the old
// one are lost.
func (p *UDPProxy) Rebind() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, f := range p.flows {
		up, err := net.DialUDP("udp", nil, p.target)
		if err != nil {
			continue
		}
		_ = f.up.Close()
		f.up = up
		go p.back.relay(p, up, f)
	}
}

func (p *UDPProxy) relay() {
	buf := make([]byte, 64<<10)
	for {
		n, src, err := p.pc.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if p.hole.Load() {
			continue
		}
		k := src.String()
		p.mu.Lock()
		f := p.flows[k]
		if f == nil && !p.dead[k] {
			up, err := net.DialUDP("udp", nil, p.target)
			if err == nil {
				f = &udpFlow{src: src, up: up}
				p.flows[k] = f
				go p.back.relay(p, up, f)
			}
		}
		var up *net.UDPConn
		if f != nil {
			up = f.up
		}
		p.mu.Unlock()
		if up != nil {
			p.tos.send(p, buf[:n], func(b []byte) { _, _ = up.Write(b) })
		}
	}
}

// relay carries up's datagrams back to f's source while up is f's socket.
func (l *udpLimiter) relay(p *UDPProxy, up *net.UDPConn, f *udpFlow) {
	buf := make([]byte, 64<<10)
	for {
		n, err := up.Read(buf)
		if err != nil {
			return
		}
		p.mu.Lock()
		current := f.up == up
		p.mu.Unlock()
		if !current || p.hole.Load() {
			continue
		}
		l.send(p, buf[:n], func(b []byte) { _, _ = p.pc.WriteToUDP(b, f.src) })
	}
}

// udpLimiter paces one direction at the UDPProxy's rate with a 64 KiB
// queue: a datagram that would wait longer than the queue holds is
// dropped.
type udpLimiter struct {
	mu   sync.Mutex
	next time.Time // when the queue drains
}

func (l *udpLimiter) send(p *UDPProxy, b []byte, write func([]byte)) {
	r := p.rate.Load()
	if r == 0 {
		write(b)
		return
	}
	cost := time.Duration(len(b)) * time.Second / time.Duration(r)
	queue := time.Duration(64<<10) * time.Second / time.Duration(r)
	l.mu.Lock()
	now := time.Now()
	start := now
	if l.next.After(now) {
		start = l.next
	}
	if start.Sub(now) > queue {
		l.mu.Unlock()
		return // the queue is full: drop
	}
	l.next = start.Add(cost)
	at := l.next
	l.mu.Unlock()
	pkt := append([]byte(nil), b...)
	time.AfterFunc(at.Sub(now), func() { write(pkt) })
}
