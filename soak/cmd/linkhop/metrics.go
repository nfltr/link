// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/quic-go/quic-go"
)

// bounds are the histograms' upper bounds in seconds.
var bounds = []float64{.0001, .0002, .0005, .001, .002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5, 10, 20, 30}

type hist struct {
	counts []uint64 // per bound, then +Inf
	sum    float64
	n      uint64
}

func (h *hist) observe(v float64) {
	if h.counts == nil {
		h.counts = make([]uint64, len(bounds)+1)
	}
	h.counts[sort.SearchFloat64s(bounds, v)]++
	h.sum += v
	h.n++
}

// quantile is q's value interpolated within its bucket (0: no
// observations; past the last bound: the last bound).
func (h *hist) quantile(q float64) float64 {
	target, cum := q*float64(h.n), 0.0
	for i, c := range h.counts {
		if c == 0 || cum+float64(c) < target {
			cum += float64(c)
			continue
		}
		if i == len(bounds) {
			return bounds[i-1]
		}
		lo := 0.0
		if i > 0 {
			lo = bounds[i-1]
		}
		return lo + (bounds[i]-lo)*(target-cum)/float64(c)
	}
	return 0
}

type series struct{ name, labels string }

// metrics is one role's Prometheus text (/metrics): every series carries
// role and hop; other labels are low-cardinality (peer next or prev,
// reason, code). Safe for concurrent use.
type metrics struct {
	labels string // role="..",hop=".."
	mu     sync.Mutex
	types  map[string]string
	vals   map[series]float64
	hists  map[string]*hist
}

func newMetrics(role string, hop int) *metrics {
	return &metrics{labels: fmt.Sprintf("role=%q,hop=\"%d\"", role, hop),
		types: map[string]string{}, vals: map[series]float64{}, hists: map[string]*hist{}}
}

func (m *metrics) add(typ, name, labels string, v float64) {
	m.mu.Lock()
	m.types[name] = typ
	m.vals[series{name, labels}] += v
	m.mu.Unlock()
}

func (m *metrics) inc(name, labels string) { m.add("counter", name, labels, 1) }
func (m *metrics) set(name, labels string, v float64) {
	m.mu.Lock()
	m.types[name] = "gauge"
	m.vals[series{name, labels}] = v
	m.mu.Unlock()
}

func (m *metrics) observe(name string, d time.Duration) {
	m.mu.Lock()
	h := m.hists[name]
	if h == nil {
		h = &hist{}
		m.hists[name] = h
	}
	h.observe(d.Seconds())
	m.mu.Unlock()
}

// quantile is name's histogram's q quantile in seconds.
func (m *metrics) quantile(name string, q float64) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h := m.hists[name]; h != nil {
		return h.quantile(q)
	}
	return 0
}

func (m *metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.set("linkhop_goroutines", "", float64(runtime.NumGoroutine()))
	m.set("linkhop_heap_inuse_bytes", "", float64(ms.HeapInuse))
	if fds, err := openFDs(); err == nil {
		m.set("linkhop_open_fds", "", float64(fds))
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m.write(w)
}

func (m *metrics) write(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]series, 0, len(m.vals))
	for k := range m.vals {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].name < keys[j].name || keys[i].name == keys[j].name && keys[i].labels < keys[j].labels
	})
	last := ""
	for _, k := range keys {
		if k.name != last {
			fmt.Fprintf(w, "# TYPE %s %s\n", k.name, m.types[k.name])
			last = k.name
		}
		fmt.Fprintf(w, "%s{%s} %g\n", k.name, m.join(k.labels), m.vals[k])
	}
	names := make([]string, 0, len(m.hists))
	for n := range m.hists {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		h := m.hists[n]
		fmt.Fprintf(w, "# TYPE %s histogram\n", n)
		var cum uint64
		for i, b := range bounds {
			cum += h.counts[i]
			fmt.Fprintf(w, "%s_bucket{%s,le=\"%g\"} %d\n", n, m.labels, b, cum)
		}
		fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n%s_sum{%s} %g\n%s_count{%s} %d\n",
			n, m.labels, h.n, n, m.labels, h.sum, n, m.labels, h.n)
	}
}

func (m *metrics) join(labels string) string {
	if labels == "" {
		return m.labels
	}
	return m.labels + "," + labels
}

// openFDs counts this process's open file descriptors (Linux, macOS).
func openFDs() (int, error) {
	for _, dir := range []string{"/proc/self/fd", "/dev/fd"} {
		if es, err := os.ReadDir(dir); err == nil {
			return len(es) - 1, nil // ReadDir's own
		}
	}
	return 0, errors.ErrUnsupported
}

// observer maps a link connection's events onto m; peer is next (dialled)
// or prev (accepted).
type observer struct {
	link.NopObserver
	m    *metrics
	peer string
}

func (o observer) ConnUp(link.Identity) { o.m.inc("linkhop_conn_up_total", "peer=\""+o.peer+"\"") }

func (o observer) ConnDown(_ link.Identity, err error) {
	o.m.inc("linkhop_conn_down_total", fmt.Sprintf("peer=%q,reason=%q", o.peer, reason(err)))
}

func (o observer) SessionOpen() {
	o.m.inc("linkhop_sessions_opened_total", "")
	o.m.add("gauge", "linkhop_sessions_open", "", 1)
}

func (o observer) SessionClose(error) {
	o.m.inc("linkhop_sessions_closed_total", "")
	o.m.add("gauge", "linkhop_sessions_open", "", -1)
}

func (o observer) CreditWait(d time.Duration) {
	o.m.inc("linkhop_credit_waits_total", "")
	o.m.add("counter", "linkhop_credit_wait_seconds_total", "", d.Seconds())
}

func (o observer) PingRTT(d time.Duration) { o.m.observe("linkhop_ping_rtt_seconds", d) }
func (o observer) GoAway(link.Identity)    { o.m.inc("linkhop_goaways_total", "peer=\""+o.peer+"\"") }

var hopKinds = [...]string{link.HopOpened: "opened", link.HopAnswered: "answered", link.HopEnded: "ended"}

func (o observer) Hop(e link.HopEvent) {
	o.m.inc("linkhop_forward_events_total", fmt.Sprintf("event=%q,code=%q", hopKinds[e.Kind], code(e.Err)))
	if e.Kind == link.HopEnded {
		o.m.add("counter", "linkhop_forward_bytes_total", `dir="in"`, float64(e.In))
		o.m.add("counter", "linkhop_forward_bytes_total", `dir="out"`, float64(e.Out))
	}
}

// code is err's *link.Abort code, "" for nil, "other" for another error.
func code(err error) string {
	var a *link.Abort
	switch {
	case err == nil:
		return ""
	case errors.As(err, &a):
		return fmt.Sprint(a.Code)
	}
	return "other"
}

// reason classifies why a connection ended, link's or QUIC's.
func reason(err error) string {
	var idle *quic.IdleTimeoutError
	var app *quic.ApplicationError
	switch {
	case err == nil:
		return "local"
	case errors.Is(err, link.ErrDeadPeer), errors.As(err, &idle):
		return "dead_peer"
	case errors.As(err, &app) && app.ErrorCode == quiclink.CodeDeadPeer:
		return "dead_peer"
	case errors.As(err, &app) && !app.Remote:
		return "local"
	case errors.As(err, &app) && app.ErrorCode == quiclink.CodeDone:
		return "goaway"
	case errors.Is(err, io.EOF):
		return "eof"
	}
	return "error"
}
