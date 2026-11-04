// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

// mbtCredit is cr[p][s] of spec/Link.tla.
type mbtCredit struct {
	On            bool
	N, Held, Cons int
}

// bySid decodes a function of Sids: TLC writes one over 1..n as an
// array, any other as an object.
type bySid[T any] map[int]T

func (m *bySid[T]) UnmarshalJSON(b []byte) error {
	*m = bySid[T]{}
	var a []T
	if json.Unmarshal(b, &a) == nil {
		for i, v := range a {
			(*m)[i+1] = v
		}
		return nil
	}
	var o map[string]T
	if err := json.Unmarshal(b, &o); err != nil {
		return err
	}
	for k, v := range o {
		i, err := strconv.Atoi(k)
		if err != nil {
			return err
		}
		(*m)[i] = v
	}
	return nil
}

// linkRow is a state of spec/Link.tla as spec/MBTLink.tla writes it.
type linkRow struct {
	A     string
	C     json.RawMessage
	Cr    map[string]bySid[mbtCredit]
	Pend  map[string]bySid[int]
	Gpend map[string]bySid[int]
	H     map[string]struct {
		T string
		S int
	}
}

// mbtOwner is MCLink.tla's MCOwner: sessions 1 and 3 the dialer's.
func mbtOwner(s int) string {
	if s == 2 {
		return "l"
	}
	return "d"
}

func mbtPeer(p string) string {
	if p == "d" {
		return "l"
	}
	return "d"
}

// TestCreditFollowsTheModel replays the behaviours TLC -simulate walked in
// spec/Link.tla (testdata/mbt/link-*, scripts/spec.sh behaviours) on the
// two sides' Credits (window 4, MCLink.tla's), step by step: each change
// of the model's credit state (cr, pend, gpend) is the Credit call the
// code makes for it (open, acquire, sent, refund, Grant for a window
// update, receivedSID for data, Consumed for a read, answering and
// answered for the ACCEPT, forget), the call must succeed as the model's
// step did (credit there to take, a window update within the window, a
// charge within it, a grant at a quarter window read), and after every
// step each Credit's state must be the model's: the session known, its
// credit, held, consumed and pend.
func TestCreditFollowsTheModel(t *testing.T) {
	const win = 4 // MCWin
	steps := 0
	for _, f := range mbtFiles(t, "link") {
		rows := mbtRead[linkRow](t, f)
		granted := map[string]int{}
		cs := map[string]*Credit{}
		for _, p := range []string{"d", "l"} {
			cs[p] = NewCredit(win, win, func(sid string, n int) { granted[p] = n })
		}
		var sids []int
		for s := range rows[0].Cr["d"] {
			sids = append(sids, s)
		}
		sort.Ints(sids)
		for i := 1; i < len(rows); i++ {
			prev, next := rows[i-1], rows[i]
			at := fmt.Sprintf("%s step %d (%s %s)", filepath.Base(f), i, next.A, next.C)
			var ctx struct {
				P string
				S int
			}
			_ = json.Unmarshal(next.C, &ctx)
			for _, p := range []string{"d", "l"} {
				c := cs[p]
				for _, s := range sids {
					sid := string([]byte{byte(s)})
					a, b := prev.Cr[p][s], next.Cr[p][s]
					dp := next.Pend[p][s] - prev.Pend[p][s]
					// The ACCEPT: sent by the accepter, read by the opener.
					// (answering is false once the credit is forgotten: an ABORT
					// that forgot it and has yet to go, as sendAccept ignores.)
					if next.A == "Accept" && ctx.P == p && ctx.S == s && !c.answering(sid) && a.On {
						t.Errorf("%s: %s's answering(%d) refused the ACCEPT", at, p, s)
					}
					if next.A == "Recv" && ctx.P == p && prev.H[mbtPeer(p)].T == "accept" && prev.H[mbtPeer(p)].S == s && a.On {
						if _, err := c.answered(sid); err != nil {
							t.Errorf("%s: %s's answered(%d): %v", at, p, s, err)
						}
					}
					switch {
					case !a.On && b.On:
						by := byPeer
						if mbtOwner(s) == p {
							by = byUs
						}
						c.open(sid, by)
					case a.On && !b.On:
						c.forget(sid, -1)
					case a.On && b.On:
						cur := c.sessions[sid]
						switch {
						case b.N < a.N && dp == a.N-b.N: // Acquire
							if cur.n < dp {
								t.Errorf("%s: %s's acquire(%d, %d) would wait: credit %d", at, p, s, dp, cur.n)
								continue
							}
							c.acquire(sid, dp, nil, nil, true)
						case dp < 0 && b.N == a.N-dp: // Refund
							c.refund(sid, -dp)
						case dp < 0 && b.N == a.N: // the frame buffered
							c.sent(sid, -dp)
						case b.N > a.N && dp == 0: // a window update read
							if err := c.Grant(sid, b.N-a.N); err != nil {
								t.Errorf("%s: %s's Grant(%d, %d): %v", at, p, s, b.N-a.N, err)
							}
						}
						switch {
						case b.Held > a.Held: // data read off the connection
							if _, err := c.receivedSID([]byte(sid), b.Held-a.Held, true); err != nil {
								t.Errorf("%s: %s's receivedSID(%d, %d): %v", at, p, s, b.Held-a.Held, err)
							}
						case b.Cons > a.Cons: // a read below a quarter window
							c.Consumed(sid, b.Cons-a.Cons)
						case next.Gpend[p][s] > prev.Gpend[p][s]: // a read that grants
							delete(granted, p)
							g := next.Gpend[p][s]
							c.Consumed(sid, g-a.Cons)
							if granted[p] != g {
								t.Errorf("%s: %s's Consumed granted %d, the model %d", at, p, granted[p], g)
							}
						}
					}
					// The state after the step.
					cur := c.sessions[sid]
					if (cur != nil) != b.On {
						t.Errorf("%s: %s's session %d known %v, the model's %v", at, p, s, cur != nil, b.On)
						continue
					}
					if cur != nil && (cur.n != b.N || cur.held != b.Held || cur.consumed != b.Cons || cur.pend != next.Pend[p][s]) {
						t.Errorf("%s: %s's session %d: credit %d held %d consumed %d pend %d; the model's %d %d %d %d",
							at, p, s, cur.n, cur.held, cur.consumed, cur.pend, b.N, b.Held, b.Cons, next.Pend[p][s])
					}
				}
			}
			steps++
		}
	}
	t.Logf("%d steps replayed", steps)
}
