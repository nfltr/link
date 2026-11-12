#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# spec.sh check|mutants|traces: the TLA+ model of spec/ (spec/README.md).
#   check    TLC on spec/Link.cfg, LinkDrain.cfg and LinkSess.cfg (no crash),
#            LinkCrash.cfg, then spec/Pool.cfg, Liveness.cfg, Join.cfg,
#            JoinClose.cfg and CreditWaiters.cfg, DirectPath.cfg:
#            every invariant and liveness property, and Refines.
#   invariants  the same, Link's invariants only, Refines only on
#            LinkCrash.cfg (CI's; Pool's liveness is checked too, seconds).
#   nightly  Link.cfg with its crash: invariants and Refines.
#   nightly-wide  the configurations wider than make spec's: PoolWide.cfg,
#            PoolClient.cfg with a third slot, LinkBoth.cfg, LinkCutHalf.cfg.
#   mutants  each Mutant of Link.tla, Pool.tla, Liveness.tla, Join.tla and JoinClose.tla must be
#            caught (a TLC violation); each Mutant of Link.tla must also
#            fail the session interface alone (Refines, LinkSession.tla),
#            each witness must be violated, and each trace fixture of
#            spec/traces/ (a recorded run with a bug put in) rejected.
#   traces   the tests in TRACE_TESTS and TRACE_DEAD_TESTS run with -tags
#            linktrace, each connection's trace checked against the model
#            (spec/Trace.tla).
#   trace F  one recorded trace.
#   behaviours  TLC -simulate walks for the model-based tests (testdata/mbt).
# TLC_WORKERS (default 4) caps TLC's threads. Java and tla2tools.jar come
# from scripts/fetch-tla.sh.
set -eu
cd "$(dirname "$0")/.."
tools=$(scripts/fetch-tla.sh)
java=$(echo "$tools" | sed -n 1p)
jar=$(echo "$tools" | sed -n 2p)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# tlc CFG MODULE [args]: TLC in spec/, its state files in $META (default $work/states).
tlc() {
	c=$1 mod=$2
	shift 2
	(cd spec && "$java" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-4}" \
		-noGenerateSpecTE -metadir "${META:-$work/states}" -config "$c" "$@" "$mod")
}

# live FILE SIDE: TLC on one side's keepAlive steps (spec/TraceLive.tla);
# it fails unless the trace is accepted.
live() {
	cat >"${jw:-$work}/live.cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Side = "$2"
INIT TraceInit
NEXT TraceNext
POSTCONDITION TraceAccepted
INVARIANT TraceNeverEarly
CFG
	META=$work/states-live-$(basename "$1")-$2 TLC_WORKERS=1 tlc "${jw:-$work}/live.cfg" TraceLive.tla -deadlock
}

# trace FILE: TLC on one recorded trace (spec/Trace.tla).
trace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Sids <- TraceSids
    Owner <- TraceOwner
    Win <- TraceWin
    Chain <- TraceChain
    NoSid = ""
    Chunk = 0
    Mutant = "none"
    MaxBytes = 0
    MaxPings = 0
    Crash = 0
    Drain = {"d", "l"}
    Ends = {0, 1, 2, 3, 5}
    MaxOut = 0
    MaxSess = 1048576
INIT TraceInit
NEXT TraceNext
POSTCONDITION TraceAccepted
INVARIANTS
    NoProtocolError
    CreditBounds
    BufferBound
    Conservation
    QueueWithinHeld
    FinBeforeShut
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" Trace.tla -deadlock
}

# jtrace FILE: TLC on one joint's recorded trace (spec/TraceJoin.tla); it
# passes when TLC reaches the trace's end (TraceUnfinished violated).
jtrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    N <- TraceN
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANT TraceUnfinished
CFG
	jst=0 # not st: the callers' (sh has no locals)
	META=$work/states-$(basename "$1") tlc "$cfg" TraceJoin.tla -deadlock >"$cfg.out" 2>&1 || jst=$?
	cat "$cfg.out"
	[ $jst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# ptrace FILE: TLC on one Pool's recorded trace (spec/TracePool.tla); it
# fails unless TLC reaches the trace's end (TraceUnfinished violated).
ptrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Ids <- TraceIds
    Opens <- TraceOpens
    Min <- TraceMin
    Max <- TraceMax
    Cap <- TraceCap
    MaxEnv <- TraceEnv
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANT TraceUnfinished
CFG
	pst=0 # not st: the callers'
	META=$work/states-$(basename "$1") tlc "$cfg" TracePool.tla -deadlock >"$cfg.out" 2>&1 || pst=$?
	cat "$cfg.out"
	[ $pst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# dtrace FILE DIR: TLC on one direction (c2a, a2c) of a pathswitch
# session's recorded trace (spec/TraceDirectPath.tla); it fails unless TLC
# reaches the trace's end (TraceUnfinished violated). The trace's own
# constants are counted here: TLC would evaluate them on every state.
dtrace() {
	cfg=$work/$(basename "$1").$2.cfg
	s=client
	[ "$2" = c2a ] || s=server
	tot=$(grep -c "\"ev\":\"Write\",\"by\":\"$s\"" "$1" || true)
	grep -q "\"ev\":\"SendEnd\",\"by\":\"$s\"" "$1" || tot=$((tot + 1))
	big=$(($(wc -l <"$1") + 1))
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Dir = "$2"
    Total = $tot
    W = $(head -1 "$1" | sed 's/.*"n":\([0-9]*\).*/\1/')
    MaxDie = $big
    MaxPunch = $big
    MaxMoves = $big
    MaxAbort = 1
    MaxRebind = $big
    Proof = 0
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANT TraceUnfinished
CFG
	dst=0 # not st: the callers'
	META=$work/states-$(basename "$1")-$2 tlc "$cfg" TraceDirectPath.tla -deadlock >"$cfg.out" 2>&1 || dst=$?
	cat "$cfg.out"
	[ $dst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# tlcrun CFG: TLC on MCLink.tla with CFG, which must pass.
tlcrun() {
	st=0
	tlc "$1" MCLink.tla >"$work/check.out" 2>&1 || st=$?
	grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/check.out" || true
	if [ $st != 0 ]; then
		tail -60 "$work/check.out"
		exit $st
	fi
}

case ${1:-} in
nightly)
	# Link.cfg with its crash: invariants and Refines (45 million states,
	# 35 min on the runner; its liveness with the crash is past any bound
	# here).
	echo "== Link.cfg (crash, invariants and Refines)"
	sed -e '/^PROPERTIES/,$d' spec/Link.cfg >"$work/Link.cfg"
	tlcrun "$work/Link.cfg"
	;;
nightly-wide)
	# Its own nightly job: after spec and Link.cfg's crash (about 60 min on
	# the runner) these (about 35 min) went past the job's 90 min bound.
	# The Pool wider than Pool.cfg (review r2 L5): two Opens, Min below Max.
	echo "== PoolWide.cfg"
	st=0
	tlc PoolWide.cfg Pool.tla >"$work/check.out" 2>&1 || st=$?
	grep -E '^[0-9]+ states generated|Finished in|Error|violated' "$work/check.out" || true
	[ $st = 0 ] || { tail -60 "$work/check.out"; exit $st; }
	# A client's Pool past one connection's max sessions, wider (lane CONNPERF).
	echo "== PoolClient.cfg (Ids {1, 2, 3}, MaxEnv 1)"
	sed -e 's/Ids = {1, 2}/Ids = {1, 2, 3}/' -e 's/MaxEnv = 0/MaxEnv = 1/' spec/PoolClient.cfg >"$work/PoolClient.cfg"
	st=0
	tlc "$work/PoolClient.cfg" Pool.tla >"$work/check.out" 2>&1 || st=$?
	grep -E '^[0-9]+ states generated|Finished in|Error|violated' "$work/check.out" || true
	[ $st = 0 ] || { tail -60 "$work/check.out"; exit $st; }
	# Sessions of both sides at once, data both ways (review r2 LinkSession
	# 7): 3.6 million states, about 15 minutes on 4 workers.
	echo "== LinkBoth.cfg (invariants and Refines)"
	tlcrun LinkBoth.cfg
	# The drain's cut with CloseWrite (lane LINKATOMIC): 2.6 million states.
	echo "== LinkCutHalf.cfg (invariants)"
	sed -e '/^PROPERTY Refines/d' -e '/^PROPERTIES/,$d' spec/LinkCutHalf.cfg >"$work/LinkCutHalf.cfg"
	tlcrun "$work/LinkCutHalf.cfg"
	;;
check | invariants)
	for cfg in Link.cfg LinkDrain.cfg LinkSess.cfg LinkSessHalf.cfg LinkCrash.cfg LinkRefuse.cfg LinkCut.cfg; do
		echo "== $cfg ($1)"
		# No crash in the first three: Link.cfg's crash grows its states past
		# 20 million (spec-nightly); LinkCrash.cfg takes it on fewer bytes.
		# invariants (CI) checks Refines only on LinkCrash.cfg: the
		# interface's mutants and witnesses (spec-mutants) are CI's too.
		if [ "$1" = invariants ] && [ $cfg != LinkCrash.cfg ]; then
			sed -e '/^PROPERTY Refines/d' -e '/^PROPERTIES/,$d' -e 's/Crash = 1/Crash = 0/' "spec/$cfg" >"$work/$cfg"
		else
			sed -e 's/Crash = 1/Crash = 0/' "spec/$cfg" >"$work/$cfg"
		fi
		# LinkCrash.cfg, LinkRefuse.cfg and LinkSessHalf.cfg (seconds): Refines and liveness in CI too.
		# LinkCut.cfg's (the drain's cut, 142,465 states): in check only (minutes).
		[ $cfg != LinkCrash.cfg ] && [ $cfg != LinkRefuse.cfg ] && [ $cfg != LinkSessHalf.cfg ] || cp spec/$cfg "$work/$cfg"
		tlcrun "$work/$cfg"
	done
	# PoolDrain (Min 2: a Drain failing halfway) and QuicWatchStops (two
	# short stops, ~10 s) only in full: their mutants are CI's.
	# PoolGrow: background growth (Max above Min).
	pools="Pool PoolGrow PoolClient Liveness LivenessStarve QuicWatch QuicWatchPTO QuicWatchStarve Join JoinClose CreditWaiters DirectPath DirectPathReset DirectPathRebind DirectPathProof"
	[ "$1" = invariants ] || pools="$pools PoolDrain QuicWatchStops DirectPathWide"
	for m in $pools; do
		echo "== $m.cfg"
		st=0
		# Liveness explores a horizon of ticks, Join one session,
		# CreditWaiters a fixed number of Acquires: their last states end there; PoolDrain has no Open to keep it going.
		dl=; [ $m = Pool ] || [ $m = PoolGrow ] || [ $m = PoolClient ] || dl=-deadlock
		mod=$m.tla; case $m in Pool*) mod=Pool.tla ;; Liveness*) mod=Liveness.tla ;; QuicWatch*) mod=QuicWatch.tla ;; CreditWaiters*) mod=CreditWaiters.tla ;; DirectPath*) mod=DirectPath.tla ;; esac
		cfg=$m.cfg
		# DirectPath's liveness (AllDelivered, ResetReaches: minutes) only in
		# full; spec-mutants catches keep-dead-path and abort-not-resent by it.
		if [ "$1" = invariants ] && [ "${m#DirectPath}" != "$m" ]; then
			sed -e '/^PROPERTIES/,$d' spec/$m.cfg >"$work/$m.cfg"
			cfg=$work/$m.cfg
		fi
		tlc "$cfg" "$mod" $dl >"$work/check.out" 2>&1 || st=$?
		grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/check.out" || true
		[ $st = 0 ] || exit $st
	done
	;;
mutants)
	# Each check below runs in a job of its own (SPEC_JOBS at once, one
	# TLC worker each: a check's cost is mostly the JVM's start); its line
	# is printed in order once all ran.
	# mutant CFG NAME [MODULE]: TLC must report a violation (Link: of an
	# invariant; Pool and Liveness keep their properties).
	mutant() {
		mod=${4:-MCLink.tla}
		if [ "$mod" = MCLink.tla ]; then
			# its invariant must catch it (the interface's checks are iface's)
			sed -e "s/$2/$3/" -e '/^PROPERTY Refines/d' -e '/^PROPERTIES/,$d' "spec/$1" >"$jw/Mutant.cfg"
		else
			sed -e "s/$2/$3/" "spec/$1" >"$jw/Mutant.cfg"
		fi
		dl=; [ "$mod" != Liveness.tla ] && [ "$mod" != QuicWatch.tla ] && [ "$mod" != Join.tla ] && [ "$mod" != JoinClose.tla ] && [ "$mod" != CreditWaiters.tla ] && [ "$mod" != DirectPath.tla ] && [ "$1" != PoolDrain.cfg ] || dl=-deadlock
		if tlc "$jw/Mutant.cfg" "$mod" $dl >"$jw/m.out" 2>&1; then
			echo "$3: not caught"
			return 1
		elif grep -q '^Error: \(Invariant\|Temporal propert\(y\|ies\)\|Action property\) .* violated' "$jw/m.out"; then
			echo "$3: caught: $(grep '/\\ err = ' "$jw/m.out" | tail -1 | sed 's/.*err = //') ($(grep -m1 -E '^Error: (Invariant|Temporal property|Action property)' "$jw/m.out" | sed -E 's/^Error: (Invariant|Temporal property|Action property) ([A-Za-z]+).*/\2/'), $(grep -c '^State [0-9]*:' "$jw/m.out") states deep)"
		else
			echo "$3: TLC failed:"
			tail -20 "$jw/m.out"
			return 1
		fi
	}
	# iface CFG MUTANT: Link with MUTANT fails the session interface
	# (Refines, LinkSession.tla) with every invariant of Link left out.
	iface() {
		sed -e "s/Mutant = \"none\"/Mutant = \"$2\"/" -e '/^INVARIANTS/,$d' "spec/$1" >"$jw/Mutant.cfg"
		if tlc "$jw/Mutant.cfg" MCLink.tla >"$jw/m.out" 2>&1; then
			echo "$2: not caught by the interface"
			return 1
		elif grep -q '^Error: Action property .* of module LinkSession is violated' "$jw/m.out"; then
			echo "$2: fails the interface ($(grep -c '^State [0-9]*:' "$jw/m.out") states deep)"
		else
			echo "$2: TLC failed:"
			tail -20 "$jw/m.out"
			return 1
		fi
	}
	# witness CFG NAME [MODULE]: the witness NAME must be violated.
	witness() {
		sed -e '/^PROPERTY/,$d' "spec/$1" >"$jw/Witness.cfg"
		# WitnessAct*: an action property (checked as a PROPERTY).
		case $2 in WitnessAct*) kind=PROPERTY what="Action property" ;; *) kind=INVARIANT what=Invariant ;; esac
		printf '%s %s\n' "$kind" "$2" >>"$jw/Witness.cfg"
		dl=; [ "${3:-}" != Join.tla ] && [ "${3:-}" != JoinClose.tla ] && [ "${3:-}" != Liveness.tla ] && [ "${3:-}" != QuicWatch.tla ] && [ "${3:-}" != CreditWaiters.tla ] && [ "${3:-}" != DirectPath.tla ] || dl=-deadlock
		if tlc "$jw/Witness.cfg" "${3:-MCLink.tla}" $dl >"$jw/m.out" 2>&1; then
			echo "$2: never violated (vacuous)"
			return 1
		elif grep -q "^Error: $what $2 is violated" "$jw/m.out"; then
			echo "$2: violated, as it must be ($(grep -c '^State [0-9]*:' "$jw/m.out") states deep)"
		else
			echo "$2: TLC failed:"
			tail -20 "$jw/m.out"
			return 1
		fi
	}
	# livetrace FILE: keepAlive traces put wrong: TraceLive.tla must reject each.
	livetrace() {
		st=0
		live "$PWD/$1" d >"$jw/m.out" 2>&1 || st=$?
		case $(basename "$1"):$st in
		live-ok*:0 | live-[!o]*:[1-9]*) echo "$(basename "$1"): as expected" ;;
		*)
			echo "$(basename "$1"): not as expected (exit $st)"
			return 1
			;;
		esac
	}
	# linkfixture FILE: a session trace with a bug put in (spec/traces/link-*):
	# Trace.tla must accept link-ok* and reject each other one.
	linkfixture() {
		st=0
		TLC_WORKERS=1 trace "$PWD/$1" >"$jw/m.out" 2>&1 || st=$?
		case $(basename "$1"):$st in
		link-ok*:0) echo "$(basename "$1"): accepted, as expected" ;;
		link-ok*:*) echo "$(basename "$1"): rejected"; grep -E -A6 'rejected at|Error' "$jw/m.out" | head -10; return 1 ;;
		*:0) echo "$(basename "$1"): accepted (a bug the trace check misses)"; return 1 ;;
		*)
			if grep -q 'trace rejected at line\|Invariant .* is violated' "$jw/m.out"; then
				echo "$(basename "$1"): rejected, as expected"
			else
				echo "$(basename "$1"): TLC failed:"; tail -10 "$jw/m.out"; return 1
			fi
			;;
		esac
	}
	# joinfixture FILE: a joint's trace with a bug put in (spec/traces/join-*):
	# TraceJoin.tla must accept join-ok and reject each other one.
	joinfixture() {
		st=0
		TLC_WORKERS=1 jtrace "$PWD/$1" >"$jw/m.out" 2>&1 || st=$?
		case $(basename "$1"):$st in
		join-ok.*:0) echo "$(basename "$1"): accepted, as expected" ;;
		join-ok.*:*) echo "$(basename "$1"): rejected"; tail -10 "$jw/m.out"; return 1 ;;
		*:0) echo "$(basename "$1"): accepted (a bug the trace check misses)"; return 1 ;;
		*)
			if grep -q 'Error: TLC threw\|Error: Parsing' "$jw/m.out"; then
				echo "$(basename "$1"): TLC failed:"; tail -10 "$jw/m.out"; return 1
			fi
			echo "$(basename "$1"): rejected, as expected"
			;;
		esac
	}
	# poolfixture FILE: a Pool's trace with a bug put in (spec/traces/pool-*):
	# TracePool.tla must accept pool-ok and reject each other one.
	# pathfixture FILE: a pathswitch session's trace with a bug put in
	# (spec/traces/path-*): TraceDirectPath.tla must accept path-ok* in
	# both directions and reject each other one in at least one.
	pathfixture() {
		pst=0
		for d in c2a a2c; do
			TLC_WORKERS=1 dtrace "$PWD/$1" $d >"$jw/m.out" 2>&1 || pst=1
		done
		case $(basename "$1"):$pst in
		path-ok*:0) echo "$(basename "$1"): accepted, as expected" ;;
		path-ok*:*) echo "$(basename "$1"): rejected"; tail -10 "$jw/m.out"; return 1 ;;
		*:0) echo "$(basename "$1"): accepted (a bug the trace check misses)"; return 1 ;;
		*) echo "$(basename "$1"): rejected, as expected" ;;
		esac
	}
	poolfixture() {
		st=0
		TLC_WORKERS=1 ptrace "$PWD/$1" >"$jw/m.out" 2>&1 || st=$?
		case $(basename "$1"):$st in
		pool-ok.*:0) echo "$(basename "$1"): accepted, as expected" ;;
		pool-ok.*:*) echo "$(basename "$1"): rejected"; tail -10 "$jw/m.out"; return 1 ;;
		*:0) echo "$(basename "$1"): accepted (a bug the trace check misses)"; return 1 ;;
		*)
			if grep -q 'Error: TLC threw\|Error: Parsing' "$jw/m.out"; then
				echo "$(basename "$1"): TLC failed:"; tail -10 "$jw/m.out"; return 1
			fi
			echo "$(basename "$1"): rejected, as expected"
			;;
		esac
	}
	# job CHECK ARGS: CHECK in the background, SPEC_JOBS at once.
	n=0
	job() {
		n=$((n + 1))
		(jw=$work/job$n && mkdir -p "$jw" && META=$jw/states TLC_WORKERS=1 "$@" >"$work/job$n.txt" 2>&1 || touch "$work/job$n.fail") &
		[ $((n % ${SPEC_JOBS:-3})) != 0 ] || wait
	}
	for m in fin-skips-queue grant-early write-race turns-no-recheck release-early abort-as-fin lost-as-eof; do
		job iface Link.cfg $m
	done
	for m in open-after-goaway open-no-recheck; do job iface LinkDrain.cfg $m; done
	for m in abort-forget-early reopen-known; do job iface LinkSess.cfg $m; done
	job iface LinkRefuse.cfg refuse-then-write
	# The drain's cut in its steps (lane LINKATOMIC): the second call
	# returning before the first ended (the code before 7dabd5e), and the
	# cut's ABORT leaving the credit (a Write after it).
	for m in second-cut-early cut-keeps-credit cut-closes-at-once; do
		job mutant LinkCut.cfg 'Mutant = "none"' "Mutant = \"$m\""
	done
	for w in WitnessCutWaits WitnessAbortBeforeCut WitnessCutAbortsEnded CutAbortsAll WitnessShutAnswers; do job witness LinkCut.cfg $w; done
	# An OPEN read after the cut's snapshot (lane LINKOPENREFUSE): run, not
	# refused (the code before: it got no ABORT); the write side shut with
	# a refusal still to buffer.
	for m in open-after-goaway-unanswered shut-before-refusal; do
		job mutant LinkCut.cfg 'Mutant = "none"' "Mutant = \"$m\""
	done
	job witness LinkCut.cfg WitnessDrainRefused
	for w in WitnessEof WitnessReset WitnessLost WitnessCredit; do job witness Link.cfg $w; done
	job witness LinkDrain.cfg WitnessRefused
	# A witness per invariant of Link (spec/README.md "Witnesses").
	for w in WitnessNoCredit WitnessWindowFull WitnessQueueHeld WitnessInFlight; do job witness Link.cfg $w; done
	job witness LinkSess.cfg WitnessOpenAtMax
	job witness LinkDrain.cfg WitnessFinRace
	# Not a guarantee: a clean end at one side while its FIN is lost
	# with the connection (spec/README.md "The session interface").
	job witness Link.cfg CleanEndReceived
	for m in fin-skips-queue grant-early write-race turns-no-recheck release-early; do
		job mutant Link.cfg 'Mutant = "none"' "Mutant = \"$m\""
	done
	for m in open-after-goaway open-no-recheck drop-on-mark; do
		job mutant LinkDrain.cfg 'Mutant = "none"' "Mutant = \"$m\""
	done
	for m in abort-forget-early reopen-known; do
		job mutant LinkSess.cfg 'Mutant = "none"' "Mutant = \"$m\""
	done
	job mutant LinkRefuse.cfg 'Mutant = "none"' 'Mutant = "refuse-then-write"'
	# A CLOSE that ends both halves forgetting the credit only in the drop
	# after it (the code before lane LINKDROPSPLIT): an open past max_sessions.
	job mutant LinkSessHalf.cfg 'Mutant = "none"' 'Mutant = "drop-forgets-late"'
	job witness LinkRefuse.cfg WitnessRefusing
	for m in no-wake-on-goaway drain-past-max open-on-goaway maintain-no-recheck wait-takes-late no-wake-on-dial-fail drain-counts-takes dial-no-closed-check snapshot-before-cancel; do
		job mutant Pool.cfg 'Mutant = "none"' "Mutant = \"$m\"" Pool.tla
	done
	job mutant PoolDrain.cfg 'Mutant = "none"' 'Mutant = "drain-fail-keeps"' Pool.tla
	job mutant Pool.cfg 'Mutant = "none"' 'Mutant = "drain-reserve-late"' Pool.tla
	job mutant PoolGrow.cfg 'Mutant = "none"' 'Mutant = "grow-past-max"' Pool.tla
	job mutant PoolClient.cfg 'Mutant = "none"' 'Mutant = "full-below-max"' Pool.tla
	job mutant PoolClient.cfg 'Mutant = "none"' 'Mutant = "full-with-dials"' Pool.tla
	job mutant PoolClientWake.cfg 'Mutant = "none"' 'Mutant = "no-wake-on-dial-fail"' Pool.tla
	job witness PoolClientWake.cfg WitnessActFull Pool.tla
	job witness PoolClient.cfg WitnessWaitBesideFull Pool.tla
	for w in WitnessOpen WitnessWait WitnessReplaced WitnessPastMax WitnessClosed WitnessBesideLeaving WitnessAtMax; do job witness Pool.cfg $w Pool.tla; done
	# A session moved between the relayed and the direct path (DirectPath.tla,
	# lane DIRECTPATH): a byte freed before R had it, sent before R's RESUME,
	# sent on or read from a stream left, resent from the wrong place, a
	# stream to whoever the relay pointed at, a dead direct path kept; an
	# END before the last byte, a RESUME without the END taken (lane
	# DIRECTPATH2: the END taken twice), a reset sent once only.
	# Lane PSCLOSE: the client leaving before the agent's FIN.
	for m in free-on-send send-before-resume write-stale resume-from-acked read-stale no-bind keep-dead-path end-early resume-drops-fin leave-at-complete; do
		job mutant DirectPath.cfg 'Mutant = "none"' "Mutant = \"$m\"" DirectPath.tla
	done
	job mutant DirectPathReset.cfg 'Mutant = "none"' 'Mutant = "abort-not-resent"' DirectPath.tla
	# Lane DIRECTSTALL: a move before the agent acknowledged the client's
	# handshake (the token's key) stalls the session.
	job mutant DirectPathProof.cfg 'Mutant = "none"' 'Mutant = "move-before-proof"' DirectPath.tla
	# Lane DIRECTREBIND: the agent's address changes; a token kept from the
	# old connection, a re-punch of the old candidates, none, data on both
	# paths (the stale stream kept).
	for m in stale-token repunch-cached no-repunch write-stale; do
		job mutant DirectPathRebind.cfg 'Mutant = "none"' "Mutant = \"$m\"" DirectPath.tla
	done
	for w in WitnessRedirect WitnessRebindRelayed; do job witness DirectPathRebind.cfg $w DirectPath.tla; done
	for w in WitnessDirect WitnessFallback WitnessResent WitnessRogue WitnessFull WitnessWithheld WitnessEndResent WitnessLeftAnswer; do job witness DirectPath.cfg $w DirectPath.tla; done
	for w in WitnessResetAcross WitnessResetDirect; do job witness DirectPathReset.cfg $w DirectPath.tla; done
	job witness DirectPathProof.cfg WitnessDirect DirectPath.tla
	for m in rearm-timeout deadline-from-rx reader-writes never-dead; do
		job mutant Liveness.cfg 'Mutant = "none"' "Mutant = \"$m\"" Liveness.tla
	done
	# a starved process (lane LIVESTARVE): the verdict without a look, the
	# look's re-arm without its gap.
	for m in verdict-unlooked look-no-gap; do
		job mutant LivenessStarve.cfg 'Mutant = "none"' "Mutant = \"$m\"" Liveness.tla
	done
	# quiclink's watch (QuicWatch.tla, lane QLWATCH): the keep-alive
	# stretched by the path's PTO (QW1), the starved gap counted (S1), the
	# idle timeout below the watch's bound (the code before LIVESTARVE),
	# never closing; stops under 1.25 ticks counted (QW2: the watch
	# without quic-go's idle start); a verdict taken at the tick a stop
	# ended, its packet still in the socket (QW4: one look, or a second
	# look with no time for quic-go's run loop).
	job mutant QuicWatchPTO.cfg 'Mutant = "none"' 'Mutant = "quiet-ignores-pto"' QuicWatch.tla
	job mutant QuicWatchStarve.cfg 'Mutant = "none"' 'Mutant = "count-gap"' QuicWatch.tla
	job mutant QuicWatch.cfg 'Mutant = "none"' 'Mutant = "backstop-low"' QuicWatch.tla
	job mutant QuicWatch.cfg 'Mutant = "none"' 'Mutant = "never-close"' QuicWatch.tla
	job mutant QuicWatchStops.cfg 'Mutant = "none"' 'Mutant = "ignores-idle-start"' QuicWatch.tla
	for m in one-look look-at-once; do
		job mutant QuicWatchStops.cfg 'Mutant = "none"' "Mutant = \"$m\"" QuicWatch.tla
	done
	for w in WitnessWatch WitnessTight; do job witness QuicWatch.cfg $w QuicWatch.tla; done
	job witness QuicWatchPTO.cfg WitnessTight QuicWatch.tla
	job witness QuicWatchStarve.cfg WitnessStarvedLive QuicWatch.tla
	job witness QuicWatchStarve.cfg WitnessIdle QuicWatch.tla
	for w in WitnessLateKeepAlive WitnessSecondLook; do job witness QuicWatchStops.cfg $w QuicWatch.tla; done
	# peerlink.Join's clean end (Join.tla, relay-core finding 23).
	for m in no-sync no-hold done-skips-hold no-watch ping-late leave-ignores-sync; do
		job mutant Join.cfg 'Mutant = "none"' "Mutant = \"$m\"" Join.tla
	done
	for w in WitnessDead WitnessLateAlive WitnessTight; do job witness Liveness.cfg $w Liveness.tla; done
	job witness LivenessStarve.cfg WitnessStarved Liveness.tla
	for w in WitnessClean WitnessReset WitnessHeld WitnessCleanDown WitnessDrained; do job witness Join.cfg $w Join.tla; done
	# A joint's onward side closed whole while the origin writes
	# (JoinClose.tla, nfltr lane REFUSALFLAKE): closing the origin at the
	# failed write drops the other direction's tail (the code before);
	# leaving it to a half close leaves the origin half open.
	for m in close-at-write-fail no-whole; do
		job mutant JoinClose.cfg 'Mutant = "none"' "Mutant = \"$m\"" JoinClose.tla
	done
	for w in WitnessFailFirst WitnessHalfThenWhole; do job witness JoinClose.cfg $w JoinClose.tla; done
	# Credit's waiters (CreditWaiters.tla, lane LINKWAKE): the one-slot
	# wake (the code before) strands a waiter and leaves its Acquire for
	# good; passing the token on strands one that needs less (a stall
	# until the next grant).
	for m in one-slot pass-on; do
		job mutant CreditWaiters.cfg 'Mutant = "none"' "Mutant = \"$m\"" CreditWaiters.tla
	done
	job mutant CreditWaitersLive.cfg 'Mutant = "none"' 'Mutant = "one-slot"' CreditWaiters.tla
	for w in WitnessTwoWait WitnessTooLittle WitnessWakeAll; do job witness CreditWaiters.cfg $w CreditWaiters.tla; done
	for f in spec/traces/*.ndjson.live; do job livetrace "$f"; done
	for f in spec/traces/link-*.ndjson; do job linkfixture "$f"; done
	for f in spec/traces/join-*.ndjson; do job joinfixture "$f"; done
	for f in spec/traces/pool-*.ndjson; do job poolfixture "$f"; done
	for f in spec/traces/path-*.ndjson; do job pathfixture "$f"; done
	wait
	fail=0
	i=0
	while [ $i -lt $n ]; do
		i=$((i + 1))
		cat "$work/job$i.txt"
		[ ! -e "$work/job$i.fail" ] || fail=1
	done
	exit $fail
	;;
traces)
	tests=${TRACE_TESTS:-'^(TestSessionCleanEnd|TestSessionReset|TestMuxSlowSessionIsolated|TestGoAwayDrainKeepsTails|TestCrossedOpenRunsOnTheLeavingConnection|TestOpenReadAfterTheCutsSnapshotIsRefused|TestMuxV2GoAway|TestSessionCutWithItsConnection|TestMuxSessions|TestMuxV2Sessions|TestWriteRacingCloseWriteSendsNothingAfterFIN|TestWriteParkedForItsTurnSendsNothingAfterFIN|TestOpenParkedPastPeerGoAwayIsNotSent|TestAbortAtMaxSessionsRacingOpenKeepsTheConnection|TestChainRefuse|TestRefusedOpenReadsReset|TestWriteAfterRefuseSendsNoData|TestCloseWriteRacingThePeersFINDropsAfterItsCLOSE|TestMuxOpenSkipsAnIDThePeerTook|TestReceiveLoopReadsWhileARefusalWaits|TestCloseWriteReportsAFailedFIN|TestStarvedProcessKeepsALivePeer|TestDrainReturnsAfterTheCutItRaced|TestDrainDeadlineAbortsAHeldSession|TestDrainDeadlineKeepsAnEndedSessionsClose|TestChainDrainMiddleHop|TestCloseWriteEndingBothHalvesFreesItsSessionAtOnce)$'}
	mkdir -p "$work/traces"
	LINKTRACE=$work/traces go test -tags linktrace -count=1 -run "$tests" . >"$work/test.out" 2>&1 || {
		cat "$work/test.out"
		exit 1
	}
	# The dead-peer tests cut the path with a proxy (the two sides' addresses
	# differ): one subtest at a time, its connection in one file.
	for t in ${TRACE_DEAD_TESTS-TestDeadPeerWorstCaseIdle/fast/dialer TestDeadPeerWorstCaseIdle/fast/listener TestDeadPeerBusy/fast/down TestDeadPeerBusy/fast/up}; do
		LINKTRACE_FILE=$work/traces/$(echo "$t" | tr / -).ndjson go test -tags linktrace -count=1 -run "^$t\$" . >"$work/test.out" 2>&1 || {
			cat "$work/test.out"
			exit 1
		}
	done
	# peerlink's joints (spec/TraceJoin.tla) and their link connections
	# (Trace.tla, beside the others).
	mkdir -p "$work/ptraces"
	LINKTRACE=$work/ptraces go test -tags linktrace -count=1 -run "${TRACE_JOIN_TESTS:-^TestJoin}" ./peerlink/ >"$work/test.out" 2>&1 || {
		cat "$work/test.out"
		exit 1
	}
	for t in "$work"/ptraces/*; do
		[ -e "$t" ] || continue
		case $(basename "$t") in
		join-*) ;;
		pool-*) mkdir -p "$work/pool-peerlink" && mv "$t" "$work/pool-peerlink/" ;; # TracePool.tla's, below
		*) mv "$t" "$work/traces/peerlink-$(basename "$t")" ;;
		esac
	done
	for t in "$work"/ptraces/join-*.ndjson; do
		[ -e "$t" ] || continue
		if TLC_WORKERS=1 jtrace "$t" >"$t.out" 2>&1; then
			echo "join $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: accepted"
		else
			echo "join $(basename "$t"): REJECTED"
			grep -E -B2 -A6 'unknown event|Error' "$t.out" | head -12
			exit 1
		fi
	done
	cat "$work"/ptraces/join-*.ndjson >"$work/jall" 2>/dev/null || :
	for e in Copy CloseWrite Fin Synced Finish Lost; do printf 'join %s %s; ' "$e" "$(grep -c "\"ev\":\"$e\"" "$work/jall" || true)"; done
	echo
	# The Pool's steps (spec/TracePool.tla), one test at a time: a test's
	# Pools are numbered from 1 in its own directory.
	for t in ${TRACE_POOL_TESTS-TestPoolOpenPassesABrokenConnection TestPoolMinReplacesAConnectionGoingAway TestPoolDrainStaysWithinMax TestPoolOpenFailsWithTheDialErrorWhileMaintainDials TestPoolMinRedialsAfterAnOpensDialFails TestPoolDrainReplacesAFullConnectionFirst TestPoolDrainFailingHalfwayStaysWithinMax TestPoolDrainReservesWithItsSnapshot TestPoolOpenFullAndClosed TestPool TestPoolAccept TestPoolMin ExampleNewPool}; do
		d=$work/pool-$t
		mkdir -p "$d"
		LINKTRACE=$d go test -tags linktrace -count=1 -run "^$t\$" . >"$work/test.out" 2>&1 || {
			cat "$work/test.out"
			exit 1
		}
		for f in "$d"/pool-*.ndjson; do
			[ -e "$f" ] || continue
			if TLC_WORKERS=1 ptrace "$f" >"$f.out" 2>&1; then
				echo "pool $t $(basename "$f"): $(wc -l <"$f" | tr -d ' ') lines: accepted"
			else
				echo "pool $t $(basename "$f"): REJECTED"
				grep -E -B2 -A6 'unknown|Error' "$f.out" | head -12
				exit 1
			fi
		done
	done
	for f in "$work"/pool-peerlink/pool-*.ndjson; do # peerlink's Pools (TestJoin, above)
		[ -e "$f" ] || continue
		if TLC_WORKERS=1 ptrace "$f" >"$f.out" 2>&1; then
			echo "pool peerlink $(basename "$f"): $(wc -l <"$f" | tr -d ' ') lines: accepted"
		else
			echo "pool peerlink $(basename "$f"): REJECTED"
			grep -E -B2 -A6 'unknown|Error' "$f.out" | head -12
			exit 1
		fi
	done
	cat "$work"/pool-*/pool-*.ndjson >"$work/pall" 2>/dev/null || :
	for e in Start Pick Place End MaintainWait MaintainDial Dial PeerGoAway Down Remove DrainStart GoAway CloseStart CloseEnd; do printf 'pool %s %s; ' "$e" "$(grep -c "\"ev\":\"$e\"" "$work/pall" || true)"; done
	echo
	for k in place dial full wait grow closed; do printf 'pick %s %s; ' "$k" "$(grep -c "\"ev\":\"Pick\".*\"t\":\"$k\"\|\"t\":\"$k\".*\"ev\":\"Pick\"" "$work/pall" || true)"; done
	echo
	# keepAlive's steps (<trace>.ndjson.live), each side's.
	for t in "$work"/traces/*.ndjson.live; do
		[ -e "$t" ] || continue
		for side in d l; do
			grep -q "\"side\":\"$side\"" "$t" || continue
			if live "$t" $side >"$t.$side.out" 2>&1; then
				echo "live $(basename "$t") $side: $(grep -c "\"side\":\"$side\"" "$t") lines: accepted"
			else
				echo "live $(basename "$t") $side: REJECTED"
				grep -E -A6 'rejected at|Error' "$t.$side.out" | head -12
				exit 1
			fi
		done
	done
	# TLC_WORKERS traces at once, one TLC worker each.
	n=0
	for t in "$work"/traces/*.ndjson; do
		n=$((n + 1))
		(TLC_WORKERS=1 trace "$t" >"$t.out" 2>&1 && touch "$t.ok") &
		[ $((n % ${TLC_WORKERS:-4})) != 0 ] || wait
	done
	wait
	bad=0
	for t in "$work"/traces/*.ndjson; do
		if [ -f "$t.ok" ]; then
			echo "trace $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: accepted"
		else
			bad=$((bad + 1))
			echo "trace $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: REJECTED"
			grep -E -A12 'rejected at|Error' "$t.out" | head -24
			[ -z "${TRACE_KEEP:-}" ] || cp "$t" "$TRACE_KEEP/"
		fi
	done
	# The path layer (pathswitch, lane DIRECTPATH2): each session's trace,
	# both directions, against DirectPath.tla (TraceDirectPath.tla).
	mkdir -p "$work/dtraces"
	LINKTRACE=$work/dtraces go test -tags linktrace -count=1 -run "${TRACE_PATHSWITCH_TESTS:-^(TestInOrderRelayed|TestInOrderAcrossMoves|TestNoLossDirectDies|TestDirectAgainAfterRebind|TestAllDeliveredRelayRestart|TestEndOnceResentAcrossSwitch|TestResetReachesFromClient|TestResetReachesFromServer|TestStaleEpochRefused|TestCleanClose|TestCloseAfterReattachEndsBeforeTheBound|TestCloseAfterReattachPathDiesBeforeTheFIN|TestCloseLeftAnswersAReattach|TestWaitAckedThenMove)\$}" ./pathswitch/ >"$work/test.out" 2>&1 || {
		cat "$work/test.out"
		exit 1
	}
	for t in "$work"/dtraces/*.ndjson; do
		for d in c2a a2c; do
			n=$((n + 1))
			if TLC_WORKERS=1 dtrace "$t" $d >"$t.$d.out" 2>&1; then
				echo "trace $(basename "$t") $d: $(wc -l <"$t" | tr -d ' ') lines: accepted"
			else
				bad=$((bad + 1))
				echo "trace $(basename "$t") $d: $(wc -l <"$t" | tr -d ' ') lines: REJECTED"
				grep -E 'Error|unknown event' "$t.$d.out" | head -5
				[ -z "${TRACE_KEEP:-}" ] || cp "$t" "$TRACE_KEEP/"
			fi
		done
	done
	cat "$work"/dtraces/*.ndjson >"$work/dall"
	gap=
	for e in $(grep -o 'E.ev = "[A-Za-z]*"' spec/TraceDirectPath.tla | sed 's/.*"\(.*\)"/\1/' | sort -u); do
		k=$(grep -c "\"ev\":\"$e\"" "$work/dall" || true)
		[ "$k" != 0 ] || gap="$gap $e"
		printf '%s %s; ' "$e" "$k"
	done
	echo
	echo "path layer events no trace took:${gap:- none}"
	echo "$n traces, $bad rejected"
	# Action coverage: each event Trace.tla maps (a step of Link), how many
	# times the traces took it; one never taken is a test gap.
	cat "$work"/traces/*.ndjson >"$work/all"
	gap=
	for e in $(grep -o 'E.ev = "[A-Za-z]*"' spec/Trace.tla | sed 's/.*"\(.*\)"/\1/' | sort -u); do
		k=$(grep -c "\"ev\":\"$e\"" "$work/all" || true)
		[ "$k" != 0 ] || gap="$gap $e"
		printf '%s %s; ' "$e" "$k"
	done
	for t in open data win accept close goaway ping pong; do
		printf 'Recv %s %s; ' "$t" "$(grep -c "\"ev\":\"Recv\".*\"t\":\"$t\"\|\"t\":\"$t\".*\"ev\":\"Recv\"" "$work/all" || true)"
	done
	for k in 4 5; do # 4 a refusal's, 5 the cut's (Mux.cut)
		printf 'Close %s %s; Forget %s %s; ' $k "$(grep -c "\"ev\":\"Close\".*\"n\":$k,\\|\"n\":$k,.*\"ev\":\"Close\"" "$work/all" || true)" $k "$(grep -c "\"ev\":\"Forget\".*\"n\":$k,\\|\"n\":$k,.*\"ev\":\"Forget\"" "$work/all" || true)"
	done
	echo
	echo "events no trace took:${gap:- none}"
	[ "$bad" = 0 ]
	;;
behaviours)
	# Behaviours for the model-based tests (spec/README.md "Model-based
	# tests"): TLC -simulate walks, MBT_NUM (default 4) per configuration,
	# seeded (MBT_SEED, default 1), into MBT_OUT (default testdata/mbt):
	# link-<cfg>-<n>.ndjson (MBTLink.tla) and live-<n>.ndjson (MBTLive.tla).
	# The Go tests replay every file there (credit_mbt_test.go,
	# liveness_mbt_test.go); MBT_DIR points them at another directory.
	out=$(cd "${MBT_OUT:-testdata/mbt}" 2>/dev/null && pwd || { mkdir -p "${MBT_OUT:-testdata/mbt}" && cd "${MBT_OUT:-testdata/mbt}" && pwd; })
	gen() { # gen CFG MODULE NAME DEPTH [sed expr] [walks]
		mkdir -p "$work/gen"
		rm -f "$work/gen"/*.ndjson
		sed -e '/^PROPERTY/,$d' -e '/^INVARIANT/,$d' -e "${5:-s/^\$//}" -e "s|^CONSTANTS|CONSTANTS\n    MBTDir = \"$work/gen\"|" "spec/$1" >"$work/gen.cfg"
		echo 'ACTION_CONSTRAINT MBTDump' >>"$work/gen.cfg"
		# -deadlock: a walk may end anywhere (LinkData's sessions never end).
		TLC_WORKERS=1 tlc "$work/gen.cfg" "$2" -simulate "num=${6:-${MBT_NUM:-4}}" -depth "$4" -seed "${MBT_SEED:-1}" -deadlock >"$work/gen.out" 2>&1 || {
			tail -30 "$work/gen.out"
			exit 1
		}
		for f in "$work"/gen/*.ndjson; do
			b=$(basename "$f" .ndjson)
			mv "$f" "$out/$3-${b#*-}.ndjson"
		done
		echo "$3: $(ls "$out" | grep -c "^$3-") behaviours"
	}
	gen Link.cfg MBTLink.tla link-Link 120 's/Crash = 1/Crash = 0/'
	# Link.cfg without closes: every walk moves its 6 bytes through the
	# window (reads, grants, window updates).
	gen Link.cfg MBTLink.tla link-LinkData 120 's/Crash = 1/Crash = 0/;s/Ends = {0, 1, 2}/Ends = {}/'
	gen LinkCrash.cfg MBTLink.tla link-LinkCrash 80
	gen LinkSess.cfg MBTLink.tla link-LinkSess 120
	gen LinkRefuse.cfg MBTLink.tla link-LinkRefuse 120
	gen LinkDrain.cfg MBTLink.tla link-LinkDrain 120
	gen Liveness.cfg MBTLive.tla live 60 '' $((${MBT_NUM:-4} * 8))
	gen LivenessStarve.cfg MBTLive.tla live-starve 60 '' $((${MBT_NUM:-4} * 8))
	# quiclink's watch (MBTWatch.tla; peerlink/quiclink/watch_mbt_test.go)
	for c in QuicWatch QuicWatchPTO QuicWatchStarve QuicWatchStops; do
		gen $c.cfg MBTWatch.tla qwatch-$c 300 '' $((${MBT_NUM:-4} * 4))
	done
	;;
trace)
	trace "$2"
	;;
jtrace)
	jtrace "$2"
	;;
dtrace)
	dtrace "$2" "$3"
	;;
*)
	echo "usage: spec.sh check|invariants|nightly|mutants|traces|trace FILE" >&2
	exit 2
	;;
esac
