#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# spec.sh check|mutants|traces: the TLA+ model of spec/ (spec/README.md).
#   check    TLC on spec/Link.cfg, LinkDrain.cfg and LinkSess.cfg (no crash),
#            LinkCrash.cfg, then spec/Pool.cfg, Liveness.cfg and Join.cfg:
#            every invariant and liveness property, and Refines.
#   invariants  the same, Link's invariants only, Refines only on
#            LinkCrash.cfg (CI's; Pool's liveness is checked too, seconds).
#   nightly  Link.cfg with its crash: invariants and Refines.
#   mutants  each Mutant of Link.tla, Pool.tla, Liveness.tla and Join.tla must be
#            caught (a TLC violation); each Mutant of Link.tla must also
#            fail the session interface alone (Refines, LinkSession.tla),
#            each witness must be violated, and each trace fixture of
#            spec/traces/ (a recorded run with a bug put in) rejected.
#   traces   the tests in TRACE_TESTS and TRACE_DEAD_TESTS run with -tags
#            linktrace, each connection's trace checked against the model
#            (spec/Trace.tla).
#   trace F  one recorded trace.
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
    Ends = {0, 1, 2, 3}
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
	# Link.cfg with its crash: invariants and Refines (20 million states;
	# its liveness with the crash is past any bound here).
	echo "== Link.cfg (crash, invariants and Refines)"
	sed -e '/^PROPERTIES/,$d' spec/Link.cfg >"$work/Link.cfg"
	tlcrun "$work/Link.cfg"
	# The Pool wider than Pool.cfg (review r2 L5): two Opens, Min below Max.
	echo "== PoolWide.cfg"
	st=0
	tlc PoolWide.cfg Pool.tla >"$work/check.out" 2>&1 || st=$?
	grep -E '^[0-9]+ states generated|Finished in|Error|violated' "$work/check.out" || true
	[ $st = 0 ] || { tail -60 "$work/check.out"; exit $st; }
	;;
check | invariants)
	for cfg in Link.cfg LinkDrain.cfg LinkSess.cfg LinkCrash.cfg LinkRefuse.cfg; do
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
		# LinkCrash.cfg and LinkRefuse.cfg (seconds): Refines and liveness in CI too.
		[ $cfg != LinkCrash.cfg ] && [ $cfg != LinkRefuse.cfg ] || cp spec/$cfg "$work/$cfg"
		tlcrun "$work/$cfg"
	done
	# PoolDrain (Min 2: a Drain failing halfway) only in full: its
	# mutant is CI's.
	pools="Pool Liveness Join"
	[ "$1" = invariants ] || pools="$pools PoolDrain"
	for m in $pools; do
		echo "== $m.cfg"
		st=0
		# Liveness explores a horizon of ticks, Join one session: their last
		# states end there; PoolDrain has no Open to keep it going.
		dl=; [ $m = Pool ] || dl=-deadlock
		mod=$m.tla; [ $m != PoolDrain ] || mod=Pool.tla
		tlc "$m.cfg" "$mod" $dl >"$work/check.out" 2>&1 || st=$?
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
		dl=; [ "$mod" != Liveness.tla ] && [ "$mod" != Join.tla ] && [ "$1" != PoolDrain.cfg ] || dl=-deadlock
		if tlc "$jw/Mutant.cfg" "$mod" $dl >"$jw/m.out" 2>&1; then
			echo "$3: not caught"
			return 1
		elif grep -q '^Error: \(Invariant\|Temporal property\|Action property\) .* violated' "$jw/m.out"; then
			echo "$3: caught: $(grep '/\\ err = ' "$jw/m.out" | tail -1 | sed 's/.*err = //') ($(grep -m1 -E '^Error: (Invariant|Temporal property)' "$jw/m.out" | sed -E 's/^Error: (Invariant|Temporal property) ([A-Za-z]+).*/\2/'), $(grep -c '^State [0-9]*:' "$jw/m.out") states deep)"
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
		printf 'INVARIANT %s\n' "$2" >>"$jw/Witness.cfg"
		dl=; [ "${3:-}" != Join.tla ] && [ "${3:-}" != Liveness.tla ] || dl=-deadlock
		if tlc "$jw/Witness.cfg" "${3:-MCLink.tla}" $dl >"$jw/m.out" 2>&1; then
			echo "$2: never violated (vacuous)"
			return 1
		elif grep -q "^Error: Invariant $2 is violated" "$jw/m.out"; then
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
		live-ok.*:0 | live-*:[1-9]*) echo "$(basename "$1"): as expected" ;;
		*)
			echo "$(basename "$1"): not as expected (exit $st)"
			return 1
			;;
		esac
	}
	# linkfixture FILE: a session trace with a bug put in (spec/traces/link-*):
	# Trace.tla must accept link-ok and reject each other one.
	linkfixture() {
		st=0
		TLC_WORKERS=1 trace "$PWD/$1" >"$jw/m.out" 2>&1 || st=$?
		case $(basename "$1"):$st in
		link-ok.*:0) echo "$(basename "$1"): accepted, as expected" ;;
		link-ok.*:*) echo "$(basename "$1"): rejected"; grep -E -A6 'rejected at|Error' "$jw/m.out" | head -10; return 1 ;;
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
	job witness LinkRefuse.cfg WitnessRefusing
	for m in no-wake-on-goaway drain-past-max open-on-goaway maintain-no-recheck wait-takes-late no-wake-on-dial-fail drain-counts-takes dial-no-closed-check snapshot-before-cancel; do
		job mutant Pool.cfg 'Mutant = "none"' "Mutant = \"$m\"" Pool.tla
	done
	job mutant PoolDrain.cfg 'Mutant = "none"' 'Mutant = "drain-fail-keeps"' Pool.tla
	for w in WitnessOpen WitnessWait WitnessReplaced WitnessPastMax WitnessClosed WitnessBesideLeaving WitnessAtMax; do job witness Pool.cfg $w Pool.tla; done
	for m in rearm-timeout deadline-from-rx reader-writes; do
		job mutant Liveness.cfg 'Mutant = "none"' "Mutant = \"$m\"" Liveness.tla
	done
	# peerlink.Join's clean end (Join.tla, relay-core finding 23).
	for m in no-sync no-hold done-skips-hold; do
		job mutant Join.cfg 'Mutant = "none"' "Mutant = \"$m\"" Join.tla
	done
	for w in WitnessDead WitnessLateAlive WitnessTight; do job witness Liveness.cfg $w Liveness.tla; done
	for w in WitnessClean WitnessReset WitnessHeld WitnessCleanDown; do job witness Join.cfg $w Join.tla; done
	for f in spec/traces/*.ndjson.live; do job livetrace "$f"; done
	for f in spec/traces/link-*.ndjson; do job linkfixture "$f"; done
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
	tests=${TRACE_TESTS:-'^(TestSessionCleanEnd|TestSessionReset|TestMuxSlowSessionIsolated|TestGoAwayDrainKeepsTails|TestCrossedOpenRunsOnTheLeavingConnection|TestMuxV2GoAway|TestSessionCutWithItsConnection|TestMuxSessions|TestMuxV2Sessions|TestWriteRacingCloseWriteSendsNothingAfterFIN|TestWriteParkedForItsTurnSendsNothingAfterFIN|TestOpenParkedPastPeerGoAwayIsNotSent|TestAbortAtMaxSessionsRacingOpenKeepsTheConnection)$'}
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
	echo "$n traces, $bad rejected"
	[ "$bad" = 0 ]
	;;
trace)
	trace "$2"
	;;
*)
	echo "usage: spec.sh check|invariants|nightly|mutants|traces|trace FILE" >&2
	exit 2
	;;
esac
