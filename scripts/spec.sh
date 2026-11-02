#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# spec.sh check|mutants|traces: the TLA+ model of spec/ (spec/README.md).
#   check    TLC on spec/Link.cfg and spec/LinkDrain.cfg, then spec/Pool.cfg
#            and spec/Liveness.cfg: every invariant and liveness property.
#   invariants  the same, Link's invariants only (about a minute: CI's;
#            Pool's liveness is checked too, seconds).
#   mutants  each Mutant of Link.tla, Pool.tla and Liveness.tla must be
#            caught (a TLC violation).
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
	cat >"$work/live.cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Side = "$2"
INIT TraceInit
NEXT TraceNext
POSTCONDITION TraceAccepted
INVARIANT TraceNeverEarly
CFG
	META=$work/states-live-$(basename "$1")-$2 TLC_WORKERS=1 tlc "$work/live.cfg" TraceLive.tla -deadlock
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
INIT TraceInit
NEXT TraceNext
POSTCONDITION TraceAccepted
INVARIANTS
    NoProtocolError
    CreditBounds
    BufferBound
    Conservation
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" Trace.tla -deadlock
}

case ${1:-} in
check | invariants)
	for cfg in Link.cfg LinkDrain.cfg; do
		echo "== $cfg ($1)"
		if [ "$1" = invariants ]; then
			sed '/^PROPERTIES/,$d' "spec/$cfg" >"$work/$cfg"
			cfg=$work/$cfg
		fi
		st=0
		tlc "$cfg" MCLink.tla >"$work/check.out" 2>&1 || st=$?
		grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/check.out" || true
		if [ $st != 0 ]; then
			tail -60 "$work/check.out"
			exit $st
		fi
	done
	for m in Pool Liveness; do
		echo "== $m.cfg"
		st=0
		# Liveness explores a horizon of ticks: its last states end there.
		dl=; [ $m = Pool ] || dl=-deadlock
		tlc "$m.cfg" "$m.tla" $dl >"$work/check.out" 2>&1 || st=$?
		grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/check.out" || true
		[ $st = 0 ] || exit $st
	done
	;;
mutants)
	# mutant CFG NAME [MODULE]: TLC must report a violation (Link: of an
	# invariant; Pool and Liveness keep their properties).
	fail=0
	mutant() {
		mod=${4:-MCLink.tla}
		if [ "$mod" = MCLink.tla ]; then
			sed -e "s/$2/$3/" -e '/^PROPERTIES/,$d' "spec/$1" >"$work/Mutant.cfg"
		else
			sed -e "s/$2/$3/" "spec/$1" >"$work/Mutant.cfg"
		fi
		dl=; [ "$mod" != Liveness.tla ] || dl=-deadlock
		if tlc "$work/Mutant.cfg" "$mod" $dl >"$work/m.out" 2>&1; then
			echo "$3: not caught"
			fail=1
		elif grep -q '^Error: \(Invariant\|Temporal property\) .* violated' "$work/m.out"; then
			echo "$3: caught: $(grep '/\\ err = ' "$work/m.out" | tail -1 | sed 's/.*err = //') ($(grep -m1 -E '^Error: (Invariant|Temporal property)' "$work/m.out" | sed -E 's/^Error: (Invariant|Temporal property) ([A-Za-z]+).*/\2/'), $(grep -c '^State [0-9]*:' "$work/m.out") states deep)"
		else
			echo "$3: TLC failed:"
			tail -20 "$work/m.out"
			fail=1
		fi
	}
	mutant Link.cfg 'Mutant = "none"' 'Mutant = "fin-skips-queue"'
	mutant Link.cfg 'Mutant = "none"' 'Mutant = "grant-early"'
	mutant LinkDrain.cfg 'Mutant = "none"' 'Mutant = "open-after-goaway"'
	mutant Link.cfg 'Mutant = "none"' 'Mutant = "write-race"'
	for m in no-wake-on-goaway drain-past-max open-on-goaway maintain-no-recheck; do
		mutant Pool.cfg 'Mutant = "none"' "Mutant = \"$m\"" Pool.tla
	done
	for m in rearm-timeout deadline-from-rx reader-writes; do
		mutant Liveness.cfg 'Mutant = "none"' "Mutant = \"$m\"" Liveness.tla
	done
	# keepAlive traces put wrong: TraceLive.tla must reject each.
	for f in spec/traces/*.ndjson.live; do
		st=0
		live "$PWD/$f" d >"$work/m.out" 2>&1 || st=$?
		case $(basename "$f"):$st in
		live-ok.*:0 | live-*:[1-9]*) echo "$(basename "$f"): as expected" ;;
		*)
			echo "$(basename "$f"): not as expected (exit $st)"
			fail=1
			;;
		esac
	done
	exit $fail
	;;
traces)
	tests=${TRACE_TESTS:-'^(TestSessionCleanEnd|TestSessionReset|TestMuxSlowSessionIsolated|TestGoAwayDrainKeepsTails|TestCrossedOpenRunsOnTheLeavingConnection|TestMuxV2GoAway|TestSessionCutWithItsConnection|TestMuxSessions|TestMuxV2Sessions|TestWriteRacingCloseWriteSendsNothingAfterFIN)$'}
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
	echo "usage: spec.sh check|invariants|mutants|traces|trace FILE" >&2
	exit 2
	;;
esac
