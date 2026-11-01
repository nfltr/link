#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# spec.sh check|mutants|traces: the TLA+ model of spec/ (spec/README.md).
#   check    TLC on spec/Link.cfg and spec/LinkDrain.cfg: every invariant and
#            liveness property.
#   mutants  each Mutant of Link.tla must be caught (a TLC violation).
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
    WriteRace = TRUE
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
check)
	for cfg in Link.cfg LinkDrain.cfg; do
		echo "== $cfg"
		st=0
		tlc $cfg MCLink.tla >"$work/check.out" 2>&1 || st=$?
		grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/check.out"
		if [ $st != 0 ]; then
			tail -60 "$work/check.out"
			exit $st
		fi
	done
	;;
mutants)
	# mutant CFG NAME: TLC must report an invariant violation.
	fail=0
	mutant() {
		sed -e "s/$2/$3/" -e '/^PROPERTIES/,$d' "spec/$1" >"$work/Mutant.cfg"
		if tlc "$work/Mutant.cfg" MCLink.tla >"$work/m.out" 2>&1; then
			echo "$3: not caught"
			fail=1
		elif grep -q '^Error: Invariant .* is violated' "$work/m.out"; then
			echo "$3: caught: $(grep '/\\ err = ' "$work/m.out" | tail -1 | sed 's/.*err = //') ($(grep -m1 '^Error: Invariant' "$work/m.out" | cut -d' ' -f3), $(grep -c '^State [0-9]*:' "$work/m.out") states deep)"
		else
			echo "$3: TLC failed:"
			tail -20 "$work/m.out"
			fail=1
		fi
	}
	mutant Link.cfg 'Mutant = "none"' 'Mutant = "fin-skips-queue"'
	mutant Link.cfg 'Mutant = "none"' 'Mutant = "grant-early"'
	mutant LinkDrain.cfg 'Mutant = "none"' 'Mutant = "open-after-goaway"'
	mutant Link.cfg 'WriteRace = FALSE' 'WriteRace = TRUE'
	exit $fail
	;;
traces)
	tests=${TRACE_TESTS:-'^(TestSessionCleanEnd|TestSessionReset|TestMuxSlowSessionIsolated|TestGoAwayDrainKeepsTails|TestCrossedOpenRunsOnTheLeavingConnection|TestMuxV2GoAway|TestSessionCutWithItsConnection|TestMuxSessions|TestMuxV2Sessions)$'}
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
	echo "usage: spec.sh check|mutants|traces|trace FILE" >&2
	exit 2
	;;
esac
