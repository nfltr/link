#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# spec.sh check|mutants|traces: the TLA+ model of spec/ (spec/README.md).
#   check    TLC on spec/Link.cfg, LinkDrain.cfg and LinkSess.cfg (no crash),
#            LinkCrash.cfg, then spec/Pool.cfg, Liveness.cfg, Join.cfg,
#            JoinClose.cfg and CreditWaiters.cfg, CreditGrow.cfg, CreditGrowOne.cfg, SessionRead.cfg, SessionReadWriteTo.cfg, SessionReadPump.cfg, SessionReadPumpWriteTo.cfg, SessionReadWriteTos.cfg, SessionWrite.cfg, DirectPath.cfg, DirectPathGrow.cfg, RelayedClose.cfg, RelayedCloseEmpty.cfg, HopHold.cfg, HopHoldSeq.cfg, HopHoldConc.cfg, EndAckLeg.cfg, EndAckLegQuic.cfg, AutoWindow.cfg:
#            every invariant and liveness property, and Refines.
#   invariants  the same, Link's invariants only, Refines only on
#            LinkCrash.cfg (CI's; Pool's liveness is checked too, seconds).
#   nightly  Link.cfg with its crash: invariants and Refines.
#   nightly-wide  the configurations wider than make spec's: PoolWide.cfg,
#            PoolClient.cfg with a third slot, LinkBoth.cfg, LinkCutHalf.cfg.
#   mutants  each Mutant of Link.tla, Pool.tla, Liveness.tla, Join.tla, JoinClose.tla, RelayedClose.tla, HopHold.tla and EndAckLeg.tla must be
#            caught (a TLC violation); each Mutant of Link.tla must also
#            fail the session interface alone (Refines, LinkSession.tla),
#            each witness must be violated, and each trace fixture of
#            spec/traces/ (a recorded run with a bug put in) rejected.
#   traces   the tests in TRACE_TESTS and TRACE_DEAD_TESTS run with -tags
#            linktrace, each connection's trace checked against the model
#            (spec/Trace.tla).
#   trace F  one recorded trace (rtrace F: a session's reads; wtrace F: a
#            session credit's waiters; swtrace F: a session's writes; gtrace F:
#            a side's growing windows; jctrace F:
#            a joint's side closed whole; rctrace F: a relayed path's Close;
#            hhtrace F: two joints' holds; aftrace F: a QUIC receiver's ACKs;
#            artrace F: its ACK ranges; awtrace F: a QUIC stream's auto-tuned window).
#   behaviours  TLC -simulate walks for the model-based tests (testdata/mbt).
# TLC_WORKERS (default 4) caps TLC's threads. Java and tla2tools.jar come
# from scripts/fetch-tla.sh.
set -eu
cd "$(dirname "$0")/.."
tools=$(scripts/fetch-tla.sh)
java=$(echo "$tools" | sed -n 1p)
jar=$(echo "$tools" | sed -n 2p)
work=$(mktemp -d)
# TLC runs go through scripts/cicache.sh: kept only when this script passes.
cicache=$PWD/scripts/cicache.sh
CICACHE_STAGE=$("$cicache" begin || true)
export CICACHE_STAGE
trap 'st=$?; "$cicache" end "$CICACHE_STAGE" $st || st=1; rm -rf "$work"; exit $st' EXIT

# tlc CFG MODULE [args]: TLC in spec/, its state files in $META (default $work/states).
# The heap: TLC_RAM_PERCENT (scripts/fetch-tla.sh's java) of the job's
# memory, 60 by default: Link.cfg's liveness (make spec) ran out of Java's
# default 25% of a runner's 8 GB once LINKABORT's "here" added 10% of its
# states. A caller running TLCs at once sets TLC_JVMS, so they share it.
tlc() {
	c=$1 mod=$2
	shift 2
	(cd spec && TLC_RAM_PERCENT=${TLC_RAM_PERCENT:-60} "$cicache" tlc -- "$java" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-4}" \
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

# jctrace FILE: TLC on one joint's recorded trace of a side closed whole
# (spec/TraceJoinClose.tla, lane LINKTRACE2); it passes when TLC reaches
# the trace's end (TraceUnfinished violated).
jctrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    K <- TraceK
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    TailDelivered
    TraceUnfinished
CFG
	jcst=0 # not st: the callers'
	META=$work/states-$(basename "$1") tlc "$cfg" TraceJoinClose.tla -deadlock >"$cfg.out" 2>&1 || jcst=$?
	cat "$cfg.out"
	[ $jcst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# rctrace FILE: TLC on one relayed path's recorded trace of a Close
# (spec/TraceRelayedClose.tla, lane ENDACKCODE); it passes when TLC
# reaches the trace's end (TraceUnfinished violated).
rctrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    K = 1
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    NoSilentLoss
    CloseTruth
    TraceUnfinished
CFG
	rcst=0 # not st: the callers'
	META=$work/states-$(basename "$1") tlc "$cfg" TraceRelayedClose.tla -deadlock >"$cfg.out" 2>&1 || rcst=$?
	cat "$cfg.out"
	[ $rcst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# eatrace FILE: TLC on one quiclink session's recorded end handshake
# (spec/TraceEndAckLeg.tla, lane ENDACKQUIC); it fails unless TLC reaches
# the trace's end (TraceUnfinished violated).
eatrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    K = 1
    Kind = "quic"
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    AckTruth
    ErrTruth
    IdleTruth
    NoFalseReset
    TraceUnfinished
CFG
	east=0
	META=$work/states-$(basename "$1") tlc "$cfg" TraceEndAckLeg.tla -deadlock >"$cfg.out" 2>&1 || east=$?
	cat "$cfg.out"
	[ $east != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# aftrace FILE: TLC on one QUIC receiver's recorded ACK decisions
# (spec/TraceAckFrequency.tla, lane LINKSPEED2; the quic-go fork's
# -tags quicacktrace); it fails unless TLC reaches the trace's end
# (TraceUnfinished violated).
aftrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    CountBound
    AlarmOnTime
    ImmediateAcked
    TraceUnfinished
CFG
	afst=0
	META=$work/states-$(basename "$1") tlc "$cfg" TraceAckFrequency.tla -deadlock >"$cfg.out" 2>&1 || afst=$?
	cat "$cfg.out"
	[ $afst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# artrace FILE: TLC on one QUIC receiver's recorded ACK ranges
# (spec/TraceAckRanges.tla, lane SPURIOUS; the fork's -tags quicacktrace,
# its Ack lines' rs); it fails unless TLC reaches the trace's end.
artrace() {
	cfg=$work/$(basename "$1").ar.cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    K = 64
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TraceUnfinished
CFG
	arst=0
	META=$work/states-ar-$(basename "$1") tlc "$cfg" TraceAckRanges.tla -deadlock >"$cfg.out" 2>&1 || arst=$?
	cat "$cfg.out"
	[ $arst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# awtrace FILE: TLC on one QUIC stream direction's recorded window steps
# (spec/TraceAutoWindow.tla, lane LINKLEFT; quiclink's -tags linktrace,
# LINKTRACE_AUTOWIN); it fails unless TLC reaches the trace's end.
awtrace() {
	cfg=$work/$(basename "$1").aw.cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    W0 <- TraceW0
    MaxW <- TraceMaxW
    Total <- TraceTotal
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    Credit
    MemoryBound
    WindowBound
    TraceUnfinished
CFG
	awst=0
	META=$work/states-aw-$(basename "$1") tlc "$cfg" TraceAutoWindow.tla -deadlock >"$cfg.out" 2>&1 || awst=$?
	cat "$cfg.out"
	[ $awst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}

# hhtrace FILE [SEQAPP]: TLC on two joints' recorded holds
# (spec/TraceHopHold.tla, lane LINKTRACE2); it passes when TLC reaches the
# trace's end with every end acknowledged (TraceUnfinished violated).
# SEQAPP, CONCAPP: HopHold.tla's SeqApp and ConcApp (default {}: both
# apps TCP; quiclink's runs, R's app a QUIC far end: a sequential one
# {"R"}, hhtraceseq; one reading beside its CloseWriteSync, hhtraceconc).
hhtrace() {
	cfg=$work/$(basename "$1").cfg
	seq=${2:-}; [ -n "$seq" ] || seq={}
	conc=${3:-}; [ -n "$conc" ] || conc={}
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Mutant = "none"
    SeqApp = $seq
    ConcApp = $conc
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    HoldTruth
    AckTruth
    CloseAfterAck
    TraceUnfinished
CFG
	hhst=0 # not st: the callers'
	META=$work/states-$(basename "$1") tlc "$cfg" TraceHopHold.tla -deadlock >"$cfg.out" 2>&1 || hhst=$?
	cat "$cfg.out"
	[ $hhst != 0 ] && grep -q '^Error: Invariant TraceUnfinished is violated' "$cfg.out"
}
hhtraceseq() { hhtrace "$1" '{"R"}'; }
hhtraceconc() { hhtrace "$1" '{}' '{"R"}'; }

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
	mw=$(head -1 "$1" | sed -n 's/.*"x":"\([0-9][0-9]*\)".*/\1/p') # Config.MaxWindow (lane LINKLEFT)
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Dir = "$2"
    Total = $tot
    W = $(head -1 "$1" | sed 's/.*"n":\([0-9]*\).*/\1/')
    MaxW = ${mw:-$(head -1 "$1" | sed 's/.*"n":\([0-9]*\).*/\1/')}
    MaxDie = $big
    MaxPunch = $big
    MaxMoves = $big
    MaxAbort = 1
    MaxRebind = $big
    Proof = 0
    AppSteps = TRUE
    MaxVanish = 0
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

# rtrace FILE: TLC on one session's recorded reads (spec/TraceSessionRead.tla,
# lane LINKTRACE); it fails unless TLC takes every line.
rtrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Readers <- TraceReaders
    Slices <- TraceSlices
    Copier <- TraceCopier
    WCopier <- TraceWCopier
    Pumps <- TracePumps
    Bytes <- TraceBytes
    SliceOf <- TraceSliceOf
    Mutant = "none"
POSTCONDITION TraceAccepted
INIT TraceInit
NEXT TraceNext
INVARIANTS
    Once
    InOrder
    NoLoss
    ReleaseOnce
    NoUseAfterRelease
    NoForgedEOF
    PumpAlone
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TraceSessionRead.tla -deadlock
}

# wtrace FILE: TLC on one session credit's recorded waiters
# (spec/TraceCreditWaiters.tla, lane LINKTRACE); it fails unless TLC takes
# every line.
wtrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Waiters <- TraceWaiters
    Need <- TraceNeed
    K = 1
    W <- TraceW
    Mutant = "none"
POSTCONDITION TraceAccepted
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    Conservation
    NoStrandedWaiter
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TraceCreditWaiters.tla -deadlock
}

# gtrace FILE: TLC on one receiving side's recorded growing windows
# (spec/TraceCreditGrow.tla, lane RELAYWIN2); it fails unless TLC takes
# every line.
gtrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Sids <- TraceSids
    W <- TraceW
    Max <- TraceMax
    G <- TraceG
    Total = 0
    Q = 4
    Mutant = "none"
POSTCONDITION TraceAccepted
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    NoErr
    TraceConservation
    WinBounds
    Budget
    TraceGrownAccount
    QueueBound
    TraceConnBound
    NeverReadNoGrowth
    NoGrowthAhead
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TraceCreditGrow.tla -deadlock
}

# swtrace FILE: TLC on one session's recorded writes waiting for credit
# (spec/TraceSessionWrite.tla, lane LINKTRACE2); it fails unless TLC takes
# every line.
swtrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Writers <- TraceWriters
    Sets <- TraceSets
    Window <- TraceWindow
    Need <- TraceNeed
    Mutant = "none"
POSTCONDITION TraceAccepted
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    NoStrandedWriter
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TraceSessionWrite.tla -deadlock
}

# tlcrun CFG: TLC on MCLink.tla with CFG, which must pass.
tlcrun() {
	st=0
	tlc "$1" MCLink.tla >"$work/check-$(basename "$1").out" 2>&1 || st=$?
	grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/check-$(basename "$1").out" || true
	if [ $st != 0 ]; then
		tail -60 "$work/check-$(basename "$1").out"
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
	# Link's configurations and the other models run side by side, each
	# group in order with its own state directory (Link.cfg alone takes
	# minutes; the others, most of a second each, would wait behind it);
	# their outputs printed in this order.
	links() {
		for cfg in Link.cfg LinkDrain.cfg LinkSess.cfg LinkSessHalf.cfg LinkCrash.cfg LinkRefuse.cfg LinkCut.cfg; do
			# Not Link.cfg in invariants (CI; lane CIFAST): 4.6 million states,
			# 10 to 16 minutes, the job's long pole. spec-nightly checks it
			# with its crash (every state of this check and more, with Refines),
			# and spec-mutants runs its mutants and witnesses on it in CI.
			[ "$1" != invariants ] || [ $cfg != Link.cfg ] || continue
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
	}
	allmodels="Pool PoolGrow PoolClient Liveness LivenessStarve QuicWatch QuicWatchPTO QuicWatchStarve Join JoinClose CreditWaiters SessionRead SessionReadWriteTo SessionReadPump SessionReadPumpWriteTo SessionReadWriteTos SessionWrite DirectPath DirectPathReset DirectPathRebind DirectPathProof DirectPathClose DirectPathGrow RelayedClose RelayedCloseEmpty HopHold HopHoldSeq HopHoldConc EndAckLeg EndAckLegQuic AckFrequency AckFrequencyDeep CreditGrow CreditGrowOne AutoWindow AckRanges"
	models() {
		# PoolDrain (Min 2: a Drain failing halfway) and QuicWatchStops (two
		# short stops, ~10 s) only in full: their mutants are CI's.
		# PoolGrow: background growth (Max above Min).
		pools=$2
		for m in $pools; do
			echo "== $m.cfg"
			st=0
			# Liveness explores a horizon of ticks, Join one session,
			# CreditWaiters a fixed number of Acquires, SessionRead a fixed number of bytes, SessionWrite a fixed number of SetWriteDeadlines: their last states end there; PoolDrain has no Open to keep it going.
			dl=; [ $m = Pool ] || [ $m = PoolGrow ] || [ $m = PoolClient ] || dl=-deadlock
			mod=$m.tla; case $m in Pool*) mod=Pool.tla ;; Liveness*) mod=Liveness.tla ;; QuicWatch*) mod=QuicWatch.tla ;; CreditWaiters*) mod=CreditWaiters.tla ;; SessionRead*) mod=SessionRead.tla ;; DirectPath*) mod=DirectPath.tla ;; RelayedClose*) mod=RelayedClose.tla ;; HopHold*) mod=HopHold.tla ;; EndAckLeg*) mod=EndAckLeg.tla ;; AckFrequency*) mod=AckFrequency.tla ;; CreditGrow*) mod=CreditGrow.tla ;; esac
			cfg=$m.cfg
			# DirectPath's liveness (AllDelivered, ResetReaches: minutes) only in
			# full; spec-mutants catches keep-dead-path and abort-not-resent by it.
			if [ "$1" = invariants ] && [ "${m#DirectPath}" != "$m" ]; then
				sed -e '/^PROPERTIES/,$d' spec/$m.cfg >"$work/$m.cfg"
				cfg=$work/$m.cfg
			fi
			tlc "$cfg" "$mod" $dl >"$work/models-check-$m.out" 2>&1 || st=$?
			grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/models-check-$m.out" || true
			[ $st = 0 ] || exit $st
		done
	}
	st=0
	[ "$1" = invariants ] || allmodels="$allmodels PoolDrain QuicWatchStops DirectPathWide CreditGrowWide"
	half=$(((${TLC_WORKERS:-4} + 1) / 2))
	# Pool.cfg and PoolGrow.cfg (the models' longest, 7 of their 13
	# minutes on the Mac) a third group beside the other two (lane SPECFAST).
	(META=$work/states-pools TLC_WORKERS=$half models "$1" "Pool PoolGrow") >"$work/pools.out" 2>&1 &
	pp=$!
	(META=$work/states-models TLC_WORKERS=$half models "$1" "${allmodels#Pool PoolGrow }") >"$work/models.out" 2>&1 &
	mp=$!
	(META=$work/states-links links "$1") >"$work/links.out" 2>&1 || st=1
	wait $pp || st=1
	wait $mp || st=1
	cat "$work/links.out" "$work/pools.out" "$work/models.out"
	exit $st
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
			sed -e "s/$2/$3/" -e '/^PROPERTY Refines/d' -e '/^PROPERTIES/,$d' "$(cfgpath "$1")" >"$jw/Mutant.cfg"
		else
			sed -e "s/$2/$3/" "$(cfgpath "$1")" >"$jw/Mutant.cfg"
		fi
		dl=; [ "$mod" != Liveness.tla ] && [ "$mod" != QuicWatch.tla ] && [ "$mod" != Join.tla ] && [ "$mod" != JoinClose.tla ] && [ "$mod" != CreditWaiters.tla ] && [ "$mod" != SessionRead.tla ] && [ "$mod" != SessionWrite.tla ] && [ "$mod" != DirectPath.tla ] && [ "$mod" != RelayedClose.tla ] && [ "$mod" != HopHold.tla ] && [ "$mod" != EndAckLeg.tla ] && [ "$mod" != AckFrequency.tla ] && [ "$mod" != CreditGrow.tla ] && [ "$mod" != AckRanges.tla ] && [ "$1" != PoolDrain.cfg ] || dl=-deadlock
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
		sed -e "s/Mutant = \"none\"/Mutant = \"$2\"/" -e '/^INVARIANTS/,$d' "$(cfgpath "$1")" >"$jw/Mutant.cfg"
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
		sed -e '/^PROPERTY/,$d' "$(cfgpath "$1")" >"$jw/Witness.cfg"
		# WitnessAct*: an action property (checked as a PROPERTY).
		case $2 in WitnessAct*) kind=PROPERTY what="Action property" ;; *) kind=INVARIANT what=Invariant ;; esac
		printf '%s %s\n' "$kind" "$2" >>"$jw/Witness.cfg"
		dl=; [ "${3:-}" != Join.tla ] && [ "${3:-}" != JoinClose.tla ] && [ "${3:-}" != Liveness.tla ] && [ "${3:-}" != QuicWatch.tla ] && [ "${3:-}" != CreditWaiters.tla ] && [ "${3:-}" != SessionRead.tla ] && [ "${3:-}" != SessionWrite.tla ] && [ "${3:-}" != DirectPath.tla ] && [ "${3:-}" != RelayedClose.tla ] && [ "${3:-}" != HopHold.tla ] && [ "${3:-}" != EndAckLeg.tla ] && [ "${3:-}" != AckFrequency.tla ] && [ "${3:-}" != CreditGrow.tla ] && [ "${3:-}" != AckRanges.tla ] || dl=-deadlock
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
	# okfixture CHECK FILE: a trace with a bug put in (spec/traces/read-*,
	# wait-*): CHECK (rtrace, wtrace) must accept *-ok* and reject each
	# other one.
	okfixture() {
		st=0
		TLC_WORKERS=1 $1 "$PWD/$2" >"$jw/m.out" 2>&1 || st=$?
		case $(basename "$2"):$st in
		*-ok*:0) echo "$(basename "$2"): accepted, as expected" ;;
		*-ok*:*) echo "$(basename "$2"): rejected"; grep -E -A6 'rejected at|Error' "$jw/m.out" | head -10; return 1 ;;
		*:0) echo "$(basename "$2"): accepted (a bug the trace check misses)"; return 1 ;;
		*)
			if grep -q 'trace rejected at line\|Invariant .* is violated' "$jw/m.out"; then
				echo "$(basename "$2"): rejected, as expected"
			else
				echo "$(basename "$2"): TLC failed:"; tail -10 "$jw/m.out"; return 1
			fi
			;;
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
	# cfgpath CFG: spec/CFG, or CFG itself when it is a path (a smaller
	# configuration below).
	cfgpath() { case $1 in /*) echo "$1" ;; *) echo "spec/$1" ;; esac; }
	# Smaller constants for the checks that need no more (lane CIFAST:
	# each mutant still caught, each witness still reached, measured 10 to
	# 40 times faster): Link.cfg without its crash (a tenth of its states:
	# WitnessLost, CleanEndReceived and lost-as-eof need the crash and keep
	# Link.cfg), RelayedClose.cfg with one byte, DirectPath.cfg with two.
	# make spec checks every invariant on the full configurations.
	nocrash=$work/LinkNoCrash.cfg rc1=$work/RelayedCloseOne.cfg dp2=$work/DirectPathTwo.cfg
	sed 's/^    Crash = 1$/    Crash = 0/' spec/Link.cfg >"$nocrash"
	sed 's/^    K = 2$/    K = 1/' spec/RelayedClose.cfg >"$rc1"
	sed 's/^    Total = 3$/    Total = 2/' spec/DirectPath.cfg >"$dp2"
	# job CHECK ARGS: CHECK in the background, SPEC_JOBS at once: a job
	# takes a token from fd 9 to start and puts it back when it ends, so
	# the next starts as soon as any one ends (not when a batch's slowest
	# does); their TLC_JVMS share the job's heap (CI_MEM_BUDGET_MB).
	n=0
	mkfifo "$work/tokens"
	exec 9<>"$work/tokens"
	i=0
	while [ $i -lt "${SPEC_JOBS:-4}" ]; do echo >&9; i=$((i + 1)); done
	job() {
		n=$((n + 1))
		read -r _ <&9
		(job_t0=$(date +%s) jw=$work/job$n && mkdir -p "$jw" && export TLC_JVMS="${SPEC_JOBS:-4}" && META=$jw/states TLC_WORKERS=1 "$@" >"$work/job$n.txt" 2>&1 || touch "$work/job$n.fail"
			echo "$(($(date +%s) - job_t0)) $*" >"$work/job$n.secs"; echo >&9) &
	}
	for m in fin-skips-queue grant-early write-race turns-no-recheck release-early abort-as-fin local-abort-eof; do
		job iface "$nocrash" $m
	done
	job iface Link.cfg lost-as-eof
	for m in open-after-goaway open-no-recheck; do job iface LinkDrain.cfg $m; done
	for m in abort-forget-early reopen-known local-abort-eof; do job iface LinkSess.cfg $m; done
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
	for w in WitnessEof WitnessReset WitnessHere WitnessCredit; do job witness "$nocrash" $w; done
	job witness Link.cfg WitnessLost
	job witness LinkDrain.cfg WitnessRefused
	# A witness per invariant of Link (spec/README.md "Witnesses").
	for w in WitnessNoCredit WitnessWindowFull WitnessQueueHeld WitnessInFlight; do job witness "$nocrash" $w; done
	job witness LinkSess.cfg WitnessOpenAtMax
	job witness LinkDrain.cfg WitnessFinRace
	# Not a guarantee: a clean end at one side while its FIN is lost
	# with the connection (spec/README.md "The session interface").
	job witness Link.cfg CleanEndReceived
	for m in fin-skips-queue grant-early write-race turns-no-recheck release-early; do
		job mutant "$nocrash" 'Mutant = "none"' "Mutant = \"$m\""
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
	# END before the last byte, a reset sent once only (a RESUME without
	# an END not yet read is the design since lane ENDACK: R drops the
	# END sent again).
	# Lane PSCLOSE: the client leaving before the agent's FIN. Lane
	# ENDACKCODE: a Close with a total bound resets a live, slow tail.
	for m in free-on-send send-before-resume write-stale resume-from-acked read-stale no-bind keep-dead-path end-early leave-at-complete total-bound; do
		job mutant "$dp2" 'Mutant = "none"' "Mutant = \"$m\"" DirectPath.tla
	done
	job mutant DirectPathReset.cfg 'Mutant = "none"' 'Mutant = "abort-not-resent"' DirectPath.tla
	# Lane DIRECTSTALL: a move before the agent acknowledged the client's
	# handshake (the token's key) stalls the session.
	job mutant DirectPathProof.cfg 'Mutant = "none"' 'Mutant = "move-before-proof"' DirectPath.tla
	# Lane DIRECTCLOSE: a Close that leaves a Read waiting, one that waits
	# for a vanished peer (the detach bound), a reset that drops the bytes
	# an end took (relay PIPEMATRIX defect 1). Lane ENDACK: the END
	# acknowledged once taken, before the application read it; a Close
	# that dropped bytes counting the END. Lane ENDACKFIX: an ACK of an END
	# taken on a stream left, before the peer sent it again (AckSent).
	for m in close-no-wake close-waits-gone-peer reset-drops-taken ack-on-receipt close-took-end ack-end-unsent; do
		job mutant DirectPathClose.cfg 'Mutant = "none"' "Mutant = \"$m\"" DirectPath.tla
	done
	for w in WitnessCloseWake WitnessLingerOut WitnessResetRead WitnessEndRead WitnessCloseDropReset WitnessEndAckMoved; do job witness DirectPathClose.cfg $w DirectPath.tla; done
	# Lane DIRECTREBIND: the agent's address changes; a token kept from the
	# old connection, a re-punch of the old candidates, none, data on both
	# paths (the stale stream kept).
	for m in stale-token repunch-cached no-repunch write-stale; do
		job mutant DirectPathRebind.cfg 'Mutant = "none"' "Mutant = \"$m\"" DirectPath.tla
	done
	for w in WitnessRedirect WitnessRebindRelayed; do job witness DirectPathRebind.cfg $w DirectPath.tla; done
	# Lane LINKLEFT: S's window auto-tuned, R's ACKs gated on its reader:
	# growth past the cap, a window that shrinks, one restarted at a
	# RESUME (below what S keeps), the gate at the cap (a slow reader
	# holds more), a closed end that still counts what it dropped.
	for m in grow-past-cap shrink resume-resets gate-at-cap close-keeps-held; do
		job mutant DirectPathGrow.cfg 'Mutant = "none"' "Mutant = \"$m\"" DirectPath.tla
	done
	for w in WitnessGrown WitnessGated; do job witness DirectPathGrow.cfg $w DirectPath.tla; done
	for w in WitnessDirect WitnessFallback WitnessResent WitnessRogue WitnessFull WitnessWithheld WitnessEndResent WitnessLeftAnswer; do job witness "$dp2" $w DirectPath.tla; done
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
	# A relayed session's Close, then the program's exit (RelayedClose.tla,
	# lane RELAYCLOSE): Close returning before the far end read the tail
	# and the FIN (the code before), or once the relay read them (the
	# relay's pong: pipe TestRelayedEndDropAfterLastByte); waiting past
	# its connection's end; the joint's onward wait past the far end's
	# connection's end; a failure after the FIN read as the reads' end
	# (the code before: every byte, then a reset); the joint's failed
	# write aborting the client before the tail (the code before); an
	# idle bound that pings restart, so a mute far end holds a Close for
	# ever (relay psConn.Close before: V1READY's 600 s hang); a reset
	# here that keeps the session's ENDACK wait (v0.12.19's Reset), and a
	# Close after it that waits (a CloseWrite racing the Reset); the far
	# end's whole close before the FIN reached it taken for its ENDACK
	# (closeAckErr before lane ENDACKTRUTH).
	# Lane ENDACK: the far end's Mux acknowledging the FIN it received,
	# before its program read it (its program may still fail).
	for m in close-before-ack relay-read-ack ack-lost-hang sync-hang fail-drops-fin abort-at-write-fail liveness-counts-as-progress reset-keeps-endack-wait close-after-reset-waits close-acks ack-on-receipt; do
		job mutant "$rc1" 'Mutant = "none"' "Mutant = \"$m\"" RelayedClose.tla
	done
	for w in WitnessExitThenEof WitnessFinThenAbort WitnessHeld WitnessCloseErr WitnessIdle WitnessResetWaiting WitnessResetFirst WitnessCClose WitnessResetAfterArrival WitnessReadThenClose; do job witness "$rc1" $w RelayedClose.tla; done
	# J's clean close (C closed whole first) read as a reset of an empty
	# stream (closeAckErr, closedFirst after ENDACKTRUTH; lane PIPERESET2).
	job mutant RelayedCloseEmpty.cfg 'Mutant = "none"' 'Mutant = "close-resets"' RelayedClose.tla
	job witness RelayedCloseEmpty.cfg WitnessEmptyClosed RelayedClose.tla
	# Two joints' holds (HopHold.tla, lane ENDACK2D): a hold that ends at
	# the ENDACK (the code since RELAYCLOSE: crossing half closes wait for
	# each other until the idle bound resets both), no hold, an end
	# acknowledged at receipt, a held end that never acknowledges, a joint
	# that closes its app before the app's end was acknowledged (lane PIPERESET).
	for m in hold-until-ack no-hold ack-on-receipt held-end-no-ack close-at-end; do
		job mutant HopHold.cfg 'Mutant = "none"' "Mutant = \"$m\"" HopHold.tla
	done
	for w in WitnessHeldThenDone WitnessCrossing WitnessClosed; do job witness HopHold.cfg $w HopHold.tla; done
	# A held end run inside its sync's receipt callback, onto a QUIC far end
	# whose app reads only after its own CloseWriteSync (lane HOLDCB).
	job mutant HopHoldSeq.cfg 'Mutant = "none"' 'Mutant = "hold-in-receipt"' HopHold.tla
	job witness HopHoldSeq.cfg WitnessReadAfterTell HopHold.tla
	# A joint of two syncing conns (lane LINKHOLD2): an exit end onto a
	# QUIC far end passed unheld while the app's end syncs (the code
	# before: HoldTruth, its app reading beside its CloseWriteSync), and
	# the other way's hold (the app's FIN held for an exit end that waits
	# for the app's read: a sequential app's cycle, NoStuck).
	job mutant HopHoldConc.cfg 'Mutant = "none"' 'Mutant = "sync-no-hold"' HopHold.tla
	job mutant HopHoldSeq.cfg 'Mutant = "none"' 'Mutant = "hold-app-fin"' HopHold.tla
	job witness HopHoldConc.cfg WitnessConcRead HopHold.tla
	# One leg's end acknowledgement per transport (EndAckLeg.tla, lane
	# ENDACK2D): acknowledged once written (quiclink, a TCP visitor, a gRPC
	# Close today), an idle bound pings restart or a fixed one (pathswitch
	# Close today); on QUIC an acknowledgement at the FIN's frame and a wait
	# that a stream reset overtaking the acknowledgement ends, and a whole
	# close before the end was read that acknowledges it (lane ENDACKQUIC;
	# on link too: closeAckErr before lane ENDACKTRUTH).
	# A clean whole close read as a reset of a stream that carried no byte
	# (closeAckErr and closedFirst after ENDACKTRUTH; lane PIPERESET2).
	# Lane ENDACK: acknowledged at the transport's receipt, before the far
	# end's program read the end (link's far end and pathswitch before).
	for m in ack-at-write liveness-counts-as-progress total-bound close-acks close-resets ack-on-receipt; do
		job mutant EndAckLeg.cfg 'Mutant = "none"' "Mutant = \"$m\"" EndAckLeg.tla
	done
	# QUIC: a clean Close's bare 'A', read as a reset of an empty stream
	# (quiclink before lane QUICEND).
	for m in ack-at-final-size wait-on-stream-reset close-acks close-resets ack-on-receipt; do
		job mutant EndAckLegQuic.cfg 'Mutant = "none"' "Mutant = \"$m\"" EndAckLeg.tla
	done
	for w in WitnessClean WitnessIdle WitnessEmptyClosed WitnessFailAfterArrival WitnessReadThenClose; do job witness EndAckLeg.cfg $w EndAckLeg.tla; done
	for w in WitnessFinFirst WitnessEmptyClosed WitnessFailAfterArrival; do job witness EndAckLegQuic.cfg $w EndAckLeg.tla; done
	# A QUIC receiver's ACK policy under ACK_FREQUENCY (AckFrequency.tla,
	# lane SPEED-ACK): the fork's wasMissing rule before (an immediate ACK
	# for every reordered packet), no ACK for a deep hole, IMMEDIATE_ACK
	# ignored, upstream's alarm moved by each packet, an ACK a packet late,
	# the sender's packet threshold below the reordering.
	for m in threshold-ignored no-reorder-ack immediate-dropped alarm-latest count-late sender-threshold; do
		job mutant AckFrequency.cfg 'Mutant = "none"' "Mutant = \"$m\"" AckFrequency.tla
	done
	for w in WitnessTimer WitnessCount WitnessImm WitnessReorderReport WitnessLost WitnessLateQuiet WitnessWait; do job witness AckFrequency.cfg $w AckFrequency.tla; done
	# A path reordering past R (AckFrequencyDeep.cfg, lane ACKFIX): the
	# sender that never asks again once its packet threshold is off.
	job mutant AckFrequencyDeep.cfg 'Mutant = "none"' 'Mutant = "no-update"' AckFrequency.tla
	for w in WitnessSpurious WitnessApplied; do job witness AckFrequencyDeep.cfg $w AckFrequency.tla; done
	# Growing windows (CreditGrow.tla, lane RELAYWIN): growth whatever the
	# reader left unread or at every BLOCKED (a stopped reader's, a peer's
	# early one), past the cap or the budget, an end that keeps its growth,
	# growth sent but not counted, a sender capping credit at the hello's
	# window (a peer without FeatureGrow).
	for m in no-keepup no-epoch no-cap drop-keeps-growth grant-not-counted fixed-cap-sender; do
		job mutant CreditGrowOne.cfg 'Mutant = "none"' "Mutant = \"$m\"" CreditGrow.tla
	done
	job mutant CreditGrow.cfg 'Mutant = "none"' 'Mutant = "no-budget"' CreditGrow.tla
	# A wait for a turn (TurnBound, lane FAIRNESS): a session's data on the
	# path past its window; growth puts more of it there than its first.
	job mutant CreditGrowTurn.cfg 'Mutant = "none"' 'Mutant = "send-uncharged"' CreditGrow.tla
	job witness CreditGrowOne.cfg WitnessAheadPastFirst CreditGrow.tla
	for w in WitnessGrew WitnessCapped WitnessBudgetFull WitnessStoppedFull WitnessBlockedNoGrowth WitnessGrownDelivered; do job witness CreditGrow.cfg $w CreditGrow.tla; done
	# A stream window auto-tuned up to MaxW (AutoWindow.tla, lane LONGFAT):
	# a window doubled past MaxW, an update counting bytes not yet read, a
	# window halved (its limit falls below what was sent), an update that
	# re-sends the old limit.
	for m in no-clamp grant-unread shrink stale-update; do
		job mutant AutoWindow.cfg 'Mutant = "none"' "Mutant = \"$m\"" AutoWindow.tla
	done
	for w in WitnessGrown WitnessFull; do job witness AutoWindow.cfg $w AutoWindow.tla; done
	# A QUIC receiver's bounded ACK ranges (AckRanges.tla, lane SPURIOUS):
	# the history's ranges only (quic-go before: a packet below the lowest
	# range kept, dropped at once, never acknowledged), and the highest K
	# ranges with the new packets not first.
	for m in top-k top-first; do
		job mutant AckRanges.cfg 'Mutant = "none"' "Mutant = \"$m\"" AckRanges.tla
	done
	for w in WitnessEvicted WitnessFull WitnessOrphanAcked; do job witness AckRanges.cfg $w AckRanges.tla; done
	# Credit's waiters (CreditWaiters.tla, lane LINKWAKE): the one-slot
	# wake (the code before) strands a waiter and leaves its Acquire for
	# good; passing the token on strands one that needs less (a stall
	# until the next grant).
	# endfixture CHECK FILE: a trace checked to its end (TraceUnfinished
	# violated) with a bug put in: CHECK must accept *-ok* and reject each
	# other one.
	endfixture() {
		st=0
		TLC_WORKERS=1 $1 "$PWD/$2" >"$jw/m.out" 2>&1 || st=$?
		case $(basename "$2"):$st in
		*-ok*:0) echo "$(basename "$2"): accepted, as expected" ;;
		*-ok*:*) echo "$(basename "$2"): rejected"; tail -10 "$jw/m.out"; return 1 ;;
		*:0) echo "$(basename "$2"): accepted (a bug the trace check misses)"; return 1 ;;
		*)
			if grep -q 'Error: TLC threw\|Error: Parsing' "$jw/m.out"; then
				echo "$(basename "$2"): TLC failed:"; tail -10 "$jw/m.out"; return 1
			fi
			echo "$(basename "$2"): rejected, as expected"
			;;
		esac
	}
	for m in one-slot pass-on; do
		job mutant CreditWaiters.cfg 'Mutant = "none"' "Mutant = \"$m\"" CreditWaiters.tla
	done
	job mutant CreditWaitersLive.cfg 'Mutant = "none"' 'Mutant = "one-slot"' CreditWaiters.tla
	for w in WitnessTwoWait WitnessTooLittle WitnessWakeAll; do job witness CreditWaiters.cfg $w CreditWaiters.tla; done
	# Session reads (SessionRead.tla, lane LINKREAD): no lock on s.rest
	# (two readers) and the deadline flag looked at again after the
	# queue's unlock (a cleared deadline read as io.EOF), the code before.
	for m in no-read-lock flag-outside-lock; do
		job mutant SessionRead.cfg 'Mutant = "none"' "Mutant = \"$m\"" SessionRead.tla
	done
	for w in WitnessTimeout WitnessEOF WitnessBothRead WitnessSharedTail; do job witness SessionRead.cfg $w SessionRead.tla; done
	# A Read beside WriteTo (lane LINKREADWRITE): WriteTo wrote s.rest,
	# then cut it and released s.cur, all without rmu (the code before).
	job mutant SessionReadWriteTo.cfg 'Mutant = "none"' 'Mutant = "writeto-no-lock"' SessionRead.tla
	for w in WitnessWriteToTail WitnessBothKinds; do job witness SessionReadWriteTo.cfg $w SessionRead.tla; done
	# A Read after Pump (lane LINKTRACE2): it left a tail the drain's next
	# run wrote after newer bytes (the code before: no errPumped).
	job mutant SessionReadPump.cfg 'Mutant = "none"' 'Mutant = "read-beside-pump"' SessionRead.tla
	for w in WitnessWriteToTail WitnessRefused; do job witness SessionReadPump.cfg $w SessionRead.tla; done
	# WriteTo beside Pump, or a second Pump (lane SESSIONAPI): both popped
	# the queue, the joined session missing bytes (the code before: no claim).
	job mutant SessionReadPumpWriteTo.cfg 'Mutant = "none"' 'Mutant = "writeto-beside-pump"' SessionRead.tla
	for w in WitnessWriteToRefused WitnessPumpRefused; do job witness SessionReadPumpWriteTo.cfg $w SessionRead.tla; done
	job witness SessionReadWriteTos.cfg WitnessTwoWriteTos SessionRead.tla
	# Session writes (SessionWrite.tla, lane P2PLEFT): a Write waiting for
	# credit on the nil channel when the first write deadline is set.
	job mutant SessionWrite.cfg 'Mutant = "none"' 'Mutant = "nil-channel"' SessionWrite.tla
	for w in WitnessTimeout WitnessWokenWaiter WitnessGranted; do job witness SessionWrite.cfg $w SessionWrite.tla; done
	for f in spec/traces/*.ndjson.live; do job livetrace "$f"; done
	for f in spec/traces/link-*.ndjson; do job linkfixture "$f"; done
	for f in spec/traces/join-*.ndjson; do job joinfixture "$f"; done
	for f in spec/traces/pool-*.ndjson; do job poolfixture "$f"; done
	for f in spec/traces/path-*.ndjson; do job pathfixture "$f"; done
	for f in spec/traces/read-*.ndjson; do job okfixture rtrace "$f"; done
	for f in spec/traces/wait-*.ndjson; do job okfixture wtrace "$f"; done
	for f in spec/traces/write-*.ndjson; do job okfixture swtrace "$f"; done
	for f in spec/traces/grow-*.ndjson; do job okfixture gtrace "$f"; done
	for f in spec/traces/joinclose-*.ndjson; do job endfixture jctrace "$f"; done
	for f in spec/traces/relayedclose-*.ndjson; do job endfixture rctrace "$f"; done
	for f in spec/traces/endackleg-*.ndjson; do job endfixture eatrace "$f"; done
	for f in spec/traces/hophold-*.ndjson; do job endfixture hhtrace "$f"; done
	for f in spec/traces/hopholdseq-*.ndjson; do job endfixture hhtraceseq "$f"; done
	for f in spec/traces/hopholdconc-*.ndjson; do job endfixture hhtraceconc "$f"; done
	for f in spec/traces/ackfreq-*.ndjson; do job endfixture aftrace "$f"; done
	for f in spec/traces/ackranges-*.ndjson; do job endfixture artrace "$f"; done
	for f in spec/traces/autowin-*.ndjson; do job endfixture awtrace "$f"; done
	wait
	fail=0
	i=0
	while [ $i -lt $n ]; do
		i=$((i + 1))
		cat "$work/job$i.txt"
		[ ! -e "$work/job$i.fail" ] || fail=1
	done
	# The longest checks, for a timing table (lane SPECFAST).
	cat "$work"/job*.secs | sort -rn | sed -n '1,8s/^/slowest: /p'
	cat "$work"/job*.secs | awk '{ t += $1 } END { print "checks: " NR ", " t " s in all" }'
	exit $fail
	;;
traces)
	tests=${TRACE_TESTS:-'^(TestSessionCleanEnd|TestSessionReset|TestMuxSlowSessionIsolated|TestGoAwayDrainKeepsTails|TestCrossedOpenRunsOnTheLeavingConnection|TestOpenReadAfterTheCutsSnapshotIsRefused|TestMuxV2GoAway|TestSessionCutWithItsConnection|TestMuxSessions|TestMuxV2Sessions|TestWriteRacingCloseWriteSendsNothingAfterFIN|TestWriteParkedForItsTurnSendsNothingAfterFIN|TestOpenParkedPastPeerGoAwayIsNotSent|TestAbortAtMaxSessionsRacingOpenKeepsTheConnection|TestChainRefuse|TestRefusedOpenReadsReset|TestWriteAfterRefuseSendsNoData|TestCloseWriteRacingThePeersFINDropsAfterItsCLOSE|TestMuxOpenSkipsAnIDThePeerTook|TestReceiveLoopReadsWhileARefusalWaits|TestCloseWriteReportsAFailedFIN|TestStarvedProcessKeepsALivePeer|TestDrainReturnsAfterTheCutItRaced|TestDrainDeadlineAbortsAHeldSession|TestDrainDeadlineKeepsAnEndedSessionsClose|TestChainDrainMiddleHop|TestCloseWriteEndingBothHalvesFreesItsSessionAtOnce|TestTraceFileNamesTheConnection)$'}
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
	# One connection per file: a second Up (or Live) of a side is another
	# connection's, and no model accepts two interleaved (the recorder once
	# named a file by the dialer's port alone, which Linux reuses).
	for t in "$work"/traces/*.ndjson "$work"/traces/*.ndjson.live; do
		[ -e "$t" ] || continue
		for side in d l; do
			k=$(grep -E '"ev":"(Up|Live)"' "$t" | grep -c "\"side\":\"$side\"" || true)
			[ "$k" -le 1 ] || { echo "trace $(basename "$t"): $k connections of side $side in one file"; exit 1; }
		done
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
		(export TLC_JVMS="${TLC_WORKERS:-4}" && TLC_WORKERS=1 trace "$t" >"$t.out" 2>&1 && touch "$t.ok") &
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
	(cd pathswitch && LINKTRACE=$work/dtraces GOWORK=off go test -tags linktrace -count=1 -run "${TRACE_PATHSWITCH_TESTS:-^(TestInOrderRelayed|TestInOrderAcrossMoves|TestNoLossDirectDies|TestDirectAgainAfterRebind|TestAllDeliveredRelayRestart|TestEndOnceResentAcrossSwitch|TestResetReachesFromClient|TestResetReachesFromServer|TestStaleEpochRefused|TestCleanClose|TestCloseAfterReattachEndsBeforeTheBound|TestCloseAfterReattachPathDiesBeforeTheFIN|TestCloseLeftAnswersAReattach|TestWaitAckedThenMove|TestCleanCloseThenStreamDiesEndsTheWriter|TestAbortAfterCleanKeepsTheTail|TestCloseReadDropsAndResets|TestEndAckedOnlyOnceRead|TestEndReadAfterMove|TestWindowGrowsTraced)\$}" .) >"$work/test.out" 2>&1 || {
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
	# quiclink's end handshake (lane ENDACKQUIC): each session's trace, the
	# direction of its first SClose, against EndAckLeg.tla Kind quic
	# (TraceEndAckLeg.tla).
	mkdir -p "$work/eatraces"
	(cd peerlink/quiclink && LINKTRACE_ENDACK=$work/eatraces GOWORK=off go test -tags linktrace -count=1 -run "${TRACE_ENDACK_TESTS:-^TestEndAck(CloseWaitsForFarEnd|MuteFarEndIdles|ConnectionEndFailsTheWait|CloseBeforeReadFails|FarEndResetFails|ResetAfterAckIsClean|EmptyStreamAfterThePeersWholeClose|EmptyStreamPeerResetFails)\$}" .) >"$work/test.out" 2>&1 || {
		cat "$work/test.out"
		exit 1
	}
	for t in "$work"/eatraces/*.ndjson; do
		n=$((n + 1))
		if TLC_WORKERS=1 eatrace "$t" >"$t.out" 2>&1; then
			echo "endack $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: accepted"
		else
			bad=$((bad + 1))
			echo "endack $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: REJECTED"
			grep -E 'Error|unknown event' "$t.out" | head -5
			[ -z "${TRACE_KEEP:-}" ] || cp "$t" "$TRACE_KEEP/"
		fi
	done
	cat "$work"/eatraces/*.ndjson >"$work/eaall"
	gap=
	for e in $(grep -o 'E.ev = "[A-Za-z]*"' spec/TraceEndAckLeg.tla | sed 's/.*"\(.*\)"/\1/' | sort -u); do
		k=$(grep -c "\"ev\":\"$e\"" "$work/eaall" || true)
		[ "$k" != 0 ] || gap="$gap $e"
		printf '%s %s; ' "$e" "$k"
	done
	echo
	echo "end handshake events no trace took:${gap:- none}"
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
	# Session reads (TraceSessionRead.tla), credit's waiters
	# (TraceCreditWaiters.tla), lane LINKTRACE, and session writes
	# (TraceSessionWrite.tla) and a joint's side closed whole
	# (TraceJoinClose.tla), lane LINKTRACE2: each session's (joint's) file, its
	# events counted; an event no trace took is a test gap.
	for k in read wait write grow joinclose peerjoinclose relayed hophold hopholdquic hopholdconc ackfreq ackranges autowin; do
		mkdir -p "$work/$k"
		pkg= mdir=. tags=linktrace
		case $k in
		read) env=LINKTRACE_READ check=rtrace tests=${TRACE_READ_TESTS:-'^(TestSessionConcurrentReads|TestSessionReadDeadlineClearedNeverEOF|TestSessionReadBesideWriteTo|TestSessionReadAfterPumpRefused|TestSessionPumpErrors|TestSessionWriteToBesidePumpRefused|TestSessionAPIPairs)$'} model=TraceSessionRead ;;
		wait) env=LINKTRACE_WAIT check=wtrace tests=${TRACE_WAIT_TESTS:-'^(TestConcurrentAcquiresShareAGrant|TestSessionWriteWaitingForCreditSeesFirstDeadline|TestMuxSlowSessionIsolated)$'} model=TraceCreditWaiters ;;
		joinclose) env=LINKTRACE_JOIN check=jctrace tests=${TRACE_JOINCLOSE_TESTS:-'^(TestJoinWholeCloseDeliversTheTail|TestChainHalfClose)$'} model=TraceJoinClose ;;
		relayed) env=LINKTRACE_RELAYED check=rctrace tests=${TRACE_RELAYED_TESTS:-'^TestEndAck(WaitsForTheFarEnd|FailsWhenTheFarEndIsCut|FailsWhenItsConnectionIsCut|GivesUpWithoutProgress|ResetEndsTheWait|ExitThenTheFarEndWrites|CutThenTheFarEndWrites|CutWhileTheTailCopiesHoldsTheAbort|FailsWhenTheRelayIsGone|FarEndCloseBeforeFinFails)$'} model=TraceRelayedClose ;;
		peerjoinclose) env=LINKTRACE_JOIN check=jctrace pkg=./peerlink/ tests=${TRACE_PEER_JOINCLOSE_TESTS:-'^TestPeerWholeCloseDeliversTheTail$'} model=TraceJoinClose ;;
		hophold) env=LINKTRACE_HOPHOLD check=hhtrace pkg=./peerlink/ tests=${TRACE_HOPHOLD_TESTS:-'^TestJoinCrossingHalfClosesEnd$'} model=TraceHopHold ;;
		# R's app a sequential QUIC far end (HopHold.tla SeqApp, lane LINKGATE): quiclink's module
		hopholdquic) env=LINKTRACE_HOPHOLD check=hhtraceseq mdir=peerlink/quiclink tests=${TRACE_HOPHOLD_QUIC_TESTS:-'^TestJoinsHoldOntoASequentialQuicApp$'} model=TraceHopHold ;;
		# R's app a QUIC far end reading beside its CloseWriteSync (ConcApp, lane LINKHOLD2)
		hopholdconc) env=LINKTRACE_HOPHOLD check=hhtraceconc mdir=peerlink/quiclink tests=${TRACE_HOPHOLD_CONC_TESTS:-'^TestJoinHoldsOntoAConcurrentQuicApp$'} model=TraceHopHold ;;
		# The quic-go fork's 1-RTT receivers (ACK frequency, lane LINKSPEED2):
		# its own build tag; IMMEDIATE_ACK no sender of the fork's sends.
		ackfreq) env=QUICACKTRACE check=aftrace mdir=peerlink/quiclink tags=quicacktrace tests=${TRACE_ACKFREQ_TESTS:-'^(TestAckFrequencyOnReorderingPath|TestRefusesPeerWithoutAckFrequency)$'} model=TraceAckFrequency ;;
		# FeatureGrow on (the only recorder that keeps it: trace_grow_on.go traceFixedWindows)
		grow) env=LINKTRACE_GROW check=gtrace tests=${TRACE_GROW_TESTS:-'^(TestWindowGrowsOverALongPath|TestGrantPastTheHelloWindow|TestMuxSlowSessionIsolated|TestMuxSessions|TestSessionCleanEnd|TestSessionReset)$'} model=TraceCreditGrow ;;
		# The same receivers' ACK ranges (Ack lines' rs, lane SPURIOUS; fork v0.63.1-nfltr.10).
		ackranges) env=QUICACKTRACE check=artrace mdir=peerlink/quiclink tags=quicacktrace tests=${TRACE_ACKRANGES_TESTS:-'^(TestAckFrequencyOnReorderingPath|TestRefusesPeerWithoutAckFrequency)$'} model=TraceAckRanges ;;
		# quiclink's auto-tuned stream windows (Config.MaxWindow, lane LINKLEFT): both ends' qlog events and the receiver's reads.
		autowin) env=LINKTRACE_AUTOWIN check=awtrace mdir=peerlink/quiclink tests=${TRACE_AUTOWIN_TESTS:-'^TestMaxWindowOnALongPath$'} model=TraceAutoWindow ;;
		write) env=LINKTRACE_WRITE check=swtrace tests=${TRACE_WRITE_TESTS:-'^(TestSessionWriteWaitingForCreditSeesFirstDeadline|TestWriteDeadline|TestWriteDeadlineStuckNetwork)$'} model=TraceSessionWrite ;;
		esac
		(cd "$mdir" && env "$env=$work/$k" GOWORK=off go test -tags "$tags" -count=1 -run "$tests" "${pkg:-.}") >"$work/test.out" 2>&1 || {
			cat "$work/test.out"
			exit 1
		}
		m=0
		for t in "$work/$k"/*.ndjson; do
			[ -s "$t" ] || continue
			m=$((m + 1))
			(export TLC_JVMS="${TLC_WORKERS:-4}" && TLC_WORKERS=1 $check "$t" >"$t.out" 2>&1 && touch "$t.ok") &
			[ $((m % ${TLC_WORKERS:-4})) != 0 ] || wait
		done
		wait
		for t in "$work/$k"/*.ndjson; do
			[ -s "$t" ] || continue
			n=$((n + 1))
			if [ -f "$t.ok" ]; then
				echo "$k $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: accepted"
			else
				bad=$((bad + 1))
				echo "$k $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: REJECTED"
				grep -E -A6 'rejected at|Error' "$t.out" | head -12
				[ -z "${TRACE_KEEP:-}" ] || cp "$t" "$TRACE_KEEP/"
			fi
		done
		cat "$work/$k"/*.ndjson >"$work/${k}all"
		gap=
		for e in $(grep -o 'E.ev = "[A-Za-z0-9]*"' "spec/$model.tla" | sed 's/.*"\(.*\)"/\1/' | sort -u); do
			c=$(grep -c "\"ev\":\"$e\"" "$work/${k}all" || true)
			[ "$c" != 0 ] || gap="$gap $e"
			printf '%s %s; ' "$e" "$c"
		done
		echo
		echo "$k events no trace took:${gap:- none}"
	done
	echo "$n traces, $bad rejected"
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
rtrace)
	rtrace "$2"
	;;
wtrace)
	wtrace "$2"
	;;
gtrace)
	gtrace "$2"
	;;
swtrace)
	swtrace "$2"
	;;
jctrace)
	jctrace "$2"
	;;
rctrace)
	rctrace "$2"
	;;
hhtrace)
	hhtrace "$2"
	;;
aftrace)
	aftrace "$2"
	;;
artrace)
	artrace "$2"
	;;
awtrace)
	awtrace "$2"
	;;
*)
	echo "usage: spec.sh check|invariants|nightly|mutants|traces|trace FILE" >&2
	exit 2
	;;
esac
