#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# fuzz-short.sh: every Go fuzz target (every decoder), FUZZTIME each
# (default 10s; make fuzz-long: 30m each). FUZZPARALLEL caps the workers
# per target (default: GOMAXPROCS). A crasher fails the run and go writes
# it to <package>/testdata/fuzz/<Target>/; commit it with the fix as a
# regression seed (it then runs in every plain `go test`). FUZZING.md
# records the long runs.
set -uo pipefail
cd "$(dirname "$0")/.."
TARGETS=(
	". FuzzRecv"
	". FuzzSendRecvRoundTrip"
	". FuzzParseLiveness"
	". FuzzServerHello"
	". FuzzClientReply"
	". FuzzRecvV2"
	". FuzzMuxServeV2"
	". FuzzParseWindow"
	". FuzzForward"
	"peerlink/quiclink FuzzPreamble"
	"peerlink/quiclink FuzzAnswer"
)
failed=0
for t in "${TARGETS[@]}"; do
	read -r dir name <<<"$t"
	echo "── ${dir} ${name} (${FUZZTIME:-10s})"
	start=$(date +%s)
	(cd "${dir}" && GOWORK=off go test -run '^$' -fuzz "^${name}\$" -fuzztime "${FUZZTIME:-10s}" \
		-fuzzminimizetime 100x ${FUZZPARALLEL:+-parallel "${FUZZPARALLEL}"} .) || failed=1
	echo "── ${dir} ${name}: $(($(date +%s) - start))s"
done
exit "${failed}"
