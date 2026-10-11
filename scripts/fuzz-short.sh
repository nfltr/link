#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# fuzz-short.sh: every Go fuzz target (every decoder), FUZZTIME each
# (default 10s; nightly FUZZTIME=10m). A crasher fails the run and go writes
# it to <package>/testdata/fuzz/<Target>/; commit it with the fix as a
# regression seed (it then runs in every plain `go test`).
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
)
failed=0
for t in "${TARGETS[@]}"; do
	read -r pkg name <<<"$t"
	echo "── ${pkg} ${name} (${FUZZTIME:-10s})"
	go test -run '^$' -fuzz "^${name}\$" -fuzztime "${FUZZTIME:-10s}" -fuzzminimizetime 100x "${pkg}" || failed=1
done
exit "${failed}"
