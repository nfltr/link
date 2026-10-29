#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
# test-ci.sh: make test-race, test-linkdebug and fuzz-short at once, each
# to its own log, printed in that order once all three ended; fails if any
# failed. The test packages spend most of their wall time waiting (timers,
# peers, deadlines), not on CPU, so one pass beside the others costs CI
# little CPU and saves the sum of their walls.
set -uo pipefail
cd "$(dirname "$0")/.."
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
parts="test-race test-linkdebug fuzz-short"
for p in $parts; do
	(
		s=$(date +%s)
		make --no-print-directory "$p" >"$dir/$p.log" 2>&1
		rc=$?
		echo "$rc $(($(date +%s) - s))" >"$dir/$p.rc"
	) &
done
wait
failed=0
for p in $parts; do
	read -r rc secs <"$dir/$p.rc"
	echo "── make $p (exit $rc, ${secs}s)"
	cat "$dir/$p.log"
	[ "$rc" -eq 0 ] || failed=1
done
for p in $parts; do
	read -r rc secs <"$dir/$p.rc"
	echo "test-ci: make $p exit $rc in ${secs}s"
done
exit "$failed"
