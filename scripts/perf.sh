#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# perf.sh: the raw-speed benchmarks (perf_bench_test.go,
# docs/performance.md), A/B and profiled.
#
#   scripts/perf.sh ab OLD NEW OUT [COUNT] [BENCH] [BENCHTIME]
#     builds the package's test binary at git refs OLD and NEW and runs
#     BENCH (default: Throughput|Latency|IdleSessions) COUNT times each
#     (default 6), old and new alternating so that the machine's load hits
#     both alike; OUT gets old.txt, new.txt, benchstat.txt (benchstat of
#     the two), load.txt (the load average before each run) and
#     manifest.json.
#   scripts/perf.sh profile REF OUT [BENCH] [BENCHTIME]
#     runs each benchmark BENCH names (default: the profiled set below) at
#     REF alone with cpu, heap (alloc and inuse), mutex and block profiles,
#     and the goroutine (debug=2) and goroutine leak profiles (Go 1.27 has
#     it without GOEXPERIMENT=goroutineleakprofile); OUT/<bench>.* and
#     manifest.json.
#
# benchstat is pinned (BENCHSTAT_VERSION). Needs a clean ref: it uses git
# worktrees under a temporary directory, removed at the end.
set -euo pipefail
cd "$(dirname "$0")/.."
BENCHSTAT_VERSION=v0.0.0-20261009192801-be2c69fb417e
DEFAULT_BENCH='Throughput|Latency|IdleSessions'
PROFILED=(
	'Throughput/tr=tcp/size=64/sessions=8/dir=oneway$'
	'Throughput/tr=tcp/size=65536/sessions=8/dir=oneway$'
	'Throughput/tr=tls/size=65536/sessions=8/dir=oneway$'
	'Latency/tr=tcp/held$'
)

tmp=$(mktemp -d)
cleanup() {
	for w in "$tmp"/wt-*; do
		[ -d "$w" ] && git worktree remove --force "$w" >/dev/null 2>&1
	done
	rm -rf "$tmp"
}
trap cleanup EXIT

# build REF NAME: the test binary of the package at REF, as $tmp/NAME.test.
build() {
	local w="$tmp/wt-$2"
	git worktree add --detach "$w" "$1" >/dev/null 2>&1
	(cd "$w" && GOWORK=off go test -c -o "$tmp/$2.test" .) || return 1
	git -C "$w" rev-parse HEAD
}

loadavg() { sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' || cut -d' ' -f1-3 /proc/loadavg; }

# manifest OUT CMD BENCH SHAS...: OUT/manifest.json.
manifest() {
	local out=$1 cmd=$2 bench=$3
	shift 3
	cat >"$out/manifest.json" <<EOF
{
  "repo": "github.com/nfltr/link",
  "sha": "$*",
  "go": "$(go env GOVERSION)",
  "goos": "$(go env GOOS)",
  "goarch": "$(go env GOARCH)",
  "machine": "$(uname -mn) $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -r), $(getconf _NPROCESSORS_ONLN) cpus",
  "loadavg": "$(loadavg | xargs)",
  "command": "$cmd",
  "bench": "$bench",
  "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
}

case "${1:-}" in
ab)
	old=$2 new=$3 out=$4 count=${5:-6} bench=${6:-$DEFAULT_BENCH} bt=${7:-1s}
	mkdir -p "$out"
	osha=$(build "$old" old)
	nsha=$(build "$new" new)
	: >"$out/old.txt"
	: >"$out/new.txt"
	: >"$out/load.txt"
	for i in $(seq 1 "$count"); do
		for side in old new; do
			echo "run $i $side: load $(loadavg)" | tee -a "$out/load.txt"
			"$tmp/$side.test" -test.run '^$' -test.bench "$bench" -test.benchtime "$bt" -test.count 1 -test.timeout 0 >>"$out/$side.txt"
		done
	done
	GOWORK=off go run "golang.org/x/perf/cmd/benchstat@$BENCHSTAT_VERSION" "$out/old.txt" "$out/new.txt" >"$out/benchstat.txt"
	manifest "$out" "scripts/perf.sh $*" "$bench" "old=$osha new=$nsha"
	cat "$out/benchstat.txt"
	;;
profile)
	ref=$2 out=$3 bt=${5:-2s}
	mkdir -p "$out"
	sha=$(build "$ref" prof)
	if [ -n "${4:-}" ]; then PROFILED=("$4"); fi
	for b in "${PROFILED[@]}"; do
		n=$(echo "$b" | tr '/=' '_-' | tr -d '$')
		LINK_PERF_PROFILE_DIR="$out" "$tmp/prof.test" -test.run '^$' -test.bench "$b" -test.benchtime "$bt" -test.count 1 \
			-test.cpuprofile "$out/$n.cpu.pprof" -test.memprofile "$out/$n.heap.pprof" -test.memprofilerate 4096 \
			-test.mutexprofile "$out/$n.mutex.pprof" -test.mutexprofilefraction 1 \
			-test.blockprofile "$out/$n.block.pprof" -test.blockprofilerate 10000 -test.outputdir "$out" | tee "$out/$n.bench.txt"
	done
	cp "$tmp/prof.test" "$out/link.test" # pprof reads the symbols from it
	manifest "$out" "scripts/perf.sh $*" "${PROFILED[*]}" "$sha"
	;;
*)
	sed -n '5,22p' "$0"
	exit 2
	;;
esac
