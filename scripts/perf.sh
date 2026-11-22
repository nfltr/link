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
#     manifest.json. PERF_CPU (e.g. 1,10) runs each at those GOMAXPROCS;
#     PERF_PKG (default .) is the package directory (make bench-ab-quiclink).
#   scripts/perf.sh profile REF OUT [BENCH] [BENCHTIME]
#     runs each benchmark BENCH names (default: the profiled set below) at
#     REF alone with cpu, heap (alloc and inuse), mutex and block profiles,
#     and the goroutine (debug=2) and goroutine leak profiles (Go 1.27 has
#     it without GOEXPERIMENT=goroutineleakprofile); OUT/<bench>.* and
#     manifest.json.
#   scripts/perf.sh baseline OUT   (make profile-baseline OUT=<dir>)
#     the profile baseline of the checked-out commit (a clean tree): every
#     benchmark of link, pathswitch and quiclink with -benchmem -count
#     BASELINE_COUNT (6; OUT/<module>.bench.txt and its benchstat), then
#     each benchmark once more alone with cpu, heap, mutex and block
#     profiles and, once it ended, the goroutine (debug=2) and goroutine
#     leak profiles of what it left (OUT/<module>.<Benchmark>.*; their
#     totals in OUT/summary.txt). The manifest's series is the commit's v1+
#     tag, else pre-freeze.
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
	(cd "$w/${PERF_PKG:-.}" && GOWORK=off go test -c -o "$tmp/$2.test" .) || return 1
	git -C "$w" rev-parse HEAD
}

loadavg() { sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' || cut -d' ' -f1-3 /proc/loadavg; }

# json STRING: STRING as a JSON string (backslash, quote, tab, newline and
# carriage return escaped: commands and patterns hold no other control
# characters).
json() {
	local s=${1//\\/\\\\}
	s=${s//\"/\\\"}
	s=${s//$'\t'/\\t}
	s=${s//$'\n'/\\n}
	s=${s//$'\r'/\\r}
	printf '"%s"' "$s"
}

# series SHA: the profile series SHA belongs to (nfltr's AGENTS.md
# "Profiling Evidence after API Freeze"): its v1+ tag (the freeze and
# after), else "pre-freeze" (working data, not the comparable series).
series() {
	git describe --exact-match --tags --match 'v[1-9]*' "$1" 2>/dev/null || echo pre-freeze
}

# cpuname: the machine's CPU model.
cpuname() {
	sysctl -n machdep.cpu.brand_string 2>/dev/null && return
	sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo 2>/dev/null | sed -n 1p | grep . || uname -r
}

# manifest OUT CMD BENCH SHAS...: OUT/manifest.json; the series is the last
# sha's (ab: new=<sha>). LOAD_START, when set, is the load average when the
# run began.
manifest() {
	local out=$1 cmd=$2 bench=$3
	shift 3
	local all="$*"
	local last=${all##* }
	cat >"$out/manifest.json" <<EOF
{
  "repo": "github.com/nfltr/link",
  "sha": $(json "$*"),
  "series": $(json "$(series "${last#*=}")"),
  "go": $(json "$(go env GOVERSION)"),
  "goos": $(json "$(go env GOOS)"),
  "goarch": $(json "$(go env GOARCH)"),
  "machine": $(json "$(uname -mn) $(cpuname), $(getconf _NPROCESSORS_ONLN) cpus"),
  "loadavg_start": $(json "${LOAD_START:-}"),
  "loadavg": $(json "$(loadavg | xargs)"),
  "command": $(json "$cmd"),
  "bench": $(json "$bench"),
  "date": $(json "$(date -u +%Y-%m-%dT%H:%M:%SZ)")
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
			"$tmp/$side.test" -test.run '^$' -test.bench "$bench" -test.benchtime "$bt" -test.count 1 -test.timeout 0 \
				${PERF_CPU:+-test.cpu "$PERF_CPU"} >>"$out/$side.txt"
		done
	done
	GOWORK=off go run "golang.org/x/perf/cmd/benchstat@$BENCHSTAT_VERSION" "$out/old.txt" "$out/new.txt" >"$out/benchstat.txt"
	manifest "$out" "PERF_CPU=${PERF_CPU:-} scripts/perf.sh $*" "$bench" "old=$osha new=$nsha"
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
baseline)
	out=$2 count=${BASELINE_COUNT:-6} bt=${BASELINE_BENCHTIME:-1s}
	mods=".:link pathswitch:pathswitch peerlink/quiclink:quiclink"
	clean() { [ -z "$(git status --porcelain)" ] && [ "$(git rev-parse HEAD)" = "${sha:-$(git rev-parse HEAD)}" ]; }
	clean || {
		echo "perf.sh baseline: the tree has changes; a baseline measures a commit" >&2
		exit 1
	}
	sha=$(git rev-parse HEAD)
	LOAD_START=$(loadavg | xargs)
	mkdir -p "$out"
	: >"$out/summary.txt"
	# Every binary first, so that an edit during the run measures nothing
	# but the commit (pprof reads the symbols from them).
	for mod in $mods; do
		(cd "${mod%%:*}" && GOWORK=off go test -c -o "$out/${mod##*:}.test" .)
	done
	clean || {
		echo "perf.sh baseline: the tree changed while building" >&2
		exit 1
	}
	for mod in $mods; do
		n=${mod##*:}
		echo "$n: -benchmem -count $count" >&2
		"$out/$n.test" -test.run '^$' -test.bench . -test.benchmem -test.count "$count" -test.benchtime "$bt" \
			-test.timeout 0 >"$out/$n.bench.txt"
		GOWORK=off go run "golang.org/x/perf/cmd/benchstat@$BENCHSTAT_VERSION" "$out/$n.bench.txt" >"$out/$n.benchstat.txt"
		for b in $("$out/$n.test" -test.list '^Benchmark'); do
			p="$out/$n.$b"
			echo "$n.$b: profiles" >&2
			LINK_PERF_EXIT_PROFILE="$p" "$out/$n.test" -test.run '^$' -test.bench "^$b\$" -test.benchmem -test.count 1 \
				-test.benchtime "$bt" -test.timeout 0 \
				-test.cpuprofile "$p.cpu.pprof" -test.memprofile "$p.heap.pprof" -test.memprofilerate 4096 \
				-test.mutexprofile "$p.mutex.pprof" -test.mutexprofilefraction 1 \
				-test.blockprofile "$p.block.pprof" -test.blockprofilerate 10000 -test.outputdir "$out" >"$p.bench.txt"
			g=$(grep -c '^goroutine [0-9]* \[' "$p.goroutine.txt" || true)
			l=$(sed -n 's/^goroutineleak profile: total //p' "$p.goroutineleak.txt")
			echo "$n.$b goroutines-left=$g leaked=${l:-?}" >>"$out/summary.txt"
		done
	done
	manifest "$out" "scripts/perf.sh $*" "-test.bench . -test.benchmem -test.count $count -test.benchtime $bt; profiles: one run per benchmark" "$sha"
	cat "$out/summary.txt"
	;;
manifest) # the manifest writer alone (scripts/perf_test.sh)
	shift
	manifest "$@"
	;;
*)
	sed -n '5,29p' "$0"
	exit 2
	;;
esac
