#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# run.sh OUT [COUNT]: link, yamux and muxado with their defaults (README.md).
# Builds the test binary once, then COUNT (default 8) passes of every
# benchmark; each pass rotates the libraries' order (LIBS), and within a
# pass each benchmark runs for all three before the next, so load from
# other work hits all alike. Then one profiled pass per library (cpu, heap,
# mutex, block) of the throughput, latency and open benchmarks. OUT gets
# raw.txt, benchstat.txt, prof/ and manifest.json.
set -euo pipefail
cd "$(dirname "$0")"
BENCHSTAT_VERSION=v0.0.0-20261009192801-be2c69fb417e
out=$1 count=${2:-8}
mkdir -p "$out/prof"
loadavg() { sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' || cut -d' ' -f1-3 /proc/loadavg; }
load0=$(loadavg | xargs)
GOWORK=off go test -c -o "$out/defaults.test" .
orders=(link,yamux,muxado yamux,muxado,link muxado,link,yamux)
: >"$out/raw.txt"
for i in $(seq "$count"); do
	export LIBS=${orders[$(((i - 1) % 3))]}
	echo "pass $i/$count LIBS=$LIBS load $(loadavg | xargs)" >&2
	"$out/defaults.test" -test.run '^$' -test.bench 'Bulk|Latency|Open' -test.benchmem -test.count 1 -test.timeout 30m >>"$out/raw.txt" || echo "pass $i: a benchmark failed (raw.txt)" >&2
	"$out/defaults.test" -test.run '^$' -test.bench 'IdleMemory|SlowReader' -test.benchtime 1x -test.benchmem -test.count 1 -test.timeout 30m >>"$out/raw.txt" || echo "pass $i: a benchmark failed (raw.txt)" >&2
done
for l in link yamux muxado; do
	LIBS=$l "$out/defaults.test" -test.run '^$' -test.bench 'Bulk/sessions=(1|64)/rtt=0s|Latency/held|Open' -test.benchmem \
		-test.cpuprofile "$out/prof/$l.cpu" -test.memprofile "$out/prof/$l.heap" \
		-test.mutexprofile "$out/prof/$l.mutex" -test.blockprofile "$out/prof/$l.block" >"$out/prof/$l.txt" || echo "profile $l: failed" >&2
done
load1=$(loadavg | xargs)
rm -f "$out/defaults.test"
GOWORK=off go run golang.org/x/perf/cmd/benchstat@$BENCHSTAT_VERSION -col /lib "$out/raw.txt" >"$out/benchstat.txt"
cat >"$out/manifest.json" <<EOF
{
  "repo": "github.com/nfltr/link",
  "sha": "$(git rev-parse HEAD)",
  "dirty": "$(git status --porcelain . | wc -l | xargs) files under compare/defaults",
  "versions": "$(GOWORK=off go list -m -f '{{.Path}}@{{.Version}}' github.com/hashicorp/yamux golang.ngrok.com/muxado/v2 | xargs)",
  "go": "$(GOWORK=off go env GOVERSION)",
  "goos": "$(go env GOOS)",
  "goarch": "$(go env GOARCH)",
  "machine": "$(uname -mn) $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -r), $(getconf _NPROCESSORS_ONLN) cpus",
  "loadavg_before": "$load0",
  "loadavg_after": "$load1",
  "command": "compare/defaults/run.sh $out $count",
  "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
cat "$out/benchstat.txt"
