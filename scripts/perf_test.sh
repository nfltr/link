#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
#
# perf_test.sh: scripts/perf.sh's manifest writer writes valid JSON with
# every field the profile series needs (a command with quotes, backslashes
# and tabs kept exactly), and its series is the commit's v1+ tag, else
# pre-freeze. Offline, in a temp repository holding a copy of perf.sh.
set -euo pipefail
src="$(cd "$(dirname "$0")" && pwd)/perf.sh"
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
unset "${!GIT_@}"
mkdir -p "$t/repo/scripts" "$t/out"
cp "$src" "$t/repo/scripts/perf.sh"
cd "$t/repo"
git init -q .
export GIT_AUTHOR_NAME=nfltr GIT_AUTHOR_EMAIL=oss@nfltr.xyz GIT_COMMITTER_NAME=nfltr GIT_COMMITTER_EMAIL=oss@nfltr.xyz
git add . && git commit -q -m base
sha=$(git rev-parse HEAD)
fail=0
cmd=$'scripts/perf.sh baseline "/tmp/a b" \\d+\tx $HOME'
# check SERIES SHAFIELD: the manifest parses and holds what was written.
check() {
	LOAD_START="1.0 2.0 3.0" scripts/perf.sh manifest "$t/out" "$cmd" '^Benchmark$/^x=1$' $2 ||
		{ echo "FAIL: perf.sh manifest exited $?" >&2; fail=1; return; }
	python3 - "$t/out/manifest.json" "$cmd" "$1" "$2" <<'PY' || fail=1
import json, re, sys
path, cmd, series, sha = sys.argv[1:]
m = json.load(open(path))
want = {"repo": "github.com/nfltr/link", "sha": sha, "series": series,
        "command": cmd, "bench": "^Benchmark$/^x=1$", "loadavg_start": "1.0 2.0 3.0"}
bad = [f"{k}={m.get(k)!r}, want {v!r}" for k, v in want.items() if m.get(k) != v]
for k in ("go", "goos", "goarch", "machine", "loadavg"):
    if not m.get(k):
        bad.append(f"{k} empty")
if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", m.get("date", "")):
    bad.append(f"date={m.get('date')!r}")
for b in bad:
    print("FAIL:", series, b, file=sys.stderr)
sys.exit(1 if bad else 0)
PY
}
check pre-freeze "$sha"
git tag v0.13.0
check pre-freeze "$sha"
git tag -a -m v1 v1.0.0
check v1.0.0 "$sha"
check v1.0.0 "old=0000000 new=$sha"
[ "$fail" = 0 ] && echo "perf_test: ok"
exit "$fail"
