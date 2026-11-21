# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

GOVULNCHECK_VERSION := v1.8.0

.PHONY: check test-identity build vet check-fmt test test-race test-linkdebug check-static fuzz-short fuzz-long check-gosec check-semgrep check-govulncheck
check: check-static test-race test-linkdebug fuzz-short
check-static: build vet check-fmt test-identity test-perf check-gosec check-semgrep check-govulncheck

# check-fmt: gofmt (the toolchain's, not one on PATH) changes no tracked Go
# file (vendor/ aside); go vet and the tests accept unformatted code.
check-fmt:
	@out="$$("$$(go env GOROOT)/bin/gofmt" -l $$(git ls-files '*.go' | grep -v '^vendor/'))"; \
	if [ -n "$$out" ]; then echo "gofmt would change:"; echo "$$out"; exit 1; fi

# scripts/check-identity.sh (the pre-push hook's) refuses what it should.
test-identity:
	./scripts/check-identity_test.sh

# scripts/perf.sh's manifest writer (the profile series' manifest.json).
.PHONY: test-perf
test-perf:
	./scripts/perf_test.sh

# Everything CI runs (make ci, from the workflow): check plus every module of
# its own (the soak module's go.mod goes stale on each link or quiclink bump
# unless it is checked too).
.PHONY: check-all
check-all: ci

build:
	go build ./...

vet:
	go vet ./...

test:
	go test -count=1 ./...

test-race:
	go test -count=1 -race ./...

# test-under-load: PKGS with -count LOAD_COUNT (5) on a starved machine
# (scripts/test-under-load.sh, the same file in link, relay and relay-core):
# a test that waits a fixed time for what a busy machine does later fails
# here; wait for the event or state instead, with a bound only against a hang.
# A nested module's: PKGS='-C peerlink/quiclink .'.
LOAD_COUNT ?= 5
.PHONY: test-under-load
test-under-load:
	@test -n "$(PKGS)" || { echo "test-under-load: PKGS='<packages>' is required" >&2; exit 2; }
	@./scripts/test-under-load.sh $(LOAD_COUNT) $(PKGS)
# stress-race: PKGS (go test arguments: packages, -run) with -race -count
# COUNT (20) (`ci-request TARGET=stress-race PKGS='-run TestX ./peerlink/'`);
# a nested module's: PKGS='-C peerlink/quiclink -run TestX .'.
COUNT ?= 20
stressC = $(if $(filter -C,$(firstword $(PKGS))),$(wordlist 1,2,$(PKGS)))
.PHONY: stress-race
stress-race:
	@test -n "$(PKGS)" || { echo "stress-race: PKGS='<packages>' is required" >&2; exit 2; }
	go test $(stressC) -race -count=$(COUNT) $(filter-out $(stressC),$(PKGS))
# stress-flaky: every known-flaky test of scripts/flaky-tests.txt -race -count
# STRESS_COUNT (10) under test-under-load (scripts/stress-flaky.sh); the CI
# runners hammer them with `ci-request TARGET=stress-flaky`.
.PHONY: stress-flaky
stress-flaky:
	@./scripts/stress-flaky.sh

# Debug mode (-tags linkdebug, README "Debug mode"): its build and tests.
test-linkdebug:
	go vet -tags linkdebug .
	go test -count=1 -race -tags linkdebug -run TestDebug .

# FUZZTIME per target (default 10s); FUZZPARALLEL caps the workers.
fuzz-short:
	./scripts/fuzz-short.sh

# Every target for FUZZTIME (default 30m) each; record the run in FUZZING.md.
fuzz-long:
	FUZZTIME=$(or $(FUZZTIME),30m) ./scripts/fuzz-short.sh

# Pinned SAST gated by scripts/{gosec,semgrep}-baseline.txt (semgrep needs docker).
check-gosec:
	./scripts/check-gosec.sh

check-semgrep:
	./scripts/check-semgrep.sh

# Needs network (vuln.go.dev). Standard library only: any reachable
# vulnerability is the toolchain's; update Go.
check-govulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# The exported API of each module against its latest release (apidiff,
# pinned in the script): the changes must be the ones api/ lists. A change
# on purpose: CHANGELOG.md, then UPDATE=1 make check-api.
.PHONY: check-api test-check-api
# test-check-api: check-api fails, never passes, in a clone without the tags.
test-check-api:
	./scripts/check-api_test.sh

check-api: test-check-api
	./scripts/check-api.sh . '' link
	./scripts/check-api.sh peerlink/quiclink peerlink/quiclink/ quiclink
	./scripts/check-api.sh pathswitch pathswitch/ pathswitch

# pathswitch (a module of its own, v0 until its wire is: pathswitch/WIRE.md):
# build, vet, the suite with -race. Its check-api starts with its first tag.
.PHONY: check-pathswitch
check-pathswitch:
	cd pathswitch && GOWORK=off go build ./... && GOWORK=off go vet ./... && GOWORK=off go test -count=1 -race ./...

# peerlink over QUIC (peerlink/quiclink, a module of its own: quic-go never
# enters link's dependencies): build, vet, the conformance suite with -race.
# 30 min bounds a hang: on a loaded macOS host QUIC over loopback pays a
# wakeup per datagram and the package takes past go test's default 10 min
# while moving (a stuck case fails on its own progress bound first).
.PHONY: check-quiclink
check-quiclink:
	cd peerlink/quiclink && GOWORK=off go build ./... && GOWORK=off go vet ./... && GOWORK=off go test -count=1 -race -timeout 30m ./...

# linkvet (linkvet/, a module of its own: golang.org/x/tools never enters
# link's dependencies): vet, its analysistest cases with -race, then its
# checks over link's own packages (tests aside: they leave sessions to the
# peer on purpose; peerlinktest writes after CloseWrite on purpose).
.PHONY: check-linkvet
check-linkvet:
	cd linkvet && GOWORK=off go vet ./... && GOWORK=off go test -count=1 -race ./...
	t="$$(mktemp -d)" && (cd linkvet && GOWORK=off go build -o "$$t/linkvet" ./cmd/linkvet) && \
	GOWORK=off "$$t/linkvet" -test=false $$(GOWORK=off go list ./... | grep -v /peerlinktest); s=$$?; rm -rf "$$t"; exit $$s

# bench: the benchmarks nfltr's perf series measures (its
# deploy/linux-node/perf-series.sh link; keep the two sets the same),
# -benchmem -count BENCH_COUNT (6, for benchstat), into BENCH_OUT (TMPDIR's:
# a CI runner's `ci-request TARGET=bench` keeps it in the result's out).
BENCH_COUNT ?= 6
BENCH_OUT ?= $(or $(TMPDIR:%/=%),/tmp)/link-bench.txt
BENCH_SET := '^BenchmarkThroughput$$/^tr=(tcp|tls)$$/^size=(64|65536)$$/^sessions=8$$/^dir=oneway$$' '^BenchmarkLatency$$/^tr=tcp$$/^held$$'
.PHONY: bench
bench:
	@: >"$(BENCH_OUT)"; for p in $(BENCH_SET); do \
		GOWORK=off go test -run '^$$' -bench "$$p" -benchmem -count $(BENCH_COUNT) -timeout 30m . >>"$(BENCH_OUT)" || { cat "$(BENCH_OUT)"; exit 1; }; \
	done; cat "$(BENCH_OUT)"; echo "bench: $(BENCH_OUT)"

# profile-baseline: the profile baseline of this commit (scripts/perf.sh
# baseline: every benchmark of link, pathswitch and quiclink, -benchmem
# -count 6, then cpu, heap, mutex, block, goroutine and goroutine leak
# profiles of each, and manifest.json) into OUT (TMPDIR's by default: a CI
# runner's `ci-request TARGET=profile-baseline` keeps it in the result's
# out). Run at every v1+ tag on the box and on the Mac; docs/performance.md.
.PHONY: profile-baseline
profile-baseline: OUT = $(or $(TMPDIR:%/=%),/tmp)/link-profile-baseline
profile-baseline:
	./scripts/perf.sh baseline "$(OUT)"

# The soak (soak/, a module of its own; SOAK.md): link over TCP and
# quiclink over UDP, both directions, under faults, for SOAKTIME (default
# 1h); the JSON summary goes to SOAKOUT.
SOAKTIME ?= 1h
SOAKOUT ?= $(CURDIR)/soak/out/soak.json
.PHONY: soak check-soak
soak:
	cd soak && GOWORK=off SOAKTIME=$(SOAKTIME) SOAKOUT=$(SOAKOUT) go test -run TestSoak -timeout 0 -count=1 -v .

# The soak module builds and vets (its test runs only with SOAKTIME).
check-soak:
	cd soak && GOWORK=off go vet ./...

CI_WORKFLOW_FILE := .github/workflows/check.yml
# ci: what the GitHub workflow runs on a push (CI_WORKFLOW_FILE), run here.
# GitHub Actions is not a gate; this is, before a push (the pre-push hook)
# and after it (nfltr's Linux CI runner). scripts/ci.sh reads the steps from
# the workflow file itself, so the two cannot drift; a pass on a clean tree
# is stamped and not rerun for the same tree.
# hooks: install the pre-push hook (scripts/pre-push): a push to main or a
# tag needs nfltr's Linux runner's green result for that sha; bypass only with
# NFLTR_CI_BYPASS=<reason> (logged to <git-common-dir>/ci-bypass.log).
.PHONY: ci hooks
ci:
	@CI_WORKFLOW=$(CI_WORKFLOW_FILE) ./scripts/ci.sh
hooks:
	@h="$$(git rev-parse --git-path hooks)/pre-push"; \
	if [ -e "$$h" ] && ! grep -q 'scripts/pre-push' "$$h"; then echo "hooks: $$h exists and is not this one; move it away first" >&2; exit 1; fi; \
	printf '#!/bin/sh\n# make hooks: scripts/pre-push of the checkout pushing (else of %s)\ns="$$(git rev-parse --show-toplevel)/scripts/pre-push"\n[ -x "$$s" ] || s=%s/scripts/pre-push\nexec "$$s" "$$@"\n' "$(CURDIR)" "$(CURDIR)" >"$$h" && chmod +x "$$h" && echo "hooks: installed $$h"

# The TLA+ models (spec/README.md): the session protocol on its two
# configurations, the Pool and keepAlive's liveness; spec-mutants: every bug put back is caught; spec-traces:
# tests run with -tags linktrace, each trace checked against the model.
# Java and tla2tools.jar: scripts/fetch-tla.sh (cached, sha256-pinned).
.PHONY: spec spec-invariants spec-mutants spec-traces spec-nightly spec-nightly-wide
spec:
	./scripts/spec.sh check

# spec-invariants: spec without the session protocol's liveness properties,
# Refines only on LinkCrash.cfg (CI).
spec-invariants:
	./scripts/spec.sh invariants

spec-mutants: test-cicache test-fetch-tla
	./scripts/spec.sh mutants

spec-traces:
	./scripts/spec.sh traces

# Early-exit pipes under pipefail (scripts/check-grepq.sh, a copy of
# nfltr's, kept identical): `producer | grep -q` reads SIGPIPE (141) as "not
# found"; the self-test reproduces the race.
.PHONY: check-grepq
check-grepq:
	./scripts/check-grepq.sh self-test
	./scripts/check-grepq.sh
# Spec drift (scripts/check-spec-drift.sh, a copy of nfltr's, kept
# identical; its test is nfltr's scripts/check_spec_drift_test.sh): every
# spec README's code-map entry exists, and the commits since origin/main
# that change a file a code map lists change that spec too or carry
# `Spec-Reviewed: <spec> <reason>`. The pre-push hook checks the pushed range.
.PHONY: check-spec-drift
check-spec-drift:
	./scripts/check-spec-drift.sh maps
	./scripts/check-spec-drift.sh ci

# spec-nightly: spec, then Link.cfg with its crash (invariants and Refines,
# 45 million states); spec-nightly-wide: the wider Pool and Link
# configurations. Two jobs of the Linux runner's CI_NIGHTLY, each inside
# its 90 min bound.
spec-nightly: spec
	./scripts/spec.sh nightly

spec-nightly-wide:
	./scripts/spec.sh nightly-wide

# scripts/cicache.sh (the same file in every nfltr repository): the result
# cache of TLC runs and ProVerif models; a hit skips a check whose inputs
# did not change. Its soundness test, ahead of the first check using it.
.PHONY: test-cicache
test-cicache:
	./scripts/cicache_test.sh

# scripts/fetch-tla.sh: concurrent fetchers sharing one cache never see
# another's partial download; a good cache is used with no fetch.
.PHONY: test-fetch-tla
test-fetch-tla:
	./scripts/fetch-tla_test.sh
