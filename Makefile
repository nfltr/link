# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

GOVULNCHECK_VERSION := v1.8.0

.PHONY: check build vet test test-race test-linkdebug test-ci fuzz-short fuzz-long check-gosec check-semgrep check-govulncheck
check: build vet test-ci check-gosec check-semgrep check-govulncheck

# test-race, test-linkdebug and fuzz-short at once, each part's log printed
# whole after all end (scripts/test-ci.sh).
test-ci:
	./scripts/test-ci.sh

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
.PHONY: check-api
check-api:
	./scripts/check-api.sh . '' link
	./scripts/check-api.sh peerlink/quiclink peerlink/quiclink/ quiclink
	./scripts/check-api.sh peerlink/grpclink peerlink/grpclink/ grpclink

# peerlink over gRPC (peerlink/grpclink, a module of its own: gRPC never
# enters link's dependencies): build, vet, the conformance suite with -race.
.PHONY: check-grpclink
check-grpclink:
	cd peerlink/grpclink && GOWORK=off go build ./... && GOWORK=off go vet ./... && GOWORK=off go test -count=1 -race ./...

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

# link vs yamux (compare/, a module of its own; its README). OUT holds the
# JSON lines; compare-docker needs docker with NET_ADMIN (about an hour).
OUT ?= $(CURDIR)/compare/out
.PHONY: compare compare-docker compare-summary
compare:
	cd compare && GOWORK=off python3 rig.py host $(OUT)

compare-docker:
	cd compare && GOWORK=off python3 rig.py docker $(OUT)

compare-summary:
	cd compare && python3 rig.py summarize $(OUT)

# link, yamux and muxado with their defaults, as Go benchmarks
# (compare/defaults/README.md): check-compare vets the compare module (CI);
# compare-defaults runs the measured passes into COMPARE_OUT.
COMPARE_OUT ?= $(CURDIR)/compare/out/defaults
.PHONY: check-compare compare-defaults
check-compare:
	cd compare && GOWORK=off go vet ./...

compare-defaults:
	compare/defaults/run.sh $(COMPARE_OUT)

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
# hooks: install the pre-push hook (scripts/pre-push): `make ci` on the
# exact commit pushed to main or a tag; bypass only with
# NFLTR_CI_BYPASS=<reason> (logged to <git-common-dir>/ci-bypass.log).
.PHONY: ci hooks
ci:
	@CI_WORKFLOW=$(CI_WORKFLOW_FILE) ./scripts/ci.sh
hooks:
	@h="$$(git rev-parse --git-path hooks)/pre-push"; \
	if [ -e "$$h" ] && ! grep -q 'scripts/pre-push' "$$h"; then echo "hooks: $$h exists and is not this one; move it away first" >&2; exit 1; fi; \
	printf '#!/bin/sh\n# make hooks: scripts/pre-push of the checkout pushing (else of %s)\ns="$$(git rev-parse --show-toplevel)/scripts/pre-push"\n[ -x "$$s" ] || s=%s/scripts/pre-push\nexec "$$s" "$$@"\n' "$(CURDIR)" "$(CURDIR)" >"$$h" && chmod +x "$$h" && echo "hooks: installed $$h"
