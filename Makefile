# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

GOVULNCHECK_VERSION := v1.8.0

.PHONY: check build vet test test-race fuzz-short fuzz-long check-gosec check-semgrep check-govulncheck
check: build vet test-race fuzz-short check-gosec check-semgrep check-govulncheck

build:
	go build ./...

vet:
	go vet ./...

test:
	go test -count=1 ./...

test-race:
	go test -count=1 -race ./...

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

# peerlink over gRPC (peerlink/grpclink, a module of its own: gRPC never
# enters link's dependencies): build, vet, the conformance suite with -race.
.PHONY: check-grpclink
check-grpclink:
	cd peerlink/grpclink && GOWORK=off go build ./... && GOWORK=off go vet ./... && GOWORK=off go test -count=1 -race ./...

# peerlink over QUIC (peerlink/quiclink, a module of its own: quic-go never
# enters link's dependencies): build, vet, the conformance suite with -race.
.PHONY: check-quiclink
check-quiclink:
	cd peerlink/quiclink && GOWORK=off go build ./... && GOWORK=off go vet ./... && GOWORK=off go test -count=1 -race ./...

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
