# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

GOVULNCHECK_VERSION := v1.8.0

.PHONY: check build vet test test-race fuzz-short check-gosec check-semgrep check-govulncheck
check: build vet test-race fuzz-short check-gosec check-semgrep check-govulncheck

build:
	go build ./...

vet:
	go vet ./...

test:
	go test -count=1 ./...

test-race:
	go test -count=1 -race ./...

# FUZZTIME per target (default 10s).
fuzz-short:
	./scripts/fuzz-short.sh

# Pinned SAST gated by scripts/{gosec,semgrep}-baseline.txt (semgrep needs docker).
check-gosec:
	./scripts/check-gosec.sh

check-semgrep:
	./scripts/check-semgrep.sh

# Needs network (vuln.go.dev). Standard library only: any reachable
# vulnerability is the toolchain's; update Go.
check-govulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...
