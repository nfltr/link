// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command linkvet runs linkvet's checks, alone (linkvet ./...) or as go
// vet's tool (go vet -vettool=$(command -v linkvet) ./...); -<check>=false
// turns one off.
package main

import (
	"github.com/nfltr/link/linkvet"
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() { multichecker.Main(linkvet.Analyzers...) }
