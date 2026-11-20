// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"os"
	"testing"

	"github.com/nfltr/link/internal/benchprof"
)

// TestMain: the exit profiles of scripts/perf.sh baseline.
func TestMain(m *testing.M) { os.Exit(benchprof.Run(m)) }
