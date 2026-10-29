// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package linkvet_test

import (
	"testing"

	"github.com/nfltr/link/linkvet"
	"golang.org/x/tools/go/analysis/analysistest"
)

// Every check against its testdata package: each `// want` is a report it
// must make, and every other line one it must not (the negative cases).
func TestAnalyzers(t *testing.T) {
	for _, a := range linkvet.Analyzers {
		t.Run(a.Name, func(t *testing.T) {
			analysistest.Run(t, analysistest.TestData(), a, a.Name)
		})
	}
}
