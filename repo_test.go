// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestModuleBoundary: the module links the standard library and itself,
// nothing else, so it can be used and moved anywhere.
func TestModuleBoundary(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for p := range strings.FieldsSeq(string(out)) {
		if p != "github.com/nfltr/link" && !strings.HasPrefix(p, "github.com/nfltr/link/") {
			t.Errorf("links %s: the module imports the standard library only", p)
		}
	}
}

func TestSourceFilesCarryLicenseHeader(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || d.Name() == ".git") {
			return filepath.SkipDir
		}
		switch filepath.Ext(path) {
		case ".go", ".sh":
		default:
			if filepath.Base(path) != "Makefile" {
				return nil
			}
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := strings.SplitN(string(data), "\n", 5)
		if !strings.Contains(strings.Join(lines[:min(4, len(lines))], "\n"), "SPDX-License-Identifier: Apache-2.0") {
			t.Errorf("%s: no SPDX-License-Identifier: Apache-2.0 header in its first lines", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
