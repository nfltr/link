// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var goBlock = regexp.MustCompile("(?s)```go\n(.*?)```")

// TestDocCodeBlocksAreCompiled: every Go block in the repository's
// Markdown is a verbatim excerpt of a Go file here (an Example, run by go
// test, or the source itself), so a doc cannot show code that does not
// build or no longer runs. Change the Example, then paste it.
func TestDocCodeBlocksAreCompiled(t *testing.T) {
	var sources []string
	var docs []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "testdata") {
			return filepath.SkipDir
		}
		switch filepath.Ext(path) {
		case ".go":
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sources = append(sources, string(b))
		case ".md":
			docs = append(docs, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	blocks := 0
	for _, doc := range docs {
		b, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range goBlock.FindAllStringSubmatch(string(b), -1) {
			blocks++
			if !containsAny(sources, m[1]) {
				t.Errorf("%s: a Go block that is no Go file's text (keep it a verbatim excerpt of an Example):\n%s", doc, m[1])
			}
		}
	}
	if blocks == 0 {
		t.Fatal("no Go blocks found: the test is not looking where the docs are")
	}
}

func containsAny(sources []string, s string) bool {
	for _, src := range sources {
		if strings.Contains(src, s) {
			return true
		}
	}
	return false
}
