package main

import (
	"path/filepath"
	"testing"
)

func TestCheck(t *testing.T) {
	bad, err := check([]string{filepath.Join("testdata", "bad")})
	if err != nil {
		t.Fatalf("check(bad) = %v", err)
	}
	fired := map[string]bool{}
	for _, f := range bad.findings {
		fired[f.rule] = true
	}
	for rule := range ruleDocs {
		if !fired[rule] {
			t.Errorf("check(testdata/bad) did not report rule %q", rule)
		}
	}

	good, err := check([]string{filepath.Join("testdata", "good")})
	if err != nil {
		t.Fatalf("check(good) = %v", err)
	}
	for _, f := range good.findings {
		t.Errorf("check(testdata/good) reported %s: %s %s", f.pos, f.rule, f.msg)
	}
}
