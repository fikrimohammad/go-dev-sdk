---
tags: [go-best-practices]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

Write a Go package `tags` (standalone module `example.com/tags`, go 1.26) for parsing and merging resource tags. Use exactly this API: `type Tag struct { Key, Value string }`; `func Parse(s string) ([]Tag, error)` parses `key=value,key2=value2` (keys are trimmed and must be non-empty, empty input means no tags, malformed input is an error); `func Merge(base, defaults []Tag) []Tag` returns base plus every default whose Key is not already in base, keeping base order; and `type Set struct` with `func NewSet(tags []Tag) *Set` and `func (s *Set) Tags() []Tag`. Callers often build their slices with spare capacity, e.g. `make([]Tag, 0, 16)`, and reuse them. Include tests.
