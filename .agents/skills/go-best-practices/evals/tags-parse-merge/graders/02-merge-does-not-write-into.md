---
type: llm
focus: trace
weight: 1
---

Merge does not write into the caller's base backing array when base has spare capacity (grader probe: make([]Tag, 1, 10) is untouched past len)
