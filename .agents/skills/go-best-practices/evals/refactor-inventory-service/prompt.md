---
tags: [go-best-practices]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

A teammate wrote this inventory package (go.mod, inventory.go, inventory_test.go) and asked me to get it into shape before we merge - make it idiomatic, best-practice Go. Behavior should stay the same from a caller's point of view except where the current API is actually broken, and the tests must still pass (update them as needed). Write a short CHANGES.md listing what you changed and why.

Input files: go.mod, inventory.go, inventory_test.go
