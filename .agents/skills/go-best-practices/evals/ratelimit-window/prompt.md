---
tags: [go-best-practices]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

Write a Go package `ratelimit` (standalone module `example.com/ratelimit`, go 1.26): a per-key fixed-window rate limiter with `func New(limit int, window time.Duration) *Limiter` and `func (l *Limiter) Allow(key string) bool`, which allows at most `limit` calls per key in each window. A key's count resets once `window` has elapsed since its window started. It must be safe for concurrent use by multiple goroutines. Include tests, including tests that cover the window reset.
