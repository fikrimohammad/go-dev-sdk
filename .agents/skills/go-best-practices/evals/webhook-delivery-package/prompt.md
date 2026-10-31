---
tags: [go-best-practices]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

I need a small Go package called `webhook` that delivers JSON events to a subscriber's URL with an HTTP POST. Requirements: sign the request body with HMAC-SHA256 using a shared secret and send it hex-encoded in an `X-Signature` header; retry on 5xx responses and network errors with exponential backoff (initial delay and max attempts configurable, with sensible defaults); stop immediately when the context is cancelled; and callers need to be able to tell a permanent rejection (4xx) apart from running out of retries. Include unit tests. Make it a standalone module (`go mod init example.com/webhook`, go 1.24).
