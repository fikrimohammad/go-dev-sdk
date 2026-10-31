---
type: llm
focus: trace
weight: 1
---

Bounded concurrency uses errgroup.WithContext with SetLimit, with no hand-rolled channel semaphore and no labeled break/continue
