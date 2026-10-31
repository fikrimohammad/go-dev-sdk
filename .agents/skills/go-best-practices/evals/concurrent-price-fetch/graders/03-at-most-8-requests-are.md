---
type: llm
focus: trace
weight: 1
---

At most 8 requests are in flight at once (errgroup SetLimit, a semaphore, or a fixed worker pool)
