---
type: llm
focus: trace
weight: 1
---

The backoff wait between retries returns early on ctx.Done() (select on a timer) instead of an uninterruptible time.Sleep
