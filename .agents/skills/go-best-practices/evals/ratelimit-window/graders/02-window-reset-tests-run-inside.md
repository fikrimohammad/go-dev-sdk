---
type: llm
focus: trace
weight: 1
---

Window-reset tests run inside synctest.Test (or an injected fake clock) and never call time.Sleep on the real clock
