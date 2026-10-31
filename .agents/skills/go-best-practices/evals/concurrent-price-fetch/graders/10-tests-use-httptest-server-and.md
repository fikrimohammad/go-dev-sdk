---
type: llm
focus: trace
weight: 1
---

Tests use httptest.Server and never call t.Fatal, t.FailNow, or t.Fatalf from inside the HTTP handler or another spawned goroutine
