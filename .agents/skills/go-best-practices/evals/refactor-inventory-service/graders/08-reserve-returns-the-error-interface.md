---
type: llm
focus: trace
weight: 1
---

Reserve returns the error interface rather than *InventoryError, and callers can still identify the failure via errors.Is or errors.As
