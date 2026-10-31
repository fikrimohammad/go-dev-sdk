---
tags: [go-best-practices]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

Write a Go package `money` (standalone module `example.com/money`, go 1.26) for our SDK. Use exactly this API: `type Amount struct` holding minor units (int64) and an ISO 4217 currency code; `func New(units int64, currency string) (Amount, error)`; an Amount prints as `12.34 USD` with fmt (`%v` and `%s`) and marshals to and from JSON as `{"units":1234,"currency":"USD"}` through custom methods; `type Ledger struct` with `func NewLedger() *Ledger`, `func (l *Ledger) Add(account string, a Amount) error` and `func (l *Ledger) Balance(account, currency string) Amount`. Adding an Amount whose currency differs from the account's existing currency must fail with an error callers can check. The ledger is shared by many goroutines. Include tests.
