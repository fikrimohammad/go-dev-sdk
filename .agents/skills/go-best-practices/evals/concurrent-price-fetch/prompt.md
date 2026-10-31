---
tags: [go-best-practices]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

Write a Go package `pricing` with a function that takes a list of product IDs and fetches each product's price from our pricing service over HTTP (GET {baseURL}/v1/prices/{id}, which returns JSON like {"product_id":"p1","price_cents":1299}). Run the requests concurrently with at most 8 in flight, return all prices keyed by product ID, and if any request fails, fail the whole call and stop the remaining requests. Include tests. Standalone module `example.com/pricing`, go 1.24.
