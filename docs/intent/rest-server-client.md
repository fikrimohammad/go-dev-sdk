# Intent: rest/server + rest/client

Confirmed with the owner on 2026-10-31. Design details: [docs/ideas/rest-server-client.md](../ideas/rest-server-client.md).

- **Outcome:** `rest/server` and `rest/client` in one `rest` module, replacing
  `apiserver`. `rest/client` also provides
  `client.RoundTripper(hc) http.RoundTripper`, backed by the same Hertz client
  and middleware. SDK-based calls (the payment gateway) and hand-written calls
  (the tax API) share one implementation of retry, timeouts, and telemetry,
  and teams write no conversion code. The server's standard layers are fixed;
  teams add their own middleware (such as webhook signature checks) in a
  documented slot.
- **User:** domain teams' use-case-specific gateway services, behind a cloud
  load balancer and nginx. They serve frontends and vendor webhooks and call
  vendors for internal RPC services. Owned by the principal engineer for
  developer experience.
- **Why:** each team has its own idea of how a REST server and client should
  work. Security fixes should happen once, and teams should focus on business
  logic.
- **Success:** the same metric names and attributes everywhere, including
  payment gateway SDK traffic, so the alert repo's templates work unchanged. A
  security fix is a version bump.
- **Constraint:** the metric contract is public API. On the Hertz client: the
  ctx deadline becomes the request timeout; timeouts satisfy
  `errors.Is(err, context.DeadlineExceeded)`; one span per attempt. At the
  edge: the server always starts a new root span and links to any incoming
  `traceparent`.
- **Out of scope:** circuit breaker; alert rules and dashboards (they live in
  the other repo); handling an ambiguous POST outcome (the domain team's job);
  webhook signature checks (team middleware); `TrustIncomingTrace`;
  `apiserver` and `cron/server` (deleted; `apiserver` hotfixes come off
  `apiserver/v1.4.1`, and `cron/server` is rebuilt on RPC later);
  `http.server.request.count`; streaming bodies through the `RoundTripper`.
