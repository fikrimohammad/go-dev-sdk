---
name: opentelemetry-semconv
description: Use when adding, changing, or reviewing OpenTelemetry traces or metrics in this repository, including span names and kinds, metric names and instruments, units, attributes, cardinality, and semantic-convention naming.
whenToUse: Load before touching tracer or metrics code, span names, metric names, telemetry attributes, units, or dashboards.
---

# OpenTelemetry semantic conventions

Use this skill whenever you add or change tracing or metrics instrumentation. The invariants in the repository `AGENTS.md` still apply.

## Normative references

- Traces: [general trace conventions](https://opentelemetry.io/docs/specs/semconv/general/trace/)
- Metrics: [general metrics conventions](https://opentelemetry.io/docs/specs/semconv/general/metrics/)
- Names: [naming conventions](https://opentelemetry.io/docs/specs/semconv/general/naming/)

The general pages index domain-specific conventions. Always also use the convention for the instrumented technology:

- Database: [DB spans](https://opentelemetry.io/docs/specs/semconv/db/database-spans/) and [DB metrics](https://opentelemetry.io/docs/specs/semconv/db/database-metrics/)
- HTTP: [HTTP spans](https://opentelemetry.io/docs/specs/semconv/http/http-spans/) and [HTTP metrics](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/)
- Messaging: [messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) and [messaging metrics](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
- RPC: [RPC spans](https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/) and [RPC metrics](https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/)
- Object stores: [object store conventions](https://opentelemetry.io/docs/specs/semconv/object-stores/)
- Exceptions: [exception span conventions](https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-spans/)

If a convention defines a span name, attribute, metric name, unit, instrument type, or span kind for an operation, use it exactly. Do not invent a parallel convention.

## Traces

- Create one span per meaningful operation. Follow the domain convention for batching and retries.
- Span names must follow the domain convention and stay low-cardinality. Do not interpolate request IDs, user IDs, URLs, query text, timestamps, or other unbounded values; include a target only when the convention defines one.
- Always set the span kind explicitly: client for outbound calls, server for inbound calls, producer/consumer for messaging, and internal for in-process work.
- Set span status Error when the operation fails, with a short description. Leave Unset on success unless there is a specific reason to set Ok. Never set Error for a successful outcome.
- Record failures with the registered `error.type` attribute. Its value must be a low-cardinality class (such as a SQLSTATE, driver error code, or canonical exception type), not a free-form error message. Use exception events or `exception.*` attributes where the convention calls for them.
- Use registered semantic attribute keys. Do not invent unnamespaced keys.
- Never record credentials, tokens, API keys, session cookies, or full request or response bodies in span names, span attributes, metric names, or metric attributes.
- Start spans from the caller's context, set attributes before ending, and ensure `span.End()` runs on every path (`defer span.End()` is preferred).
- Keep instrumentation scope names stable and package-specific (for example, `db.query` or `redis.client`), not per-call.
- Opt-in attributes (for example `db.query.text`) are disabled by default and must be sanitized and size-limited. If the metrics API has no explicit opt-in switch, keep them on spans only.

## Metrics

- Use the standard metric name, instrument type, unit, and attributes when a convention defines them. If no standard metric exists, define a custom metric under the application or system namespace, not under an existing semantic convention namespace.
- `observability/metrics.Count` is a monotonic Counter. Do not use it for values that can decrease.
- `observability/metrics.Histogram` is a distribution. Do not use it for a current gauge-like value such as pool size, active connections, or queue depth. If a gauge or UpDownCounter is needed, extend the metrics API first; do not simulate one with Count or Histogram.
- Follow UCUM units: durations in seconds (`s`), utilization as the dimensionless `1`, and counts with singular annotations such as `{request}` or `{message}`. Do not repeat the unit in the metric name.
- Use hierarchical, dot-separated namespaces. Keep common attributes consistently named, and make aggregations over attributes meaningful.
- Metric attributes must be low-cardinality and stable. Do not record request IDs, user IDs, session IDs, URLs, query text, timestamps, or other unbounded values as metric attributes by default. Opt-in attributes require an explicit configuration switch, sanitization, and size limits; without that switch they must stay on spans.
- If an UpDownCounter is added, increments and decrements must use the same attribute values so they land on the same timeseries.

## Names

- Use lowercase, dot-delimited namespaces with snake_case components, for example `db.client.operation.duration`, `http.server.request.duration`, and `messaging.client.sent.messages`.
- Use idiomatic spellings such as `postgresql` and `jsonrpc`, not `postgre_sql` or `json_rpc`.
- Do not reuse a metric, attribute, or event name for a different meaning.
- Do not use the reserved `otel.*` namespace for custom names.
- Custom names must be prefixed with a unique reverse-domain or application namespace. Do not claim an existing semantic-convention namespace for a custom metric or attribute.
- Metric names must not append `_total`. Do not pluralize namespaces or UpDownCounter names. Pluralize only when the instrument counts discrete quantities and uses a non-unit unit.
- Use `duration` for operation-duration histograms, `time` for cumulative time, `usage`, `limit`, and `utilization` for resource amounts, and `io` for data flow.
- Client/server metrics use `{area}.{client|server}.{metric_name}` when the side is not already clear from the area or metric name.
- System-specific metrics and attributes use the system name as their root namespace (for example, `postgresql.*`). The value of the corresponding `db.system.name` or `messaging.system.name` attribute must match that system name.
- Attribute names are singular for one entity and plural, with array values, for multiple entities. Use the `_ref` suffix only for external-content references.
- Treat emitted metric names and attribute keys as public API. Do not rename or remove them as drive-by cleanup; require an explicit migration or deprecation plan and update the release notes.

## Legacy names

Some existing instrumentation predates these rules, including generic fixed span names and pre-semconv metric names. Treat that code as legacy. New instrumentation must comply, and do not copy legacy names into new code. Migrating legacy telemetry names is a breaking change for dashboards and alerts, so do it only as an explicit, documented change with updated tests and release notes.

## Verification

- Check every new or changed span against the domain span convention: name, kind, status, attributes, and cardinality.
- Check every new or changed metric against the domain metric convention and the naming rules: name, instrument type, unit, attributes, and cardinality.
- Update telemetry tests whenever a span or metric name, unit, attribute, or instrument type changes.
- Reject instrumentation that records unbounded values in span names or metric attributes.

Before finishing, confirm:

- [ ] New or changed spans follow the trace semantic conventions, including the relevant domain convention, span kind, status, and cardinality.
- [ ] New or changed metrics follow the metrics and naming conventions, including instrument type, unit, and low-cardinality attributes.
- [ ] Telemetry tests assert the emitted names, attributes, and units.
