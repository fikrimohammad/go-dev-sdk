# nats

[NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) producer and consumer wrappers
with middleware (panic recovery → tracer → logger → metrics) and full
OpenTelemetry telemetry, including cross-process trace propagation via message
headers. Built using the modern, official `github.com/nats-io/nats.go/jetstream` API.

Two subpackages:

- `github.com/fikrimohammad/go-dev-sdk/nats/producer` — publish messages
  sync, sync-with-delay (via NATS 2.12+ `Nats-Schedule`), or async; multiple
  producers keyed by topic (stream).
- `github.com/fikrimohammad/go-dev-sdk/nats/consumer` — register durable
  consumers per (topic, group) with tag/subject selectors and a middleware chain.

## Features

- **Lifecycle-managed** — register topics/groups first, `Start` all, `Shutdown`
  all; both are idempotent.
- **Panic-safe consumers** — panics are recovered, logged with stack, and
  converted into errors so the message is negatively acknowledged (`Nak`) and redelivered.
- **Trace propagation** — producers inject the W3C trace context into message
  headers (`traceparent`, `baggage`); consumers extract it, preserving trace continuity.
- **Telemetry** — messaging-semantic-convention spans and metrics
  (`messaging.publish.messages`, `messaging.process.messages`, and duration
  histograms), with per-message attributes (topic/stream, tag, subject, keys, message id, body
  size).
- **Consume retry** — a `HandlerFunc` returning an error issues a `Nak` (or `NakWithDelay` when
  `RetryBackoff` is configured) so the broker redelivers.
- **Native scheduling** — `PublishSyncWithDelay` uses NATS 2.12+ server-side message
  scheduling (`Nats-Schedule: @at <RFC3339>`).

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/nats/producer
go get github.com/fikrimohammad/go-dev-sdk/nats/consumer
```

## Producer

### Step-by-step

1. **Create** with the service identity:

```go
p, err := producer.New(appinfo.Default())
if err != nil { /* handle */ }
```

2. **Register** a producer per topic (stream):

```go
err = p.Register(producer.Config{
    Endpoints:        []string{"nats://127.0.0.1:4222"},
    Topic:            "order_events", // JetStream stream name
    Timeout:          10 * time.Second,
    MaxRetryAttempts: 3,
})
```

3. **Start** all registered producers (ensures stream exists with scheduling enabled):

```go
if err := p.Start(); err != nil { /* handle */ }
```

4. **Publish**:

```go
// Synchronous (publishes to subject "{topic}.{tag}", e.g. "order_events.created").
err := p.PublishSync(ctx, "order_events", "created", "order-42", payload)

// Delayed (scheduled on NATS 2.12+ server via Nats-Schedule header).
err := p.PublishSyncWithDelay(ctx, "order_events", "reminder", "order-42", payload, 1*time.Hour)

// Asynchronous — callback receives the PubAck or error.
err := p.PublishAsync(ctx, "order_events", "created", "order-42", payload,
    func(ctx context.Context, ack *jetstream.PubAck, err error) {
        if err != nil { /* handle */ }
    },
)
```

5. **Shutdown**:

```go
if err := p.Shutdown(); err != nil { /* handle */ }
```

## Consumer

### Step-by-step

1. **Create** with the service identity:

```go
c, err := consumer.New(appinfo.Default())
if err != nil { /* handle */ }
```

2. **Register** a handler per (topic, group):

```go
err = c.Register(consumer.Config{
    Endpoints: []string{"nats://127.0.0.1:4222"},
    Topic:     "order_events", // JetStream stream name
    Group:     "order-processor", // Durable name suffix: {appName}_{topic}_{group}
    Tags:      []string{"created", "updated"}, // default: ["*"]
    Timeout:   60 * time.Second, // AckWait
    MaxRetryAttempts: 5,         // MaxDeliver
    MaxConcurrent:    10,
    // Optional: ConsumeFromWhere ("last", "first", "timestamp"),
    // ConsumeTimestamp (RFC3339), RetryBackoff (NakWithDelay duration).
}, func(ctx context.Context, body []byte) error {
    var evt OrderEvent
    if err := json.Unmarshal(body, &evt); err != nil {
        return err // → message is NAK'd and redelivered
    }
    return handleOrder(ctx, evt)
})
```

3. **Start** consuming:

```go
if err := c.Start(); err != nil { /* handle */ }
```

4. **Shutdown**:

```go
if err := c.Shutdown(); err != nil { /* handle */ }
```

## Telemetry

- **Producer spans**: `"{topic} publish"` with `SpanKindProducer`; message key / id
  attached on completion. Metrics:
  `messaging.publish.messages` (count), `messaging.publish.duration`
  (histogram).
- **Consumer spans**: `"{topic} process"` with `SpanKindConsumer`; attributes
  include message id, tag, subject, body size, and `messaging.nats.client_group`.
  Metrics: `messaging.process.messages`, `messaging.process.duration`.
- **Trace propagation**: W3C context injected by producers and extracted by
  consumers via message headers (`traceparent`).
- Inject `WithMetrics` / `WithTracer` on `New` to override the package-level
  defaults.

## API reference

### producer

| Symbol | Description |
| --- | --- |
| `New(info, opts...)` | Creates an empty `*Producer` |
| `(*Producer).Register(cfg)` | Builds a producer for `cfg.Topic` |
| `PublishSync(ctx, topic, tag, key, msg)` | Synchronous send |
| `PublishSyncWithDelay(ctx, topic, tag, key, msg, delay)` | Delayed send via `Nats-Schedule` |
| `PublishAsync(ctx, topic, tag, key, msg, cb)` | Async send with callback |
| `(*Producer).Start()` / `(*Producer).Shutdown()` | Idempotent lifecycle |
| `Config` | `Endpoints`, `Topic`, `Timeout` (10s), `MaxRetryAttempts` (3), optional auth |
| `Client` | Send surface for dependency injection |
| `ErrProducerExists` / `ErrProducerNotFound` / `ErrEndpointsRequired` / `ErrTopicRequired` / `ErrInvalidTopic` / `ErrInvalidTag` | Sentinels |

### consumer

| Symbol | Description |
| --- | --- |
| `New(info, opts...)` | Creates an empty `*Consumer` |
| `(*Consumer).Register(cfg, handler)` | Registers a consumer; handlers must be registered **before** `Start` |
| `(*Consumer).Start()` / `(*Consumer).Shutdown()` | Idempotent lifecycle |
| `HandlerFunc` | `func(ctx context.Context, body []byte) error`; error ⇒ NAK & redeliver |
| `Config` | `Endpoints`, `Topic`, `Group`, `Tags`, `Timeout` (60s), `MaxRetryAttempts` (5), `MaxConcurrent` (10), `ConsumeFromWhere`, `ConsumeTimestamp`, `RetryBackoff` |
| `ErrConsumerExists` / `ErrConsumerNotFound` / `ErrInvalidAppName` + config sentinels (`ErrInvalidTopic`, `ErrInvalidGroup`, `ErrInvalidTag`, etc.) | Matching via `errors.Is` |

## Naming conventions and dot restrictions

- **Strict no-dots rule across all identifiers**:
  For consistency and compatibility across NATS JetStream entities (streams and durable consumers), dots (`.`) are strictly disallowed in all messaging identifiers. Use underscores (`_`) or hyphens (`-`):
  - **Topic**: If `producer.Config.Topic` or `consumer.Config.Topic` contains `.`, `Register()` returns `ErrInvalidTopic`.
  - **Group**: If `consumer.Config.Group` contains `.`, `Register()` returns `ErrInvalidGroup`.
  - **Tag**: If `producer.Publish*` or `consumer.Config.Tags` contains `.`, an `ErrInvalidTag` is returned.
  - **App Name**: If `appinfo.Info.Name` contains `.`, `consumer.New()` returns `ErrInvalidAppName`.

- **Deduplication and Tracking**:
  - Message deduplication key is carried via the standard `Nats-Msg-Id` header.
  - Tag is carried via the `Nats-Msg-Tag` header and subject routing (`{topic}.{tag}`).


