# rocketmq

[Apache RocketMQ](https://rocketmq.apache.org/) producer and consumer wrappers
with middleware (panic recovery → tracer → logger → metrics) and full
OpenTelemetry telemetry, including cross-process trace propagation via message
properties.

Two subpackages:

- `github.com/fikrimohammad/go-dev-sdk/rocketmq/producer` — publish messages
  sync, sync-with-delay, or async; multiple producers keyed by topic.
- `github.com/fikrimohammad/go-dev-sdk/rocketmq/consumer` — register
  push-consumers per (topic, group) with tag selectors and a middleware chain.

## Features

- **Lifecycle-managed** — register topics/groups first, `Start` all, `Shutdown`
  all; both are idempotent.
- **Panic-safe consumers** — panics are recovered, logged with stack, and
  converted into errors so the message is redelivered.
- **Trace propagation** — producers inject the W3C trace context into message
  properties; consumers extract it, so producer → consumer traces stay on one
  trace.
- **Telemetry** — messaging-semantic-convention spans and metrics
  (`messaging.publish.messages`, `messaging.process.messages`, and duration
  histograms), with per-message attributes (topic, tag, keys, message id, body
  size).
- **Consume retry** — a `HandlerFunc` returning an error asks the broker for
  redelivery (`ConsumeRetryLater`).

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/rocketmq/producer
go get github.com/fikrimohammad/go-dev-sdk/rocketmq/consumer
```

## Producer

### Step-by-step

1. **Create** with the service identity:

```go
p, err := producer.New(appinfo.Default())
if err != nil { /* handle */ }
```

2. **Register** a producer per topic:

```go
err = p.Register(producer.Config{
    Endpoints:        []string{"127.0.0.1:9876"},
    Topic:            "order_events",
    Timeout:          10 * time.Second,
    MaxRetryAttempts: 3,
})
```

3. **Start** all registered producers:

```go
if err := p.Start(); err != nil { /* handle */ }
```

4. **Publish**:

```go
// Synchronous.
err := p.PublishSync(ctx, "order_events", "created", "order-42", payload)

// Delayed (consumable only after the delay).
err := p.PublishSyncWithDelay(ctx, "order_events", "reminder", "order-42", payload, 1*time.Hour)

// Asynchronous — callback receives the result or error.
err := p.PublishAsync(ctx, "order_events", "created", "order-42", payload,
    func(ctx context.Context, result *primitive.SendResult, err error) {
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
    Endpoints: []string{"127.0.0.1:9876"},
    Topic:     "order_events",
    Group:     "order-processor",
    Tags:      []string{"created", "updated"}, // default: ["*"]
    // Optional: ConsumeModel, ConsumeFromWhere, ConsumeTimestamp,
    // ConsumeOrderly, MaxConcurrent, MaxRetryAttempts, flow-control knobs.
}, func(ctx context.Context, body []byte) error {
    var evt OrderEvent
    if err := json.Unmarshal(body, &evt); err != nil {
        return err // → ConsumeRetryLater, message redelivered
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

- **Producer spans**: `"{topic} publish"` with `SpanKindProducer`; message id
  attached on completion. Metrics:
  `messaging.publish.messages` (count), `messaging.publish.duration`
  (histogram).
- **Consumer spans**: `"{topic} process"` with `SpanKindConsumer`; attributes
  include message id, tag, body size, and `messaging.rocketmq.client_group`.
  Metrics: `messaging.process.messages`, `messaging.process.duration`.
- **Trace propagation**: W3C context injected by producers and extracted by
  consumers via message properties.
- Inject `WithMetrics` / `WithTracer` on `New` to override the package-level
  defaults.

## API reference

### producer

| Symbol | Description |
| --- | --- |
| `New(info, opts...)` | Creates an empty `*Producer` |
| `(*Producer).Register(cfg)` | Builds a producer for `cfg.Topic` |
| `PublishSync(ctx, topic, tag, key, msg)` | Synchronous send |
| `PublishSyncWithDelay(ctx, topic, tag, key, msg, delay)` | Delayed send |
| `PublishAsync(ctx, topic, tag, key, msg, cb)` | Async send with callback |
| `(*Producer).Start()` / `(*Producer).Shutdown()` | Idempotent lifecycle |
| `Config` | `Endpoints`, `Topic`, `Timeout` (10s), `MaxRetryAttempts` (3) |
| `Client` | Send surface for dependency injection |
| `ErrProducerExists` / `ErrProducerNotFound` / `ErrEndpointsRequired` / `ErrTopicRequired` | Sentinels |

### consumer

| Symbol | Description |
| --- | --- |
| `New(info, opts...)` | Creates an empty `*Consumer` |
| `(*Consumer).Register(cfg, handler)` | Registers a push consumer; handlers must be registered **before** `Start` |
| `(*Consumer).Start()` / `(*Consumer).Shutdown()` | Idempotent lifecycle |
| `HandlerFunc` | `func(ctx context.Context, body []byte) error`; error ⇒ redeliver |
| `Config` | `Endpoints`, `Topic`, `Group`, `Tags`, `Timeout` (60s), `MaxRetryAttempts` (5), `MaxConcurrent` (10), consume model / start position / orderly / flow control |
| `ErrConsumerExists` / `ErrConsumerNotFound` + config sentinels | Matching via `errors.Is` |

## Notes

- Producer group names are derived from the service identity
  (`{appName}::{topic}`); consumer groups are `{appName}_{topic}_{group}`.
- Async publish spans span the whole send (opened before `SendAsync`, closed in
  the callback).
