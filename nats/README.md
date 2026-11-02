# nats

Wrappers around [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) for
publishing and consuming messages, with OpenTelemetry tracing and metrics that follow
the messaging semantic conventions. Trace context travels in message headers, so a
consumer span is a child of the producer span. The module uses the official
`github.com/nats-io/nats.go/jetstream` API.

Two packages:

- `github.com/fikrimohammad/go-servicekit/nats/producer` publishes messages synchronously,
  delayed, or asynchronously. One producer owns the stream of each topic.
- `github.com/fikrimohammad/go-servicekit/nats/consumer` runs durable consumers, one per
  (topic, group), that hand each message to a handler.

## Installation

```bash
go get github.com/fikrimohammad/go-servicekit/nats@v0.1.0
```

## Producer

Every package follows the same lifecycle: `New`, then `Register` (validates and stores
only, no I/O), then `Start(ctx)` (connects and ensures the streams), then use, then
`Shutdown(ctx)`.

```go
import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/producer"
)

func publishOrder(ctx context.Context, body []byte) error {
	p, err := producer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		return err
	}

	err = p.Register(producer.Config{
		Endpoints: []string{"nats://127.0.0.1:4222"},
		Topic:     "orders", // the JetStream stream name
		Timeout:   10 * time.Second,
	})
	if err != nil {
		return err
	}

	if err := p.Start(ctx); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = p.Shutdown(shutdownCtx)
	}()

	msg := producer.Message{Topic: "orders", Tag: "created", Key: "order-42", Body: body}

	// Synchronous: returns once the server acknowledges the message. It is
	// published to the subject "orders.created".
	if err := p.PublishSync(ctx, msg); err != nil {
		return err
	}

	// Delayed: delivered once, one hour from now.
	reminder := producer.Message{Topic: "orders", Tag: "reminder", Key: "order-42", Body: body}
	if err := p.PublishSyncWithDelay(ctx, reminder, time.Hour); err != nil {
		return err
	}

	// Asynchronous: the callback receives the PubAck or the error.
	return p.PublishAsync(ctx, msg, func(ctx context.Context, ack *jetstream.PubAck, err error) {
		if err != nil {
			log.Println("publish order-42:", err)
		}
	})
}
```

`producer.Message` has `Topic` (the stream), `Tag` (an optional sub-subject, published
as `Topic.Tag`), `Key` (an optional business key), and `Body`.

### The producer owns the stream

`Start` creates the stream when it is missing, with the SDK subjects (`orders`, `orders.>`,
`schedules.orders.>`) and message schedules enabled. When the stream already exists, `Start`
adds the missing SDK subjects and enables schedules. It never resets any other setting, such
as replicas or max age. Enabling schedules also turns `AllowRollup` on.

`Start` fails with a named error when the existing stream has a setting that is incompatible
with message schedules: `DenyPurge`, `Discard: DiscardNew`, `Mirror`, `Sources`, or
`AllowMsgCounter`. Change the stream, then start again.

### Delayed publish

`PublishSyncWithDelay` sends the message to a schedule subject (`schedules.<topic>.<id>`)
with the data subject as its target. The server delivers the message once, at the delay, on
the data subject. Consumers never receive the schedule message itself. A delay of zero or
less publishes at once, as `PublishSync` does.

### Headers and delivery guarantees

The SDK sets two headers: `Sdk-Msg-Key` (the key) and `Sdk-Msg-Tag` (the tag). It never sets
`Nats-Msg-Id`, so the server never deduplicates by key: two messages with the same key are
both stored and both delivered.

Delivery is at least once in both directions:

- A publish that the caller retries after a timeout can be stored twice.
- A message is redelivered when its handler does not ack within the AckWait, or when the
  handler naks it.

Handlers must therefore be idempotent. Dedupe on `msg.Key()` before a side effect. When
`msg.DeliveryCount() > 1`, the message is a redelivery.

## Consumer

```go
import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/consumer"
)

func runBilling(ctx context.Context) error {
	c, err := consumer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		return err
	}

	err = c.Register(consumer.Config{
		Endpoints: []string{"nats://127.0.0.1:4222"},
		Topic:     "orders",
		Group:     "billing", // the durable consumer is named {appName}_{Topic}_{Group}
		Tags:      []string{"created", "paid"},
	}, func(ctx context.Context, msg consumer.Message) error {
		var order struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(msg.Body(), &order); err != nil {
			// A poison message: terminate it, do not redeliver.
			return fmt.Errorf("%w: decode order: %w", consumer.ErrNoRetry, err)
		}
		// Dedupe on msg.Key() before a side effect: the key can arrive again
		// after a redelivery.
		fmt.Println("billing", order.ID, msg.Key())
		return nil // any other error naks the message
	})
	if err != nil {
		return err
	}

	if err := c.Start(ctx); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Shutdown(shutdownCtx)
	}()

	<-ctx.Done()
	return nil
}
```

`HandlerFunc` has the signature `func(ctx context.Context, msg consumer.Message) error`.
`consumer.Message` exposes `Key()`, `Tag()`, `Subject()`, `Body()`, `Sequence()`,
`DeliveryCount()`, and `Timestamp()`. The values are parsed once, when the message arrives.
A message without JetStream metadata is nak'd and never reaches the handler.

### Outcome of a handler

The error the handler returns decides how the message is settled:

| Returned error | Settlement |
| --- | --- |
| `nil` | Ack. The message is done. |
| matches `consumer.ErrNoRetry` (`errors.Is`) | Term. A poison message is never redelivered. |
| any other error | Nak, delayed by `Config.RetryBackoff` when it is set. |

The handler does not ack, nak, or term by itself, and it does not see those calls.

### Context, timeouts, and heartbeats

- The handler context is derived from the consumer's run context. `Shutdown` cancels it.
- `Config.HandlerTimeout` bounds each call. It defaults to `Config.Timeout`, the AckWait.
- `Config.Timeout` must be at least `consumer.MinTimeout` (100ms), the smallest AckWait the
  heartbeat can serve. `Register` rejects a smaller value with `consumer.ErrInvalidTimeout`.
- While a handler runs, the consumer sends an InProgress heartbeat every `Timeout/2`. A
  handler that runs longer than the AckWait is not redelivered mid-run. The heartbeat stops
  at the `HandlerTimeout` deadline, so a handler that ignores its ctx and outlives it is
  redelivered after AckWait. `HandlerTimeout` still stops a runaway handler.
- `MaxConcurrent` also caps the unacked messages the server hands out (MaxAckPending), so a
  message waiting for a free handler slot is not redelivered.

### Retries and exhaustion

`Config.MaxRetryAttempts` is the JetStream `MaxDeliver`. When a message has been delivered
that many times, JetStream stops redelivering it. The SDK has no dead-letter queue, so the
message is only left in the stream. A dead-letter queue is a separate project. Use
`ErrNoRetry` for messages that are known to be poison.

### Start and Shutdown

`Start(ctx)` connects, ensures the stream, creates or updates the durable consumer, and
starts delivery. The ctx bounds only the setup. Handlers run until `Shutdown`.

`Shutdown(ctx)` cancels the handler contexts first, then stops intake, waits for the running
handlers to return, and drains the connections. It returns `ctx.Err()` if ctx ends first. A
message that arrives after the cancel is nak'd without running the handler. `Shutdown` before a
successful `Start` is a no-op.

### ConsumeFromWhere

- `consumer.ConsumeFromLast` (the default) delivers only the messages that arrive after the
  durable consumer is created. It is the `DeliverNew` policy.
- `consumer.ConsumeFromFirst` delivers from the earliest stored message.
- `consumer.ConsumeFromTimestamp` delivers from `Config.ConsumeTimestamp` (RFC3339).

A durable consumer created by an older version with `DeliverLast` must be deleted once. The
server rejects a change of deliver policy (error 10012), so delete the durable and start again.

## Telemetry

Spans follow the semantic conventions for messaging (`messaging.system` is `nats`).

- Producer span: `publish <topic>`, kind `PRODUCER`. It records the subject, body size, key,
  tag, and the stream sequence from the PubAck.
- Consumer span: `process <topic>`, kind `CONSUMER`, a child of the producer span. It records the
  subject, stream sequence, body size, key, tag, and delivery count.
- `messaging.client.operation.duration` (histogram, s) records every publish outcome.
- `messaging.client.sent.messages` (counter, `{message}`) counts successful publishes.
- `messaging.process.duration` (histogram, s) records every processed message.
- `messaging.client.consumed.messages` (counter, `{message}`) counts every delivery.

Metric attributes are bounded: the system, operation name and type, destination (topic),
consumer group, `server.address` and `server.port`, and `error.type` on failure. No message
id, key, subject, or body size goes on a metric. A failure gets an `error.type` from the
error: the `errs` code, the JetStream API error code, `timeout`, `canceled`, `panic` for a
recovered handler panic, or `_OTHER`.

Propagation uses the W3C `traceparent` and `baggage` headers. Use `WithMetrics` and
`WithTracer` on `New` to override the global OTel providers.

## Testing your code

### Unit-test a handler with a fake message

A handler takes a `consumer.Message`, so a unit test can pass a small fake. The fake needs
seven methods:

```go
import (
	"context"
	"testing"
	"time"

	"github.com/fikrimohammad/go-servicekit/nats/consumer"
)

type fakeMessage struct {
	key, tag, subject string
	body              []byte
	seq, deliveries   uint64
}

func (m fakeMessage) Key() string           { return m.key }
func (m fakeMessage) Tag() string           { return m.tag }
func (m fakeMessage) Subject() string       { return m.subject }
func (m fakeMessage) Body() []byte          { return m.body }
func (m fakeMessage) Sequence() uint64      { return m.seq }
func (m fakeMessage) DeliveryCount() uint64 { return m.deliveries }
func (m fakeMessage) Timestamp() time.Time  { return time.Time{} }

// handleOrder is the consumer.HandlerFunc under test.
func handleOrder(ctx context.Context, msg consumer.Message) error {
	if msg.DeliveryCount() > 1 {
		// A redelivery: the earlier attempt may have run its side effect.
		_ = msg.Key() // check your store for this key before repeating side effects
	}
	return nil
}

func TestHandleOrder(t *testing.T) {
	msg := fakeMessage{key: "order-42", body: []byte(`{"id":"order-42"}`), deliveries: 2}
	if err := handleOrder(t.Context(), msg); err != nil {
		t.Fatalf("handleOrder() = %v, want nil", err)
	}
}
```

### Integration-test with an embedded JetStream server

`nats-server` runs in-process:

```go
import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/consumer"
	"github.com/fikrimohammad/go-servicekit/nats/producer"
)

func TestOrdersEndToEnd(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1 // a free port
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natsserver.RunServer(&opts)
	defer srv.Shutdown()

	endpoints := []string{srv.ClientURL()}

	p, err := producer.New(identity.Info{Name: "orders-test"})
	if err != nil {
		t.Fatalf("producer.New() = %v, want nil", err)
	}
	if err := p.Register(producer.Config{Endpoints: endpoints, Topic: "orders"}); err != nil {
		t.Fatalf("Register() = %v, want nil", err)
	}
	if err := p.Start(t.Context()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	defer func() { _ = p.Shutdown(t.Context()) }()

	got := make(chan consumer.Message, 1)
	c, err := consumer.New(identity.Info{Name: "orders-test"})
	if err != nil {
		t.Fatalf("consumer.New() = %v, want nil", err)
	}
	err = c.Register(consumer.Config{Endpoints: endpoints, Topic: "orders", Group: "billing"},
		func(ctx context.Context, msg consumer.Message) error {
			got <- msg
			return nil
		})
	if err != nil {
		t.Fatalf("consumer Register() = %v, want nil", err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("consumer Start() = %v, want nil", err)
	}
	defer func() { _ = c.Shutdown(t.Context()) }()

	msg := producer.Message{Topic: "orders", Tag: "created", Key: "order-42", Body: []byte("{}")}
	if err := p.PublishSync(t.Context(), msg); err != nil {
		t.Fatalf("PublishSync() = %v, want nil", err)
	}

	select {
	case m := <-got:
		if m.Key() != "order-42" {
			t.Fatalf("Key() = %q, want %q", m.Key(), "order-42")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called within 5s")
	}
}
```

The test is hermetic: it needs no external service and no environment variable. Pass
`Endpoints: []string{srv.ClientURL()}` to any producer or consumer under test.


### Mock the producer

`producer.Client` is the send surface of a producer, an interface with `PublishSync`,
`PublishSyncWithDelay`, and `PublishAsync`. Code that depends on it can take the interface.
Generate a mock with gomock (`mockgen`) in a test-only package, so the production code
stays free of test doubles.

## API reference

### producer

| Symbol | Description |
| --- | --- |
| `New(info, opts...)` | Returns an empty `*Producer`. Rejects a service name with dots (`ErrInvalidAppName`). |
| `(*Producer).Register(cfg)` | Validates and stores the config for `cfg.Topic`. Does no I/O. Fails after `Start` with `ErrAlreadyStarted`. |
| `(*Producer).Start(ctx)` | Connects every registered topic and ensures its stream. Idempotent. |
| `(*Producer).PublishSync(ctx, msg)` | Publishes and waits for the acknowledgement. |
| `(*Producer).PublishSyncWithDelay(ctx, msg, delay)` | Delivers the message once, after `delay`. |
| `(*Producer).PublishAsync(ctx, msg, callback)` | Publishes and calls `callback` with the PubAck or the error. |
| `(*Producer).Shutdown(ctx)` | Waits for pending async publishes, then drains the connections. Bounded by ctx. |
| `Message` | `Topic`, `Tag`, `Key`, `Body`. |
| `Config` | `Endpoints`, `Topic`, `Timeout` (default 10s), optional authentication. |
| `Client` | The send surface, for dependency injection and mocks. |
| `ErrProducerExists`, `ErrProducerNotFound`, `ErrAlreadyStarted`, `ErrEndpointsRequired`, `ErrTopicRequired`, `ErrInvalidTopic`, `ErrInvalidTag`, `ErrInvalidAppName` | Sentinels for `errors.Is`. |

### consumer

| Symbol | Description |
| --- | --- |
| `New(info, opts...)` | Returns an empty `*Consumer`. Rejects a service name with dots (`ErrInvalidAppName`). |
| `(*Consumer).Register(cfg, handler)` | Validates and stores the config and handler. Does no I/O. Fails after `Start` with `ErrAlreadyStarted`. |
| `(*Consumer).Start(ctx)` | Connects, ensures the stream and durable consumer, and starts delivery. The ctx bounds setup only. |
| `(*Consumer).Shutdown(ctx)` | Cancels handler contexts, stops intake, waits, and drains. Returns `ctx.Err()` if ctx ends first. |
| `HandlerFunc` | `func(ctx context.Context, msg consumer.Message) error`. |
| `Message` | The seven accessors: `Key`, `Tag`, `Subject`, `Body`, `Sequence`, `DeliveryCount`, `Timestamp`. |
| `Config` | `Endpoints`, `Topic`, `Group`, `Tags` (default `["*"]`), `Timeout` (AckWait, at least `MinTimeout` = 100ms, default 60s), `HandlerTimeout` (default `Timeout`), `MaxRetryAttempts` (MaxDeliver, default 5), `MaxConcurrent` (default 10), `ConsumeFromWhere`, `ConsumeTimestamp`, `RetryBackoff`, optional authentication. |
| `ErrNoRetry` | Wrap it in a handler error to terminate the message. |
| `ErrConsumerExists`, `ErrAlreadyStarted`, `ErrInvalidAppName`, and the config sentinels (`ErrInvalidTimeout`, `ErrInvalidTopic`, `ErrInvalidGroup`, `ErrInvalidTag`, and others) | Sentinels for `errors.Is`. |

## Naming conventions and dot restrictions

NATS uses dots as subject separators, so dots are not allowed in any messaging identifier.
Use `_` or `-` instead:

- **Topic**: `producer.Config.Topic` and `consumer.Config.Topic` must not contain `.`
  (`ErrInvalidTopic`).
- **Group**: `consumer.Config.Group` must not contain `.` (`ErrInvalidGroup`).
- **Tag**: `producer.Message.Tag` and each entry of `consumer.Config.Tags` must not contain `.`
  (`ErrInvalidTag`).
- **App name**: `identity.Info.Name` must not contain `.` (`ErrInvalidAppName`).
