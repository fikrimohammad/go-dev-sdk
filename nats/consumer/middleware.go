package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-servicekit/errs"
	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
	"github.com/fikrimohammad/go-servicekit/observability/logs"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "nats.consumer"

	// attrSubject, attrKey, attrTag, and attrDeliveryCount are the NATS-specific
	// span attributes.
	attrSubject       = "messaging.nats.subject"
	attrKey           = "messaging.nats.message.key"
	attrTag           = "messaging.nats.message.tag"
	attrDeliveryCount = "messaging.nats.message.delivery_count"
)

var (
	// processDurationInstrument is messaging.process.duration, recorded for every
	// processed message.
	processDurationInstrument = metrics.Instrument{
		Name:        messagingconv.ProcessDuration{}.Name(),
		Unit:        messagingconv.ProcessDuration{}.Unit(),
		Description: messagingconv.ProcessDuration{}.Description(),
	}

	// consumedInstrument is messaging.client.consumed.messages, incremented once per
	// delivery handed to the handler.
	consumedInstrument = metrics.Instrument{
		Name:        messagingconv.ClientConsumedMessages{}.Name(),
		Unit:        messagingconv.ClientConsumedMessages{}.Unit(),
		Description: messagingconv.ClientConsumedMessages{}.Description(),
	}
)

// consumeHandler processes a single consumed message.
type consumeHandler func(ctx context.Context, msg *message) error

// meta carries the attributes held by every instrumented message.
type meta struct {
	topic      string // messaging.destination.name
	group      string // messaging.consumer.group.name
	serverAddr string // server.address, from the first endpoint
	serverPort int    // server.port

	// base holds the baseAttrs of the topic and group. It is built once by
	// newHandlerAdapter and shared read-only by the middleware.
	base map[string]any

	// retryBackoff is the NakWithDelay used for a failed message, or zero for Nak.
	retryBackoff time.Duration

	// propagator extracts the incoming trace context from message headers.
	propagator propagation.TextMapPropagator

	// metrics and tracer override the OTel global providers when non-nil.
	metrics metrics.Client
	tracer  tracer.Client
}

// metricsClient returns the injected metrics client, or metrics.Default() when
// none was injected. It is resolved per call so a later otel.SetMeterProvider
// still applies.
func (m meta) metricsClient() metrics.Client {
	if m.metrics != nil {
		return m.metrics
	}
	return metrics.Default()
}

// tracerClient returns the injected tracer client, or tracer.Default() when none
// was injected. It is resolved per call so a later otel.SetTracerProvider still
// applies.
func (m meta) tracerClient() tracer.Client {
	if m.tracer != nil {
		return m.tracer
	}
	return tracer.Default()
}

// processSpanName is the span name for a processed message, per the messaging
// semantic conventions: "process <destination>".
func (m meta) processSpanName() string {
	return "process " + m.topic
}

// baseAttrs returns the attributes shared by the metrics and the span of one
// processed message: the system, operation, destination, consumer group, and
// server.
func (m meta) baseAttrs() map[string]any {
	a := map[string]any{
		string(otelsemconv.MessagingSystemKey):          "nats",
		string(otelsemconv.MessagingOperationNameKey):   "process",
		string(otelsemconv.MessagingOperationTypeKey):   string(messagingconv.OperationTypeProcess),
		string(otelsemconv.MessagingDestinationNameKey): m.topic,
	}
	if m.group != "" {
		a[string(otelsemconv.MessagingConsumerGroupNameKey)] = m.group
	}
	if m.serverAddr != "" {
		a[string(otelsemconv.ServerAddressKey)] = m.serverAddr
		a[string(otelsemconv.ServerPortKey)] = m.serverPort
	}
	return a
}

// messageAttrs returns the span-only attributes of one message: its subject,
// stream sequence, body size, delivery count, and the key and tag when set.
func messageAttrs(msg *message) map[string]any {
	a := map[string]any{
		attrSubject: msg.subject,
		string(otelsemconv.MessagingMessageIDKey):       msg.id,
		string(otelsemconv.MessagingMessageBodySizeKey): len(msg.body),
		attrDeliveryCount: msg.deliveryCount,
	}
	if msg.key != "" {
		a[attrKey] = msg.key
	}
	if msg.tag != "" {
		a[attrTag] = msg.tag
	}
	return a
}

// withAttrs returns a copy of base with extra added. base is not modified.
func withAttrs(base, extra map[string]any) map[string]any {
	a := maps.Clone(base)
	maps.Copy(a, extra)
	return a
}

// panicRecovery recovers panics raised while consuming a message, logs the
// stack trace, and converts them into errors so the message is retried.
func (m meta) panicRecovery(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				attrs := []any{
					"panic", r,
					"topic", m.topic,
					"stack", string(stack),
				}
				if id := tracer.TraceIDFrom(ctx); id != "" {
					attrs = append(attrs, "trace_id", id)
				}
				logs.Error(ctx, "panic recovered", attrs...)
				err = fmt.Errorf("%w: %v", wire.ErrPanic, r)
			}
		}()
		return next(ctx, msg)
	}
}

// tracerMW extracts the incoming trace context from the message headers,
// starts a CONSUMER span, settles the message from the outcome while the span is
// open, and sets error status and error.type on failure.
func (m meta) tracerMW(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *message) error {
		ctx = m.propagator.Extract(ctx, natsHeaderCarrier{header: msg.raw.Headers()})

		ctx, span := m.tracerClient().Tracer(tracerScope).Start(ctx, m.processSpanName(),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(tracer.Attrs(withAttrs(m.base, messageAttrs(msg)))...),
		)
		defer span.End()

		err := next(ctx, msg)
		m.settleOutcome(ctx, msg, err)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(tracer.Attrs(map[string]any{
				string(otelsemconv.ErrorTypeKey): wire.ErrorType(err),
			})...)
		}
		return err
	}
}

// logger logs every consumed message as structured attributes: success at
// Info level, failure at Error level.
func (m meta) logger(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *message) error {
		start := time.Now()
		err := next(ctx, msg)

		attrs := []slog.Attr{
			slog.String("topic", m.topic),
			slog.String("tag", msg.tag),
			slog.String("message_id", msg.id),
			slog.Duration("duration", time.Since(start)),
			slog.String("subject", msg.subject),
		}
		if m.group != "" {
			attrs = append(attrs, slog.String("group", m.group))
		}
		if id := tracer.TraceIDFrom(ctx); id != "" {
			attrs = append(attrs, slog.String("trace_id", id))
		}

		if err != nil {
			attrs = append(attrs, errs.SlogAttr(err))
			logs.Error(ctx, "consume message error", attrsToArgs(attrs)...)
		} else {
			logs.Info(ctx, "message consumed", attrsToArgs(attrs)...)
		}
		return err
	}
}

// metricsMW records messaging.process.duration for every processed message, with
// error.type on failure, and increments messaging.client.consumed.messages once per
// delivery.
func (m meta) metricsMW(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *message) error {
		start := time.Now()
		err := next(ctx, msg)

		duration := m.base
		if err != nil {
			duration = withAttrs(m.base, map[string]any{string(otelsemconv.ErrorTypeKey): wire.ErrorType(err)})
		}
		c := m.metricsClient()
		_ = c.Record(ctx, processDurationInstrument, time.Since(start).Seconds(), duration)
		_ = c.Add(ctx, consumedInstrument, 1, m.base)
		return err
	}
}

// natsHeaderCarrier adapts NATS message headers to the OTel
// propagation.TextMapCarrier interface.
type natsHeaderCarrier struct {
	header nats.Header
}

func (c natsHeaderCarrier) Get(key string) string {
	if c.header == nil {
		return ""
	}
	return c.header.Get(key)
}

func (c natsHeaderCarrier) Set(key, value string) {
	if c.header != nil {
		c.header.Set(key, value)
	}
}

func (c natsHeaderCarrier) Keys() []string {
	if c.header == nil {
		return nil
	}
	keys := make([]string, 0, len(c.header))
	for k := range c.header {
		keys = append(keys, k)
	}
	return keys
}

// attrsToArgs converts a slice of slog.Attr to []any for logs.* calls.
func attrsToArgs(attrs []slog.Attr) []any {
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	return args
}
