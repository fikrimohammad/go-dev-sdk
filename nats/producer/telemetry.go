package producer

import (
	"context"
	"maps"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/nats/internal/wire"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "nats.producer"

	// attrSubject, attrKey, and attrTag are the NATS-specific span attributes.
	attrSubject = "messaging.nats.subject"
	attrKey     = "messaging.nats.message.key"
	attrTag     = "messaging.nats.message.tag"
)

var (
	// durationInstrument is messaging.client.operation.duration, recorded for every
	// publish outcome.
	durationInstrument = metrics.Instrument{
		Name:        messagingconv.ClientOperationDuration{}.Name(),
		Unit:        messagingconv.ClientOperationDuration{}.Unit(),
		Description: messagingconv.ClientOperationDuration{}.Description(),
	}

	// sentInstrument is messaging.client.sent.messages, incremented on success only.
	sentInstrument = metrics.Instrument{
		Name:        messagingconv.ClientSentMessages{}.Name(),
		Unit:        messagingconv.ClientSentMessages{}.Unit(),
		Description: messagingconv.ClientSentMessages{}.Description(),
	}
)

// producerMeta carries the telemetry attributes of a registered producer.
type producerMeta struct {
	serverAddr string // server.address, from the first endpoint
	serverPort int    // server.port
}

// meta carries the attributes held by every instrumented send.
type meta struct {
	topic      string // messaging.destination.name
	serverAddr string // server.address
	serverPort int    // server.port

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

// baseAttrs returns the attributes shared by the metrics and the span of one
// send: the system, operation, destination, and server.
func (m meta) baseAttrs() map[string]any {
	a := map[string]any{
		string(otelsemconv.MessagingSystemKey):          "nats",
		string(otelsemconv.MessagingOperationNameKey):   "publish",
		string(otelsemconv.MessagingOperationTypeKey):   string(messagingconv.OperationTypeSend),
		string(otelsemconv.MessagingDestinationNameKey): m.topic,
	}
	if m.serverAddr != "" {
		a[string(otelsemconv.ServerAddressKey)] = m.serverAddr
		a[string(otelsemconv.ServerPortKey)] = m.serverPort
	}
	return a
}

// messageAttrs returns the span-only attributes of one message: its subject, body
// size, and the key and tag when set.
func messageAttrs(msg Message, subject string) map[string]any {
	a := map[string]any{
		attrSubject: subject,
		string(otelsemconv.MessagingMessageBodySizeKey): len(msg.Body),
	}
	if msg.Key != "" {
		a[attrKey] = msg.Key
	}
	if msg.Tag != "" {
		a[attrTag] = msg.Tag
	}
	return a
}

// withAttrs returns a copy of base with extra added. base is not modified.
func withAttrs(base, extra map[string]any) map[string]any {
	a := maps.Clone(base)
	maps.Copy(a, extra)
	return a
}

// startSpan opens the PRODUCER span for a send, named "publish <topic>", with the
// given start attributes.
func (m meta) startSpan(ctx context.Context, attrs map[string]any) (context.Context, trace.Span) {
	return m.tracerClient().Tracer(tracerScope).Start(ctx, "publish "+m.topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(tracer.Attrs(attrs)...),
	)
}

// finish sets the outcome of one send on span, ends it, and records its metrics.
// The message id is set when ack is non-nil, and error.type when err is non-nil.
func (m meta) finish(ctx context.Context, span trace.Span, start time.Time, base map[string]any, ack *jetstream.PubAck, err error) {
	outcome := make(map[string]any, 2)
	etype := ""
	if ack != nil {
		outcome[string(otelsemconv.MessagingMessageIDKey)] = strconv.FormatUint(ack.Sequence, 10)
	}
	if err != nil {
		etype = wire.ErrorType(err)
		span.SetStatus(codes.Error, err.Error())
		outcome[string(otelsemconv.ErrorTypeKey)] = etype
	}
	span.SetAttributes(tracer.Attrs(outcome)...)
	span.End()

	m.record(ctx, start, base, etype)
}

// instrumentPublish records one PRODUCER span and its metrics around a synchronous
// send of msg to subject. The trace context of the span is injected into nm before
// fn sends it, so the consumer's span is a child of this one.
func (m meta) instrumentPublish(
	ctx context.Context,
	msg Message,
	subject string,
	nm *nats.Msg,
	fn func(context.Context) (*jetstream.PubAck, error),
) (*jetstream.PubAck, error) {
	start := time.Now()
	base := m.baseAttrs()
	ctx, span := m.startSpan(ctx, withAttrs(base, messageAttrs(msg, subject)))
	injectTraceContext(ctx, nm)

	result, err := fn(ctx)
	m.finish(ctx, span, start, base, result, err)
	return result, err
}

// instrumentPublishAsync records one PRODUCER span spanning the whole async send:
// it is opened before send and closed when the send callback fires, and its
// metrics are recorded then. Like instrumentPublish, it injects the span's trace
// context into nm.
func (m meta) instrumentPublishAsync(
	ctx context.Context,
	msg Message,
	subject string,
	nm *nats.Msg,
	send func(ctx context.Context, cb func(context.Context, *jetstream.PubAck, error)) error,
	callback func(context.Context, *jetstream.PubAck, error),
) error {
	start := time.Now()
	base := m.baseAttrs()
	ctx, span := m.startSpan(ctx, withAttrs(base, messageAttrs(msg, subject)))
	injectTraceContext(ctx, nm)

	wrapped := func(cctx context.Context, result *jetstream.PubAck, sendErr error) {
		m.finish(ctx, span, start, base, result, sendErr)
		callback(cctx, result, sendErr)
	}

	if err := send(ctx, wrapped); err != nil {
		m.finish(ctx, span, start, base, nil, err)
		return err
	}
	return nil
}

// record emits messaging.client.operation.duration for one send, with error.type
// when etype is set. It also increments messaging.client.sent.messages, but only
// for a successful send.
func (m meta) record(ctx context.Context, start time.Time, base map[string]any, etype string) {
	attrs := base
	if etype != "" {
		attrs = withAttrs(base, map[string]any{string(otelsemconv.ErrorTypeKey): etype})
	}
	c := m.metricsClient()
	_ = c.Record(ctx, durationInstrument, time.Since(start).Seconds(), attrs)
	if etype == "" {
		_ = c.Add(ctx, sentInstrument, 1, attrs)
	}
}

// injectTraceContext injects the current W3C trace context into the message
// headers so consumers can continue the trace. It uses the global
// propagator (set once at startup) and never modifies it.
func injectTraceContext(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier{header: msg.Header})
}

// natsHeaderCarrier adapts a NATS message's headers to the OTel
// propagation.TextMapCarrier interface.
type natsHeaderCarrier struct {
	header nats.Header
}

func (c natsHeaderCarrier) Get(key string) string {
	return c.header.Get(key)
}

func (c natsHeaderCarrier) Set(key, value string) {
	c.header.Set(key, value)
}

func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.header))
	for k := range c.header {
		keys = append(keys, k)
	}
	return keys
}
