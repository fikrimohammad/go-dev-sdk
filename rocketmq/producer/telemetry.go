package producer

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "rocketmq.producer"

	// metricCount and metricDuration are the OTel metric names emitted per
	// published message, following the messaging semantic conventions.
	metricCount    = "messaging.publish.messages"
	metricDuration = "messaging.publish.duration"
)

// producerMeta carries the telemetry attributes of a registered producer.
type producerMeta struct {
	serverAddr string // server.address, from the first endpoint
	serverPort int    // server.port
}

// meta carries the attributes held by every instrumented send.
type meta struct {
	serverAddr string
	serverPort int

	// metrics and tracer override the package-level defaults when non-nil.
	metrics metrics.Client
	tracer  tracer.Client
}

// tracerOr returns the configured tracer or the package-level default.
func (m meta) tracerOr() trace.Tracer {
	if m.tracer != nil {
		return m.tracer.Tracer(tracerScope)
	}
	return tracer.Tracer(tracerScope)
}

// publishSpanName is the span name for a published message, per the messaging
// semantic conventions: "{destination} publish".
func publishSpanName(topic string) string {
	return topic + " publish"
}

// publishAttrs builds the OTel messaging attributes for a published message.
func (m meta) publishAttrs(topic, tag, key string, bodyLen int) map[string]any {
	a := map[string]any{
		"messaging.system":            "rocketmq",
		"messaging.operation":         "publish",
		"messaging.destination.name":  topic,
		"messaging.message.body.size": bodyLen,
	}
	if m.serverAddr != "" {
		a["server.address"] = m.serverAddr
		a["server.port"] = m.serverPort
	}
	if tag != "" {
		a["messaging.rocketmq.message.tag"] = tag
	}
	if key != "" {
		a["messaging.rocketmq.message.keys"] = key
	}
	return a
}

// eventAttrs returns the full attribute set for a completed send event.
func (m meta) eventAttrs(topic, tag, key string, bodyLen int, msgID, etype string) map[string]any {
	a := m.publishAttrs(topic, tag, key, bodyLen)
	if msgID != "" {
		a["messaging.message.id"] = msgID
	}
	a["error.type"] = etype
	return a
}

// instrumentPublish records one PRODUCER span and one count + duration pair
// around a synchronous send. The message id, when available, is attached.
func (m meta) instrumentPublish(
	ctx context.Context,
	topic, tag, key string,
	bodyLen int,
	fn func(context.Context) (*primitive.SendResult, error),
) (*primitive.SendResult, error) {
	start := time.Now()
	ctx, span := m.tracerOr().Start(ctx, publishSpanName(topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(tracer.Attrs(m.publishAttrs(topic, tag, key, bodyLen))...),
	)
	result, err := fn(ctx)

	msgID := ""
	if result != nil {
		msgID = result.MsgID
	}
	etype := ""
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		etype = err.Error()
	}

	span.SetAttributes(tracer.Attrs(m.eventAttrs(topic, tag, key, bodyLen, msgID, etype))...)
	span.End()

	m.record(ctx, start, topic, tag, key, bodyLen, msgID, etype)
	return result, err
}

// instrumentPublishAsync records one PRODUCER span spanning the whole async
// send: it is opened before SendAsync and closed when the send callback fires.
func (m meta) instrumentPublishAsync(
	ctx context.Context,
	topic, tag, key string,
	bodyLen int,
	send func(ctx context.Context, cb func(context.Context, *primitive.SendResult, error)) error,
	callback func(context.Context, *primitive.SendResult, error),
) error {
	start := time.Now()
	ctx, span := m.tracerOr().Start(ctx, publishSpanName(topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(tracer.Attrs(m.publishAttrs(topic, tag, key, bodyLen))...),
	)

	wrapped := func(cctx context.Context, result *primitive.SendResult, sendErr error) {
		defer span.End()

		msgID := ""
		if result != nil {
			msgID = result.MsgID
		}
		etype := ""
		if sendErr != nil {
			span.SetStatus(codes.Error, sendErr.Error())
			etype = sendErr.Error()
		}
		span.SetAttributes(tracer.Attrs(m.eventAttrs(topic, tag, key, bodyLen, msgID, etype))...)
		m.record(ctx, start, topic, tag, key, bodyLen, msgID, etype)

		callback(cctx, result, sendErr)
	}

	if err := send(ctx, wrapped); err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(tracer.Attrs(m.eventAttrs(topic, tag, key, bodyLen, "", err.Error()))...)
		span.End()
		m.record(ctx, start, topic, tag, key, bodyLen, "", err.Error())
		return err
	}
	return nil
}

// record emits the count and duration metrics for a single send event.
func (m meta) record(ctx context.Context, start time.Time, topic, tag, key string, bodyLen int, msgID, etype string) {
	attrs := m.eventAttrs(topic, tag, key, bodyLen, msgID, etype)
	if m.metrics != nil {
		_ = m.metrics.Count(ctx, metricCount, 1, attrs)
		_ = m.metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	} else {
		_ = metrics.Count(ctx, metricCount, 1, attrs)
		_ = metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
	}
}

// injectTraceContext injects the current W3C trace context into the message
// properties so consumers can continue the trace. It uses the global
// propagator (set once at startup) and never modifies it.
func injectTraceContext(ctx context.Context, msg *primitive.Message) {
	otel.GetTextMapPropagator().Inject(ctx, messagePropertyCarrier{msg: msg})
}

// messagePropertyCarrier adapts a message's properties to the OTel
// propagation.TextMapCarrier interface.
type messagePropertyCarrier struct {
	msg *primitive.Message
}

func (c messagePropertyCarrier) Get(key string) string {
	return c.msg.GetProperty(key)
}

func (c messagePropertyCarrier) Set(key, value string) {
	c.msg.WithProperty(key, value)
}

func (c messagePropertyCarrier) Keys() []string {
	props := c.msg.GetProperties()
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	return keys
}
