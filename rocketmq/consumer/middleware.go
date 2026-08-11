package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "rocketmq.consumer"

	// metricCount and metricDuration are the OTel metric names emitted per
	// processed message, following the messaging semantic conventions.
	metricCount    = "messaging.process.messages"
	metricDuration = "messaging.process.duration"
)

// consumeHandler processes a single consumed message.
type consumeHandler func(ctx context.Context, msg *primitive.MessageExt) error

// meta carries the attributes held by every instrumented message.
type meta struct {
	topic      string // messaging.destination.name
	group      string // messaging.rocketmq.client_group
	serverAddr string // server.address, from the first endpoint
	serverPort int    // server.port

	// propagator extracts the incoming trace context from message properties.
	propagator propagation.TextMapPropagator

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

// processSpanName is the span name for a processed message, per the messaging
// semantic conventions: "{destination} process".
func (m meta) processSpanName() string {
	return m.topic + " process"
}

// consumeAttrs builds the OTel messaging attributes for a consumed message.
func (m meta) consumeAttrs(msg *primitive.MessageExt) map[string]any {
	a := map[string]any{
		"messaging.system":            "rocketmq",
		"messaging.operation":         "process",
		"messaging.destination.name":  m.topic,
		"messaging.message.id":        msg.MsgId,
		"messaging.message.body.size": len(msg.Body),
	}
	if m.group != "" {
		a["messaging.rocketmq.client_group"] = m.group
	}
	if m.serverAddr != "" {
		a["server.address"] = m.serverAddr
		a["server.port"] = m.serverPort
	}
	if tag := msg.GetTags(); tag != "" {
		a["messaging.rocketmq.message.tag"] = tag
	}
	return a
}

// eventAttrs returns the full attribute set for a completed consume event.
func (m meta) eventAttrs(msg *primitive.MessageExt, etype string) map[string]any {
	a := m.consumeAttrs(msg)
	a["error.type"] = etype
	return a
}

// panicRecovery recovers panics raised while consuming a message, logs the
// stack trace, and converts them into errors so the message is retried.
func (m meta) panicRecovery(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *primitive.MessageExt) (err error) {
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
				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		return next(ctx, msg)
	}
}

// tracerMW extracts the incoming trace context from the message properties,
// starts a CONSUMER span, and sets error status on failure.
func (m meta) tracerMW(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *primitive.MessageExt) error {
		ctx = m.propagator.Extract(ctx, messagePropertyCarrier{msg: msg})

		ctx, span := m.tracerOr().Start(ctx, m.processSpanName(),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(tracer.Attrs(m.consumeAttrs(msg))...),
		)
		defer span.End()

		err := next(ctx, msg)

		etype := ""
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			etype = err.Error()
		}
		span.SetAttributes(tracer.Attrs(m.eventAttrs(msg, etype))...)
		return err
	}
}

// logger logs every consumed message as structured attributes: success at
// Info level, failure at Error level.
func (m meta) logger(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *primitive.MessageExt) error {
		start := time.Now()
		err := next(ctx, msg)

		attrs := []slog.Attr{
			slog.String("topic", m.topic),
			slog.String("tag", msg.GetTags()),
			slog.String("message_id", msg.MsgId),
			slog.Duration("duration", time.Since(start)),
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

// metricsMW records one count + duration pair per processed message.
func (m meta) metricsMW(next consumeHandler) consumeHandler {
	return func(ctx context.Context, msg *primitive.MessageExt) error {
		start := time.Now()
		err := next(ctx, msg)

		attrs := m.eventAttrs(msg, errorText(err))
		if m.metrics != nil {
			_ = m.metrics.Count(ctx, metricCount, 1, attrs)
			_ = m.metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
		} else {
			_ = metrics.Count(ctx, metricCount, 1, attrs)
			_ = metrics.Histogram(ctx, metricDuration, time.Since(start).Seconds(), attrs)
		}
		return err
	}
}

// errorText returns err's text, or the empty string when err is nil.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// messagePropertyCarrier adapts a message's properties to the OTel
// propagation.TextMapCarrier interface.
type messagePropertyCarrier struct {
	msg *primitive.MessageExt
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

// attrsToArgs converts a slice of slog.Attr to []any for logs.* calls.
func attrsToArgs(attrs []slog.Attr) []any {
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	return args
}
