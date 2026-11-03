package widget

import (
	"context"
	"errors"
	"maps"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// record keeps the query text on the span only; the metric gets bounded
// attributes.
func record(ctx context.Context, m metricSink, span spanSink, q string) {
	attrs := map[string]string{"messaging.nats.subject": subject}
	spanAttrs := maps.Clone(attrs)
	spanAttrs[string(semconv.DBQueryTextKey)] = q
	span.SetAttributes(spanAttrs)
	m.Record(ctx, queryDuration, 1, attrs)
}

// errorType classifies err into a bounded code.
func errorType(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "_OTHER"
}

// failed sets error.type from the coded class.
func failed(ctx context.Context, m metricSink, err error) {
	code := errorType(err)
	m.Add(ctx, failures, 1, map[string]string{"error.type": code})
}

// published uses a bounded key on a metric and a per-event key on a span.
func published(ctx context.Context, counter metricCounter, span spanSink, subj, id string) {
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.nats.subject", subj)))
	span.SetAttributes(attribute.String("messaging.message.id", id))
}
