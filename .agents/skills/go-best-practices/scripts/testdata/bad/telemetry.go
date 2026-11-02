package util

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// directLiteral passes a map literal with a per-event key straight to Record.
func directLiteral(ctx context.Context, m metricSink, q string) {
	m.Record(ctx, queryDuration, 1, map[string]string{"db.query.text": q})
}

type store struct{ metrics metricSink }

// attrs is a builder method that returns a per-event key.
func (s *store) attrs(q string) map[string]string {
	return map[string]string{"db.query.text": q}
}

// builderFlow reaches Record through the builder method.
func (s *store) builderFlow(ctx context.Context, q string) {
	attrs := s.attrs(q)
	s.metrics.Record(ctx, queryDuration, 1, attrs)
}

// indexWrite adds a per-event key to the metric map with an index write.
func indexWrite(ctx context.Context, m metricSink, tok string) {
	attrs := map[string]string{}
	attrs["fencing_token"] = tok
	m.Add(ctx, leases, 1, attrs)
}

// identFromError sets error.type from an identifier assigned from err.Error().
func identFromError(ctx context.Context, m metricSink, err error) {
	var etype string
	etype = err.Error()
	m.Add(ctx, failures, 1, map[string]string{"error.type": etype})
}

// errorText returns the text of err.
func errorText(err error) string { return err.Error() }

// helperFromError sets error.type from a package function that returns err.Error().
func helperFromError(ctx context.Context, m metricSink, err error) {
	a := map[string]string{}
	a[string(semconv.ErrorTypeKey)] = errorText(err)
	m.Add(ctx, failures, 1, a)
}

// semconvKeys uses semconv constant forms as keys.
func semconvKeys(ctx context.Context, m metricSink, id, q string) {
	m.Record(ctx, msgs, 1, map[string]string{
		string(semconv.MessagingMessageIDKey): id,
		semconv.DBQueryTextKey:                q,
	})
}

// kvForms uses attribute constructors for error.type and a per-event key.
func kvForms(ctx context.Context, m metricSink, err error, q string) {
	set := []attribute.KeyValue{
		attribute.String(string(semconv.ErrorTypeKey), err.Error()),
		semconv.ErrorTypeKey.String(err.Error()),
		semconv.DBQueryTextKey.String(q),
	}
	m.Add(ctx, failures, 1, set)
}
