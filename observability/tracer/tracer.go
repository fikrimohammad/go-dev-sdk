// Package tracer installs the trace pipeline and exposes named tracers,
// attribute conversion, and trace-ID extraction.
package tracer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

// Client provides named tracers and owns the trace pipeline's lifecycle. It is
// the injected-client contract used by middleware and the composition root.
type Client interface {
	// Tracer returns an OTel tracer for the given instrumentation scope name.
	Tracer(name string, opts ...trace.TracerOption) trace.Tracer
	// Provider exposes the underlying TracerProvider, e.g. for explicit global
	// registration via otel.SetTracerProvider at the composition root.
	Provider() trace.TracerProvider
	// Stop flushes pending traces and shuts down the underlying provider. It is
	// safe to call multiple times; only the first call performs work.
	Stop(ctx context.Context) error
}

// client implements Client with an OTel TracerProvider.
type client struct {
	provider trace.TracerProvider
	shutdown func(context.Context) error

	stopOnce sync.Once
	stopErr  error
}

func (c *client) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return c.provider.Tracer(name, opts...)
}

func (c *client) Provider() trace.TracerProvider {
	return c.provider
}

func (c *client) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.stopOnce.Do(func() {
		if c.shutdown != nil {
			c.stopErr = c.shutdown(ctx)
		}
	})
	return c.stopErr
}

func traceIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// New builds a Client backed by an OTLP gRPC exporter.
// The exporter pushes traces to the collector in batches. Call Stop on the
// returned Client when the application exits to flush pending traces.
//
// The appinfo.Info identity is attached to every exported span as the OTel
// semantic convention resource attributes service.name, service.version, and
// deployment.environment.
//
// New does not touch the OTel global registry. To route otel.Tracer and
// auto-instrumentation libraries through this client, register the provider
// explicitly at the composition root via otel.SetTracerProvider(c.Provider()).
func New(ctx context.Context, info appinfo.Info, cfg Config) (Client, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithTimeout(cfg.ExportTimeout),
	}
	if cfg.isInsecure() {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("tracer: creating OTLP trace exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(attributes.ConvertMapsToKVs(attributes.ConvertAppInfoToMap(info))...))
	if err != nil {
		_ = exporter.Shutdown(context.Background())
		return nil, fmt.Errorf("tracer: creating trace resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	return &client{provider: provider, shutdown: provider.Shutdown}, nil
}

// Wrap returns a Client backed by the given provider. Stop is a no-op; the
// caller retains ownership of the provider's lifecycle. Intended for tests
// and callers that build their own TracerProvider.
func Wrap(provider trace.TracerProvider) Client {
	return &client{provider: provider}
}

// Noop returns a Client that silently discards all traces.
func Noop() Client {
	return Wrap(noop.NewTracerProvider())
}

var defaultClient atomic.Pointer[Client]

func init() {
	SetDefault(Noop())
}

// Default returns the Client used by the package-level Tracer func. It is
// never nil: before SetDefault is called it silently discards traces.
func Default() Client {
	return *defaultClient.Load()
}

// SetDefault makes c the Client used by the package-level funcs, replacing any
// installed earlier. It is safe to call concurrently with tracing. A nil c
// resets to the noop client.
func SetDefault(c Client) {
	if c == nil {
		c = Noop()
	}
	defaultClient.Store(&c)
}

// Tracer returns an OTel tracer for the given instrumentation scope name.
func Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return Default().Tracer(name, opts...)
}

// Stop flushes pending traces and shuts down the default exporter. It is safe
// to call before SetDefault and multiple times.
func Stop(ctx context.Context) error {
	return Default().Stop(ctx)
}

// Attrs converts kv to span attributes, normalizing keys to the OTel semantic
// convention style and preserving value types. The result is sorted by key, so
// it is deterministic. It is a pure helper and does not require Init.
func Attrs(kv map[string]any) []attribute.KeyValue {
	return attributes.ConvertMapsToKVs(kv)
}

// TraceIDFrom extracts the trace ID from ctx, or "" when there is no valid
// span context. It is a pure helper and does not require Init.
func TraceIDFrom(ctx context.Context) string {
	return traceIDFrom(ctx)
}
