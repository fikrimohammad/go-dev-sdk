// Package tracer installs the trace pipeline and exposes named tracers,
// attribute conversion, trace and span ID extraction, and global setup.
package tracer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
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
	// Provider exposes the underlying TracerProvider, e.g. for [SetGlobal] or
	// explicit otel.SetTracerProvider registration at the composition root.
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

func spanContextFrom(ctx context.Context) trace.SpanContext {
	if ctx == nil {
		return trace.SpanContext{}
	}
	return trace.SpanContextFromContext(ctx)
}

func traceIDFrom(ctx context.Context) string {
	sc := spanContextFrom(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

func spanIDFrom(ctx context.Context) string {
	sc := spanContextFrom(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.SpanID().String()
}

// New builds a Client backed by an OTLP gRPC exporter.
// The exporter pushes traces to the collector in batches. Call Stop on the
// returned Client when the application exits to flush pending traces.
//
// The appinfo.Info identity and Config.GlobalKV are attached to every exported
// span as OTel resource attributes. Config.Sampler and the batch settings
// control sampling and export behavior. New returns an error if info has no
// service name, cfg is invalid, or the exporter cannot be created.
//
// New does not touch the OTel global registry. Call [SetGlobal] at the
// composition root to install both the provider and the W3C propagator.
func New(ctx context.Context, info appinfo.Info, cfg Config) (Client, error) {
	if err := attributes.ValidateAppInfo(info); err != nil {
		return nil, fmt.Errorf("tracer: %w", err)
	}
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
	return newClientWithExporter(ctx, info, cfg, exporter)
}

// newClientWithExporter builds the provider and client around exporter. It is
// separate from New so tests can supply a fake exporter.
func newClientWithExporter(ctx context.Context, info appinfo.Info, cfg Config, exporter sdktrace.SpanExporter) (Client, error) {
	provider, err := newProvider(ctx, info, cfg, exporter)
	if err != nil {
		_ = exporter.Shutdown(context.Background())
		return nil, err
	}
	return &client{provider: provider, shutdown: provider.Shutdown}, nil
}

// newProvider builds a tracer provider with the configured resource, sampler,
// and batch processor.
func newProvider(ctx context.Context, info appinfo.Info, cfg Config, exporter sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx, resource.WithAttributes(
		attributes.ConvertMapsToKVs(cfg.GlobalKV, attributes.ConvertAppInfoToMap(info))...,
	))
	if err != nil {
		return nil, fmt.Errorf("tracer: creating trace resource: %w", err)
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(cfg.BatchTimeout),
			sdktrace.WithMaxQueueSize(cfg.MaxQueueSize),
			sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatchSize),
		),
		sdktrace.WithResource(res),
	}
	if cfg.Sampler != nil {
		opts = append(opts, sdktrace.WithSampler(cfg.Sampler))
	}
	return sdktrace.NewTracerProvider(opts...), nil
}

// Wrap returns a Client backed by the given provider. A nil provider is
// treated as a noop provider. Stop is a no-op; the caller retains ownership of
// the provider's lifecycle. Intended for tests and callers that build their
// own TracerProvider.
func Wrap(provider trace.TracerProvider) Client {
	if provider == nil {
		provider = noop.NewTracerProvider()
	}
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
// never nil: before SetDefault is called, and after SetDefault(nil), it
// silently discards traces.
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

// SetGlobal installs c's provider as the OTel global tracer provider and
// installs a W3C TraceContext and Baggage propagator for inbound extraction
// and outbound injection. A nil c installs the noop provider. SetGlobal mutates
// process-global OTel state and should be called once at the composition root.
// It does not change the package default; call [SetDefault] separately when
// package-level tracer funcs should use c.
func SetGlobal(c Client) {
	if c == nil {
		c = Noop()
	}
	otel.SetTracerProvider(c.Provider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
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

// SpanIDFrom extracts the span ID from ctx, or "" when there is no valid span
// context. It is a pure helper and does not require Init.
func SpanIDFrom(ctx context.Context) string {
	return spanIDFrom(ctx)
}
