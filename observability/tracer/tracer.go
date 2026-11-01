// Package tracer installs the trace pipeline and exposes named tracers,
// attribute conversion, trace and span ID extraction, and global setup.
package tracer

import (
	"context"
	"fmt"
	"reflect"
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

	"github.com/fikrimohammad/go-dev-sdk/identity"
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
// The service identity held in identity.Info and Config.GlobalKV are attached
// to every exported span as OTel resource attributes. Config.Sampler and the
// batch settings control sampling and export behavior. New returns an error if
// info has no service name, cfg is invalid, or the exporter cannot be created.
//
// New does not touch the OTel global registry. Call [SetGlobal] at the
// composition root to install both the provider and the W3C propagator.
func New(ctx context.Context, info identity.Info, cfg Config) (Client, error) {
	if err := attributes.ValidateAppInfo(info); err != nil {
		return nil, fmt.Errorf("tracer: %w", err)
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithTimeout(cfg.ExportTimeout),
	}
	if cfg.Insecure {
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
func newClientWithExporter(ctx context.Context, info identity.Info, cfg Config, exporter sdktrace.SpanExporter) (Client, error) {
	provider, err := newProvider(ctx, info, cfg, exporter)
	if err != nil {
		_ = exporter.Shutdown(context.WithoutCancel(ctx))
		return nil, err
	}
	return &client{provider: provider, shutdown: provider.Shutdown}, nil
}

// newProvider builds a tracer provider with the configured resource, sampler,
// and batch processor.
func newProvider(ctx context.Context, info identity.Info, cfg Config, exporter sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
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

// defaultEntry caches the client built for one OTel global tracer provider.
type defaultEntry struct {
	provider trace.TracerProvider
	client   Client
}

// defaultCache holds the most recent entry built by Default.
var defaultCache atomic.Pointer[defaultEntry]

// Default returns a Client for the current OTel global tracer provider,
// resolved on every call. The package-level Tracer func records through it, so
// otel.SetTracerProvider or SetGlobal routes SDK spans with no other wiring.
// Before any install, the global is a no-op and spans are not recorded. Default
// is never nil.
func Default() Client {
	p := otel.GetTracerProvider()
	if !reflect.TypeOf(p).Comparable() {
		// ponytail: uncomparable providers are wrapped per call (uncached); fine for the rare value-typed provider.
		return Wrap(p)
	}
	if e := defaultCache.Load(); e != nil && e.provider == p {
		return e.client
	}
	e := &defaultEntry{provider: p, client: Wrap(p)}
	defaultCache.Store(e)
	return e.client
}

// SetGlobal installs c's provider as the OTel global tracer provider and
// installs a W3C TraceContext and Baggage propagator for inbound extraction
// and outbound injection. A nil c installs the noop provider. SetGlobal mutates
// process-global OTel state and should be called once at the composition root.
// Package-level tracer funcs follow the global through [Default].
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

// Attrs converts kv to span attributes, normalizing keys to the OTel semantic
// convention style and preserving value types. The result is sorted by key, so
// it is deterministic.
func Attrs(kv map[string]any) []attribute.KeyValue {
	return attributes.ConvertMapsToKVs(kv)
}

// TraceIDFrom extracts the trace ID from ctx, or "" when there is no valid
// span context.
func TraceIDFrom(ctx context.Context) string {
	return traceIDFrom(ctx)
}

// SpanIDFrom extracts the span ID from ctx, or "" when there is no valid span
// context.
func SpanIDFrom(ctx context.Context) string {
	return spanIDFrom(ctx)
}
