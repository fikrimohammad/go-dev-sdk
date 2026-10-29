// Package metrics installs the metrics pipeline and records counters,
// histograms, up-down counters, and gauges through OTel instruments.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

var (
	// ErrStopped indicates that the client no longer accepts measurements.
	ErrStopped = errors.New("metrics: client is stopped")
	// ErrInvalidMetricName indicates that a metric name is empty or invalid.
	ErrInvalidMetricName = errors.New("metrics: invalid metric name")
	// ErrInstrumentConflict indicates that a metric name was reused with
	// different metadata or a different instrument kind.
	ErrInstrumentConflict = errors.New("metrics: instrument conflict")
	// ErrUnsupportedInstrument indicates that the installed client does not
	// implement the instrument-based API.
	ErrUnsupportedInstrument = errors.New("metrics: instrument not supported by client")

	metricNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,254}$`)
)

// Instrument identifies a metric instrument and its metadata. Name is
// required and must be unique within the client; Unit and Description are
// optional OTel instrument metadata. Once an instrument is created, later
// measurements must use the same kind, unit, and description.
type Instrument struct {
	// Name is the OTel instrument name. It must start with a lowercase
	// letter and contain only lowercase letters, digits, underscores, dots,
	// and hyphens (maximum 255 characters).
	Name string
	// Unit is the OTel unit (UCUM, for example "s" or "{request}").
	Unit string
	// Description documents the instrument.
	Description string
}

// Client is the name-based metrics contract. Count and Histogram are retained
// for compatibility with existing callers and injected fakes; they cannot
// attach instrument metadata. Clients returned by New, Wrap, and Noop also
// implement InstrumentClient, which is the preferred API.
//
// Count and Histogram return errors so callers can react to registration,
// invalid-name, or post-Stop failures.
type Client interface {
	// Count increments a counter named name by value.
	Count(ctx context.Context, name string, value int64, attrs map[string]any) error
	// Histogram records value to a histogram named name.
	Histogram(ctx context.Context, name string, value float64, attrs map[string]any) error
	// Stop flushes pending measurements and shuts down the exporter. It is
	// safe to call multiple times; only the first call performs work.
	Stop(ctx context.Context) error
}

// InstrumentClient is the full metrics contract. Add records counter
// measurements, Record records histogram measurements, AddUpDown records
// up-down counter changes, and RecordGauge records instantaneous gauge values.
// Provider exposes the underlying meter provider for global registration.
//
// New, Wrap, and Noop return an InstrumentClient.
type InstrumentClient interface {
	Client

	// Add records a counter measurement identified by instrument.
	Add(ctx context.Context, instrument Instrument, value int64, attrs map[string]any) error
	// Record records a histogram measurement identified by instrument.
	Record(ctx context.Context, instrument Instrument, value float64, attrs map[string]any) error
	// AddUpDown records an up-down counter change identified by instrument.
	AddUpDown(ctx context.Context, instrument Instrument, value int64, attrs map[string]any) error
	// RecordGauge records an instantaneous gauge value identified by instrument.
	RecordGauge(ctx context.Context, instrument Instrument, value float64, attrs map[string]any) error
	// Provider returns the meter provider backing this client.
	Provider() metric.MeterProvider
}

type instrumentKind string

const (
	kindCounter   instrumentKind = "counter"
	kindHistogram instrumentKind = "histogram"
	kindUpDown    instrumentKind = "updowncounter"
	kindGauge     instrumentKind = "gauge"
)

// instrumentEntry is one registered instrument with its immutable metadata.
type instrumentEntry struct {
	kind        instrumentKind
	unit        string
	description string

	counter   metric.Int64Counter
	histogram metric.Float64Histogram
	upDown    metric.Int64UpDownCounter
	gauge     metric.Float64Gauge
}

// otelMetrics implements InstrumentClient. Instruments are cached by name;
// reusing a name with different metadata is rejected. Stop is synchronized
// against measurements and returns the first shutdown error on every call.
type otelMetrics struct {
	meter       metric.Meter
	provider    metric.MeterProvider
	instruments sync.Map // name → *instrumentEntry
	shutdown    func(context.Context) error
	stopMu      sync.RWMutex
	stopped     atomic.Bool
	stopErr     error
}

var (
	_ Client           = (*otelMetrics)(nil)
	_ InstrumentClient = (*otelMetrics)(nil)
	_ InstrumentClient = noopMetrics{}
)

var noopMeterProvider = metricnoop.NewMeterProvider()

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// prepare acquires the stop read lock and resolves the instrument. The
// returned release function must be called after recording.
func (c *otelMetrics) prepare(kind instrumentKind, instrument Instrument) (*instrumentEntry, func(), error) {
	c.stopMu.RLock()
	if c.stopped.Load() {
		c.stopMu.RUnlock()
		return nil, nil, ErrStopped
	}
	entry, err := c.instrument(kind, instrument)
	if err != nil {
		c.stopMu.RUnlock()
		return nil, nil, err
	}
	return entry, c.stopMu.RUnlock, nil
}

func (c *otelMetrics) instrument(kind instrumentKind, instrument Instrument) (*instrumentEntry, error) {
	name, err := validateInstrument(instrument)
	if err != nil {
		return nil, err
	}
	if v, ok := c.instruments.Load(name); ok {
		entry := v.(*instrumentEntry)
		if err := entry.matches(kind, instrument); err != nil {
			return nil, err
		}
		return entry, nil
	}

	entry := &instrumentEntry{kind: kind, unit: instrument.Unit, description: instrument.Description}
	switch kind {
	case kindCounter:
		entry.counter, err = c.meter.Int64Counter(name,
			metric.WithUnit(instrument.Unit),
			metric.WithDescription(instrument.Description),
		)
	case kindHistogram:
		entry.histogram, err = c.meter.Float64Histogram(name,
			metric.WithUnit(instrument.Unit),
			metric.WithDescription(instrument.Description),
		)
	case kindUpDown:
		entry.upDown, err = c.meter.Int64UpDownCounter(name,
			metric.WithUnit(instrument.Unit),
			metric.WithDescription(instrument.Description),
		)
	case kindGauge:
		entry.gauge, err = c.meter.Float64Gauge(name,
			metric.WithUnit(instrument.Unit),
			metric.WithDescription(instrument.Description),
		)
	default:
		return nil, fmt.Errorf("metrics: unknown instrument kind %q", kind)
	}
	if err != nil {
		return nil, fmt.Errorf("metrics: create %s %q: %w", kind, name, err)
	}

	actual, loaded := c.instruments.LoadOrStore(name, entry)
	if !loaded {
		return entry, nil
	}
	existing := actual.(*instrumentEntry)
	if err := existing.matches(kind, instrument); err != nil {
		return nil, err
	}
	return existing, nil
}

func (e *instrumentEntry) matches(kind instrumentKind, instrument Instrument) error {
	if e.kind == kind && e.unit == instrument.Unit && e.description == instrument.Description {
		return nil
	}
	return fmt.Errorf("%w: %q is already registered as %s (unit=%q, description=%q)",
		ErrInstrumentConflict, instrument.Name, e.kind, e.unit, e.description)
}

func validateInstrument(instrument Instrument) (string, error) {
	if !metricNamePattern.MatchString(instrument.Name) {
		return "", fmt.Errorf("%w: %q", ErrInvalidMetricName, instrument.Name)
	}
	return instrument.Name, nil
}

func (c *otelMetrics) Add(ctx context.Context, instrument Instrument, value int64, attrs map[string]any) error {
	entry, release, err := c.prepare(kindCounter, instrument)
	if err != nil {
		return err
	}
	defer release()
	entry.counter.Add(contextOrBackground(ctx), value, metric.WithAttributes(attributes.ConvertMapsToKVs(attrs)...))
	return nil
}

func (c *otelMetrics) Record(ctx context.Context, instrument Instrument, value float64, attrs map[string]any) error {
	entry, release, err := c.prepare(kindHistogram, instrument)
	if err != nil {
		return err
	}
	defer release()
	entry.histogram.Record(contextOrBackground(ctx), value, metric.WithAttributes(attributes.ConvertMapsToKVs(attrs)...))
	return nil
}

func (c *otelMetrics) AddUpDown(ctx context.Context, instrument Instrument, value int64, attrs map[string]any) error {
	entry, release, err := c.prepare(kindUpDown, instrument)
	if err != nil {
		return err
	}
	defer release()
	entry.upDown.Add(contextOrBackground(ctx), value, metric.WithAttributes(attributes.ConvertMapsToKVs(attrs)...))
	return nil
}

func (c *otelMetrics) RecordGauge(ctx context.Context, instrument Instrument, value float64, attrs map[string]any) error {
	entry, release, err := c.prepare(kindGauge, instrument)
	if err != nil {
		return err
	}
	defer release()
	entry.gauge.Record(contextOrBackground(ctx), value, metric.WithAttributes(attributes.ConvertMapsToKVs(attrs)...))
	return nil
}

// Count increments a counter named name by value. It is the legacy API;
// prefer Add, which also carries the instrument unit and description.
func (c *otelMetrics) Count(ctx context.Context, name string, value int64, attrs map[string]any) error {
	return c.Add(ctx, Instrument{Name: name}, value, attrs)
}

// Histogram records value to a histogram named name. It is the legacy API;
// prefer Record, which also carries the instrument unit and description.
func (c *otelMetrics) Histogram(ctx context.Context, name string, value float64, attrs map[string]any) error {
	return c.Record(ctx, Instrument{Name: name}, value, attrs)
}

func (c *otelMetrics) Provider() metric.MeterProvider {
	return c.provider
}

func (c *otelMetrics) Stop(ctx context.Context) error {
	if c.shutdown == nil {
		return nil
	}
	ctx = contextOrBackground(ctx)
	c.stopMu.Lock()
	defer c.stopMu.Unlock()
	if c.stopped.Load() {
		return c.stopErr
	}
	c.stopErr = c.shutdown(ctx)
	c.stopped.Store(true)
	return c.stopErr
}

func newInstrumentClient(provider metric.MeterProvider, scope string, shutdown func(context.Context) error) *otelMetrics {
	if provider == nil {
		provider = noopMeterProvider
	}
	if scope == "" {
		scope = DefaultInstrumentationScope
	}
	return &otelMetrics{
		meter:    provider.Meter(scope),
		provider: provider,
		shutdown: shutdown,
	}
}

// Wrap returns an InstrumentClient backed by provider. A nil provider is
// treated as a noop provider. Stop is a no-op; the caller retains ownership of
// the provider's lifecycle. Wrap uses DefaultInstrumentationScope.
func Wrap(provider metric.MeterProvider) InstrumentClient {
	return WrapWithScope(provider, "")
}

// WrapWithScope is like Wrap but uses scope as the instrumentation scope name.
// An empty scope uses DefaultInstrumentationScope.
func WrapWithScope(provider metric.MeterProvider, scope string) InstrumentClient {
	return newInstrumentClient(provider, scope, nil)
}

// New builds an InstrumentClient backed by an OTLP gRPC exporter.
// The exporter pushes metrics to the collector periodically. Call Stop on the
// returned client when the application exits to flush pending metrics.
//
// The appinfo.Info identity is attached to every exported metric as the OTel
// semantic convention resource attributes service.name, service.version, and
// deployment.environment. New returns an error if info has no service name,
// cfg is invalid, or the exporter cannot be created.
func New(ctx context.Context, info appinfo.Info, cfg Config) (InstrumentClient, error) {
	ctx = contextOrBackground(ctx)
	if err := attributes.ValidateAppInfo(info); err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	opts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
		otlpmetricgrpc.WithTimeout(cfg.Timeout),
	}
	if cfg.isInsecure() {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.Headers))
	}

	exporter, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("metrics: creating OTLP metric exporter: %w", err)
	}

	return newClientWithExporter(ctx, info, cfg, exporter)
}

// newClientWithExporter builds the provider and client around exporter. It is
// separate from New so tests can supply a fake exporter.
func newClientWithExporter(ctx context.Context, info appinfo.Info, cfg Config, exporter sdkmetric.Exporter) (InstrumentClient, error) {
	provider, err := newMeterProvider(ctx, info, cfg,
		sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(cfg.ExportInterval)))
	if err != nil {
		_ = exporter.Shutdown(context.Background())
		return nil, err
	}
	return newInstrumentClient(provider, cfg.Scope, provider.Shutdown), nil
}

// newMeterProvider builds a meter provider with the configured resource and
// histogram view. It is separated from New so tests can supply a manual reader.
func newMeterProvider(ctx context.Context, info appinfo.Info, cfg Config, reader sdkmetric.Reader) (*sdkmetric.MeterProvider, error) {
	res, err := resource.New(ctx, resource.WithAttributes(
		attributes.ConvertMapsToKVs(cfg.GlobalKV, attributes.ConvertAppInfoToMap(info))...,
	))
	if err != nil {
		return nil, fmt.Errorf("metrics: creating metric resource: %w", err)
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{
				MaxSize:  cfg.HistogramMaxBuckets,
				MaxScale: cfg.HistogramMaxScale,
			}},
		)),
	), nil
}

// noopMetrics discards all measurements.
type noopMetrics struct{}

func (noopMetrics) Count(context.Context, string, int64, map[string]any) error { return nil }

func (noopMetrics) Histogram(context.Context, string, float64, map[string]any) error {
	return nil
}

func (noopMetrics) Add(context.Context, Instrument, int64, map[string]any) error { return nil }

func (noopMetrics) Record(context.Context, Instrument, float64, map[string]any) error {
	return nil
}

func (noopMetrics) AddUpDown(context.Context, Instrument, int64, map[string]any) error {
	return nil
}

func (noopMetrics) RecordGauge(context.Context, Instrument, float64, map[string]any) error {
	return nil
}

func (noopMetrics) Provider() metric.MeterProvider { return noopMeterProvider }

func (noopMetrics) Stop(context.Context) error { return nil }

var defaultMetrics atomic.Pointer[Client]

func init() {
	SetDefault(Noop())
}

// Noop returns an InstrumentClient that discards all measurements.
func Noop() InstrumentClient {
	return noopMetrics{}
}

// Default returns the Client used by the package-level Count and Histogram
// funcs. It is never nil: before SetDefault is called, and after
// SetDefault(nil), it is a noop implementation. The instrument-based funcs
// require the default to implement InstrumentClient and report
// ErrUnsupportedInstrument otherwise.
func Default() Client {
	return *defaultMetrics.Load()
}

// SetDefault makes m the Client used by the package-level funcs, replacing any
// installed earlier. A nil m restores the noop client. It is safe to call
// concurrently with measurement calls.
func SetDefault(m Client) {
	if m == nil {
		m = Noop()
	}
	defaultMetrics.Store(&m)
}

func defaultInstrumentClient() (InstrumentClient, error) {
	client, ok := Default().(InstrumentClient)
	if !ok {
		return nil, ErrUnsupportedInstrument
	}
	return client, nil
}

// Count increments a counter named name by value. It reports the error so
// callers can react to registration, invalid-name, or post-Stop failures.
func Count(ctx context.Context, name string, value int64, attrs map[string]any) error {
	return Default().Count(ctx, name, value, attrs)
}

// Histogram records value to a histogram named name. It reports the error so
// callers can react to registration, invalid-name, or post-Stop failures.
func Histogram(ctx context.Context, name string, value float64, attrs map[string]any) error {
	return Default().Histogram(ctx, name, value, attrs)
}

// Add records a counter measurement through the default InstrumentClient.
func Add(ctx context.Context, instrument Instrument, value int64, attrs map[string]any) error {
	client, err := defaultInstrumentClient()
	if err != nil {
		return fmt.Errorf("%w: Add %q", err, instrument.Name)
	}
	return client.Add(ctx, instrument, value, attrs)
}

// Record records a histogram measurement through the default
// InstrumentClient.
func Record(ctx context.Context, instrument Instrument, value float64, attrs map[string]any) error {
	client, err := defaultInstrumentClient()
	if err != nil {
		return fmt.Errorf("%w: Record %q", err, instrument.Name)
	}
	return client.Record(ctx, instrument, value, attrs)
}

// AddUpDown records an up-down counter change through the default
// InstrumentClient.
func AddUpDown(ctx context.Context, instrument Instrument, value int64, attrs map[string]any) error {
	client, err := defaultInstrumentClient()
	if err != nil {
		return fmt.Errorf("%w: AddUpDown %q", err, instrument.Name)
	}
	return client.AddUpDown(ctx, instrument, value, attrs)
}

// RecordGauge records an instantaneous gauge value through the default
// InstrumentClient.
func RecordGauge(ctx context.Context, instrument Instrument, value float64, attrs map[string]any) error {
	client, err := defaultInstrumentClient()
	if err != nil {
		return fmt.Errorf("%w: RecordGauge %q", err, instrument.Name)
	}
	return client.RecordGauge(ctx, instrument, value, attrs)
}

// Stop flushes pending telemetry and shuts down the default exporter. It is
// safe to call before SetDefault and multiple times; the underlying client is
// responsible for idempotency.
func Stop(ctx context.Context) error {
	return Default().Stop(ctx)
}
