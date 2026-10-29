// Package metrics installs the metrics pipeline and records counters and
// histograms through OTel instruments.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
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

	illegalChars      = regexp.MustCompile(`[^a-zA-Z0-9_.]`)
	multiUnder        = regexp.MustCompile(`_{2,}`)
	multiDot          = regexp.MustCompile(`\.{2,}`)
	metricNamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.-]*$`)
)

// Client records counters and histograms with normalized attribute keys. It
// is the injected-client contract used by middleware and telemetry callers.
// Count and Histogram return errors so callers can react to registration or
// post-shutdown failures.
type Client interface {
	// Count increments a counter named name by value.
	Count(ctx context.Context, name string, value int64, attrs map[string]any) error
	// Histogram records value to a histogram named name.
	Histogram(ctx context.Context, name string, value float64, attrs map[string]any) error
	// Stop flushes pending measurements and shuts down the exporter. It is
	// safe to call multiple times; only the first call performs work.
	Stop(ctx context.Context) error
}

// otelMetrics wraps OTel instruments behind the Metrics API. Count and
// Histogram report registration and shutdown failures as errors; invalid
// names and post-shutdown calls are rejected. Instruments are cached in
// sync.Map keyed by canonical name. The OTel SDK also discards measurements
// after Stop.
type otelMetrics struct {
	meter      metric.Meter
	counters   sync.Map // name → metric.Int64Counter
	histograms sync.Map // name → metric.Float64Histogram
	shutdown   func(context.Context) error
	stopMu     sync.RWMutex
	stopped    atomic.Bool
}

func (c *otelMetrics) Count(ctx context.Context, name string, value int64, attrs map[string]any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.stopMu.RLock()
	defer c.stopMu.RUnlock()
	if c.stopped.Load() {
		return ErrStopped
	}
	counter, err := c.getOrCreateCounter(name)
	if err != nil {
		return err
	}
	counter.Add(ctx, value, metric.WithAttributes(attributes.ConvertMapsToKVs(attrs)...))
	return nil
}

func (c *otelMetrics) Histogram(ctx context.Context, name string, value float64, attrs map[string]any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.stopMu.RLock()
	defer c.stopMu.RUnlock()
	if c.stopped.Load() {
		return ErrStopped
	}
	hist, err := c.getOrCreateHistogram(name)
	if err != nil {
		return err
	}
	hist.Record(ctx, value, metric.WithAttributes(attributes.ConvertMapsToKVs(attrs)...))
	return nil
}

func (c *otelMetrics) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.shutdown == nil {
		return nil
	}
	c.stopMu.Lock()
	defer c.stopMu.Unlock()
	if c.stopped.Load() {
		return nil
	}
	err := c.shutdown(ctx)
	c.stopped.Store(true)
	return err
}

func (c *otelMetrics) getOrCreateCounter(name string) (metric.Int64Counter, error) {
	canonical, err := c.canonicalMetricName(name)
	if err != nil {
		return nil, err
	}
	if v, ok := c.counters.Load(canonical); ok {
		return v.(metric.Int64Counter), nil
	}

	counter, err := c.meter.Int64Counter(canonical)
	if err != nil {
		return nil, fmt.Errorf("metrics: create counter %q: %w", canonical, err)
	}
	actual, _ := c.counters.LoadOrStore(canonical, counter)
	return actual.(metric.Int64Counter), nil
}

func (c *otelMetrics) getOrCreateHistogram(name string) (metric.Float64Histogram, error) {
	canonical, err := c.canonicalMetricName(name)
	if err != nil {
		return nil, err
	}
	if v, ok := c.histograms.Load(canonical); ok {
		return v.(metric.Float64Histogram), nil
	}

	hist, err := c.meter.Float64Histogram(canonical)
	if err != nil {
		return nil, fmt.Errorf("metrics: create histogram %q: %w", canonical, err)
	}
	actual, _ := c.histograms.LoadOrStore(canonical, hist)
	return actual.(metric.Float64Histogram), nil
}

func (c *otelMetrics) canonicalMetricName(name string) (string, error) {
	canonical := sanitizeName(name)
	if canonical == "" || !metricNamePattern.MatchString(canonical) {
		return "", fmt.Errorf("%w: %q", ErrInvalidMetricName, name)
	}
	return canonical, nil
}

func sanitizeName(name string) string {
	name = illegalChars.ReplaceAllString(name, "_")
	name = multiUnder.ReplaceAllString(name, "_")
	name = multiDot.ReplaceAllString(name, ".")
	name = strings.Trim(name, "_.")
	return strings.ToLower(name)
}

// New builds a Client implementation backed by an OTLP gRPC exporter.
// The exporter pushes metrics to the collector periodically. Call Stop on the
// returned client when the application exits to flush pending metrics.
//
// The appinfo.Info identity is attached to every exported metric as the OTel
// semantic convention resource attributes service.name, service.version, and
// deployment.environment.
func New(ctx context.Context, info appinfo.Info, cfg Config) (Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(info.Name) == "" {
		return nil, fmt.Errorf("%w: app name is required", ErrInvalidMetricName)
	}
	if !metricNamePattern.MatchString(info.Name) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidMetricName, info.Name)
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

	exporter, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(attributes.ConvertMapsToKVs(cfg.GlobalKV, attributes.ConvertAppInfoToMap(info))...))
	if err != nil {
		_ = exporter.Shutdown(context.Background())
		return nil, fmt.Errorf("metrics: creating metric resource: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(cfg.ExportInterval))),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{
				MaxSize:  cfg.HistogramMaxBuckets,
				MaxScale: cfg.HistogramMaxScale,
			}},
		)),
	)
	return &otelMetrics{meter: provider.Meter(info.Name), shutdown: provider.Shutdown}, nil
}

// noopMetrics discards all measurements.
type noopMetrics struct{}

func (noopMetrics) Count(ctx context.Context, name string, value int64, attrs map[string]any) error {
	return nil
}

func (noopMetrics) Histogram(ctx context.Context, name string, value float64, attrs map[string]any) error {
	return nil
}

func (noopMetrics) Stop(ctx context.Context) error { return nil }

var defaultMetrics atomic.Pointer[Client]

func init() {
	SetDefault(Noop())
}

// Noop returns a Client implementation that discards all measurements.
func Noop() Client {
	return noopMetrics{}
}

// Default returns the Client used by the package-level Count and Histogram
// funcs. It is never nil: before SetDefault is called it is a noop
// implementation.
func Default() Client {
	return *defaultMetrics.Load()
}

// SetDefault makes m the Client used by the package-level funcs, replacing any
// installed earlier. It is safe to call concurrently with measurement calls.
func SetDefault(m Client) {
	defaultMetrics.Store(&m)
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

// Stop flushes pending telemetry and shuts down the default exporter. It is
// safe to call before SetDefault and multiple times; the underlying client is
// responsible for idempotency.
func Stop(ctx context.Context) error {
	return Default().Stop(ctx)
}
