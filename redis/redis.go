// Package redis wraps github.com/redis/go-redis/v9 and records OpenTelemetry
// traces and metrics per executed command.
//
// New builds a go-redis client from a Config, attaches an instrumentation
// hook, pings the server to verify the connection, and returns a
// [redis.UniversalClient] with the full go-redis command set. Its Close also
// stops the pool-metrics poller. Metrics and tracing clients are injectable
// via WithMetrics / WithTracer and fall back to the OTel global providers.
//
// Instrumentation rides go-redis' hook pipeline, so every command, current
// and future, is covered without per-method wrapping. Per executed command
// one span named after the command (client kind) and the
// db.client.operation.{count,duration} metrics are recorded with OTel
// attributes following the Redis semantic conventions:
// server.address, server.port, db.system.name, db.namespace (the database
// index), db.operation.name, db.query.text (values redacted as "?"),
// db.response.status_code (the Redis simple-error prefix, empty on success)
// and error.type (the error message, empty on success). redis.Nil is treated
// as a successful lookup and does not set error telemetry.
//
// Pipelines are recorded as a single span "PIPELINE" with
// db.operation.batch.size set to the number of queued commands.
//
// Tracing and metrics use the OTel global providers (otel.SetTracerProvider and
// otel.SetMeterProvider) unless overridden with [WithMetrics] or [WithTracer].
package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "redis.client"

	// metricCount and metricDuration are the OTel metric names emitted per executed command.
	metricCount    = "db.client.operation.count"
	metricDuration = "db.client.operation.duration"
)

var (
	metricCountInstrument = metrics.Instrument{
		Name:        metricCount,
		Unit:        "{operation}",
		Description: "Redis client operations",
	}
	metricDurationInstrument = metrics.Instrument{
		Name:        metricDuration,
		Unit:        "s",
		Description: "Redis client operation duration",
	}
	poolHitsInstrument = metrics.Instrument{
		Name:        "db.client.connections.hits",
		Unit:        "{connection}",
		Description: "Redis connection pool hits",
	}
	poolMissesInstrument = metrics.Instrument{
		Name:        "db.client.connections.misses",
		Unit:        "{connection}",
		Description: "Redis connection pool misses",
	}
	poolTimeoutsInstrument = metrics.Instrument{
		Name:        "db.client.connections.timeouts",
		Unit:        "{connection}",
		Description: "Redis connection pool timeouts",
	}
	poolStaleInstrument = metrics.Instrument{
		Name:        "db.client.connections.stale",
		Unit:        "{connection}",
		Description: "Redis stale connections",
	}
	poolTotalInstrument = metrics.Instrument{
		Name:        "db.client.connections.total",
		Unit:        "{connection}",
		Description: "Redis current total connections",
	}
	poolIdleInstrument = metrics.Instrument{
		Name:        "db.client.connections.idle",
		Unit:        "{connection}",
		Description: "Redis current idle connections",
	}
)

// meta carries the attributes held by every instrumented command.
type meta struct {
	serverAddr string // server.address
	serverPort int    // server.port
	namespace  string // db.namespace, the database index as a string

	// metrics and tracer override the OTel global providers when non-nil.
	metrics metrics.Client
	tracer  tracer.Client
}

// Option configures a client returned by New.
type Option func(*options)

type options struct {
	metrics             metrics.Client
	tracer              tracer.Client
	poolMetricsInterval time.Duration
}

// WithMetrics injects a metrics client for the instrumented commands. A nil
// client falls back to the OTel global meter provider (otel.SetMeterProvider).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented commands. A nil
// client falls back to the OTel global tracer provider (otel.SetTracerProvider).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// WithPoolMetrics enables background collection of connection pool metrics (hits, misses, timeouts, conns) at the given interval.
func WithPoolMetrics(interval time.Duration) Option {
	return func(o *options) { o.poolMetricsInterval = interval }
}

// New applies cfg.SetDefaults and cfg.Validate, opens a go-redis client from
// the resulting config (standalone, sentinel, or cluster), attaches the
// instrumentation hook, and pings it. The ping is bounded by both ctx and
// cfg.ConnectTimeout. The returned client is a [redis.UniversalClient]; its
// Close also stops the pool-metrics poller.
func New(ctx context.Context, cfg Config, opts ...Option) (redis.UniversalClient, error) { //nolint:ireturn // returns go-redis UniversalClient by design
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	tlsCfg, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("redis: tls: %w", err)
	}

	cli := createUnderlyingClient(cfg, tlsCfg)

	serverAddr := cfg.Host
	serverPort := cfg.Port
	if serverAddr == "" && len(cfg.Addrs) > 0 {
		serverAddr = cfg.Addrs[0]
		if host, portStr, err := net.SplitHostPort(cfg.Addrs[0]); err == nil {
			serverAddr = host
			if p, err := strconv.Atoi(portStr); err == nil {
				serverPort = p
			}
		}
	}

	cli.AddHook(&instrumentHook{
		meta: meta{
			serverAddr: serverAddr,
			serverPort: serverPort,
			namespace:  strconv.Itoa(cfg.DB),
			metrics:    o.metrics,
			tracer:     o.tracer,
		},
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := cli.Ping(pingCtx).Err(); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}

	var stopCh chan struct{}
	if o.poolMetricsInterval > 0 {
		mClient := o.metrics
		// The poller outlives the startup ctx, so it keeps the ctx values but not its cancellation.
		pollCtx := context.WithoutCancel(ctx)
		stopCh = make(chan struct{})
		baseAttrs := map[string]any{
			"server.address": serverAddr,
			"server.port":    serverPort,
			"db.system.name": "redis",
			"db.namespace":   strconv.Itoa(cfg.DB),
		}
		go func(c redis.UniversalClient, m metrics.Client, interval time.Duration, stop <-chan struct{}, attrs map[string]any) {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			var prevHits, prevMisses, prevTimeouts, prevStale uint32
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					stats := c.PoolStats()
					if stats == nil {
						continue
					}
					mCtx := pollCtx

					hitsDelta := int64(stats.Hits - prevHits)
					prevHits = stats.Hits

					missesDelta := int64(stats.Misses - prevMisses)
					prevMisses = stats.Misses

					timeoutsDelta := int64(stats.Timeouts - prevTimeouts)
					prevTimeouts = stats.Timeouts

					staleDelta := int64(stats.StaleConns - prevStale)
					prevStale = stats.StaleConns

					if m != nil {
						if hitsDelta > 0 {
							_ = m.Add(mCtx, poolHitsInstrument, hitsDelta, attrs)
						}
						if missesDelta > 0 {
							_ = m.Add(mCtx, poolMissesInstrument, missesDelta, attrs)
						}
						if timeoutsDelta > 0 {
							_ = m.Add(mCtx, poolTimeoutsInstrument, timeoutsDelta, attrs)
						}
						if staleDelta > 0 {
							_ = m.Add(mCtx, poolStaleInstrument, staleDelta, attrs)
						}
						_ = m.RecordGauge(mCtx, poolTotalInstrument, float64(stats.TotalConns), attrs)
						_ = m.RecordGauge(mCtx, poolIdleInstrument, float64(stats.IdleConns), attrs)
					} else {
						if hitsDelta > 0 {
							_ = metrics.Add(mCtx, poolHitsInstrument, hitsDelta, attrs)
						}
						if missesDelta > 0 {
							_ = metrics.Add(mCtx, poolMissesInstrument, missesDelta, attrs)
						}
						if timeoutsDelta > 0 {
							_ = metrics.Add(mCtx, poolTimeoutsInstrument, timeoutsDelta, attrs)
						}
						if staleDelta > 0 {
							_ = metrics.Add(mCtx, poolStaleInstrument, staleDelta, attrs)
						}
						_ = metrics.RecordGauge(mCtx, poolTotalInstrument, float64(stats.TotalConns), attrs)
						_ = metrics.RecordGauge(mCtx, poolIdleInstrument, float64(stats.IdleConns), attrs)
					}
				}
			}
		}(cli, mClient, o.poolMetricsInterval, stopCh, baseAttrs)
	}

	return &client{
		UniversalClient: cli,
		stopPoolMetrics: stopCh,
	}, nil
}

// createUnderlyingClient instantiates the concrete go-redis client for the configured topology.
func createUnderlyingClient(cfg Config, tlsCfg *tls.Config) redis.UniversalClient { //nolint:ireturn // returns go-redis UniversalClient by design
	switch cfg.Mode {
	case ModeSentinel:
		opts := &redis.FailoverOptions{
			MasterName:       cfg.MasterName,
			SentinelAddrs:    cfg.Addrs,
			SentinelUsername: cfg.SentinelUsername,
			SentinelPassword: cfg.SentinelPassword,
			Username:         cfg.Username,
			Password:         cfg.Password,
			DB:               cfg.DB,
			TLSConfig:        tlsCfg,
			DialTimeout:      cfg.DialTimeout,
			ReadTimeout:      cfg.ReadTimeout,
			WriteTimeout:     cfg.WriteTimeout,
			PoolSize:         cfg.PoolSize,
			MinIdleConns:     cfg.MinIdleConns,
			MaxIdleConns:     cfg.MaxIdleConns,
			ConnMaxIdleTime:  cfg.ConnMaxIdleTime,
			ConnMaxLifetime:  cfg.ConnMaxLifetime,
			ReplicaOnly:      cfg.ReadOnly,
			RouteByLatency:   cfg.RouteByLatency,
			RouteRandomly:    cfg.RouteRandomly,
		}
		if cfg.ReadOnly || cfg.RouteByLatency || cfg.RouteRandomly {
			return redis.NewFailoverClusterClient(opts)
		}
		return redis.NewFailoverClient(opts)

	case ModeCluster:
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:           cfg.Addrs,
			MaxRedirects:    cfg.MaxRedirects,
			ReadOnly:        cfg.ReadOnly,
			RouteByLatency:  cfg.RouteByLatency,
			RouteRandomly:   cfg.RouteRandomly,
			Username:        cfg.Username,
			Password:        cfg.Password,
			TLSConfig:       tlsCfg,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			MaxIdleConns:    cfg.MaxIdleConns,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
		})

	default: // ModeStandalone
		addr := ""
		if len(cfg.Addrs) > 0 {
			addr = cfg.Addrs[0]
		} else if cfg.Host != "" {
			addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
		}
		return redis.NewClient(&redis.Options{
			Addr:            addr,
			Username:        cfg.Username,
			Password:        cfg.Password,
			DB:              cfg.DB,
			TLSConfig:       tlsCfg,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			MaxIdleConns:    cfg.MaxIdleConns,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
		})
	}
}

// buildTLSConfig converts the TLS settings in Config into a *tls.Config.
func buildTLSConfig(cfg Config) (*tls.Config, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}

	serverName := cfg.TLSServerName
	if serverName == "" {
		serverName = cfg.Host
	}
	if serverName == "" && len(cfg.Addrs) > 0 {
		if host, _, err := net.SplitHostPort(cfg.Addrs[0]); err == nil {
			serverName = host
		} else {
			serverName = cfg.Addrs[0]
		}
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS12,
	}

	if cfg.TLSCACert != "" {
		caCertPool := x509.NewCertPool()
		var caData []byte
		if strings.Contains(cfg.TLSCACert, "-----BEGIN") {
			caData = []byte(cfg.TLSCACert)
		} else {
			var err error
			caData, err = os.ReadFile(cfg.TLSCACert)
			if err != nil {
				return nil, fmt.Errorf("read ca cert: %w", err)
			}
		}
		if !caCertPool.AppendCertsFromPEM(caData) {
			return nil, errors.New("failed to parse ca cert pem")
		}
		tlsCfg.RootCAs = caCertPool
	}

	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		var certData, keyData []byte
		if strings.Contains(cfg.TLSCert, "-----BEGIN") {
			certData = []byte(cfg.TLSCert)
		} else {
			var err error
			certData, err = os.ReadFile(cfg.TLSCert)
			if err != nil {
				return nil, fmt.Errorf("read client cert: %w", err)
			}
		}

		if strings.Contains(cfg.TLSKey, "-----BEGIN") {
			keyData = []byte(cfg.TLSKey)
		} else {
			var err error
			keyData, err = os.ReadFile(cfg.TLSKey)
			if err != nil {
				return nil, fmt.Errorf("read client key: %w", err)
			}
		}

		cert, err := tls.X509KeyPair(certData, keyData)
		if err != nil {
			return nil, fmt.Errorf("parse client key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// client wraps a go-redis UniversalClient. Close is the only method it
// overrides: it stops the pool-metrics poller before closing the pool.
type client struct {
	redis.UniversalClient
	stopPoolMetrics chan struct{}
	stopOnce        sync.Once
}

// Close stops the pool-metrics poller, if one runs, and releases the
// underlying connection pool.
func (c *client) Close() error {
	if c.stopPoolMetrics != nil {
		c.stopOnce.Do(func() {
			close(c.stopPoolMetrics)
		})
	}
	return c.UniversalClient.Close()
}

// instrumentHook instruments every command and pipeline flowing through the
// go-redis hook pipeline.
type instrumentHook struct {
	meta meta
}

// DialHook passes dialing through unchanged.
func (h *instrumentHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook records one span and one count + duration histogram around the
// execution of a single command.
func (h *instrumentHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		return h.meta.instrument(ctx, cmd, func(ctx context.Context) error {
			return next(ctx, cmd)
		})
	}
}

// ProcessPipelineHook records a single span and metrics around the execution
// of a pipeline batch.
func (h *instrumentHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return h.meta.instrumentPipeline(ctx, cmds, func(ctx context.Context) error {
			return next(ctx, cmds)
		})
	}
}

// instrument records one span and one count + duration histogram around fn,
// which is the actual command execution.
func (m meta) instrument(ctx context.Context, cmd redis.Cmder, fn func(context.Context) error) error {
	op := commandName(cmd)
	start := time.Now()

	var tr trace.Tracer
	if m.tracer != nil {
		tr = m.tracer.Tracer(tracerScope)
	} else {
		tr = tracer.Tracer(tracerScope)
	}
	ctx, span := tr.Start(ctx, op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(tracer.Attrs(m.samplingAttrs(op))...),
	)
	err := fn(ctx)

	// db.response.status_code and error.type are set on every event: to the
	// redis simple-error prefix / message on failure, and to the empty string
	// on success. redis.Nil is a normal "key not found" result, not an error.
	code, etype := "", ""
	if err != nil && !errors.Is(err, redis.Nil) {
		span.SetStatus(codes.Error, err.Error())
		code = redisErrorCode(err)
		etype = errorMessage(err)
	}

	span.SetAttributes(tracer.Attrs(map[string]any{
		"db.query.text":           queryText(cmd),
		"db.response.status_code": code,
		"error.type":              etype,
	})...)
	span.End()

	attrs := m.attrs(op, cmd, code, etype)
	m.record(ctx, time.Since(start), attrs)
	return err
}

// instrumentPipeline records one span and one count + duration histogram
// around the execution of a pipeline batch.
func (m meta) instrumentPipeline(ctx context.Context, cmds []redis.Cmder, fn func(context.Context) error) error {
	const op = "PIPELINE"
	start := time.Now()

	var tr trace.Tracer
	if m.tracer != nil {
		tr = m.tracer.Tracer(tracerScope)
	} else {
		tr = tracer.Tracer(tracerScope)
	}
	ctx, span := tr.Start(ctx, op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(tracer.Attrs(m.samplingAttrs(op))...),
	)
	err := fn(ctx)

	// In go-redis, fn(ctx) returns the first error among cmds.
	// If the first error is redis.Nil, but subsequent commands had real errors,
	// we inspect cmds to avoid masking real failures.
	var errToReport error
	if err != nil && !errors.Is(err, redis.Nil) {
		errToReport = err
	} else {
		for _, cmd := range cmds {
			if cErr := cmd.Err(); cErr != nil && !errors.Is(cErr, redis.Nil) {
				errToReport = cErr
				break
			}
		}
	}

	code, etype := "", ""
	if errToReport != nil {
		span.SetStatus(codes.Error, errToReport.Error())
		code = redisErrorCode(errToReport)
		etype = errorMessage(errToReport)
	}

	span.SetAttributes(tracer.Attrs(map[string]any{
		"db.operation.batch.size": len(cmds),
		"db.query.text":           pipelineText(cmds),
		"db.response.status_code": code,
		"error.type":              etype,
	})...)
	span.End()

	attrs := m.attrsPipeline(op, cmds, code, etype)
	m.record(ctx, time.Since(start), attrs)
	return err
}

// record emits the count and duration metrics for a single event.
func (m meta) record(ctx context.Context, d time.Duration, attrs map[string]any) {
	if m.metrics != nil {
		_ = m.metrics.Add(ctx, metricCountInstrument, 1, attrs)
		_ = m.metrics.Record(ctx, metricDurationInstrument, d.Seconds(), attrs)
	} else {
		_ = metrics.Add(ctx, metricCountInstrument, 1, attrs)
		_ = metrics.Record(ctx, metricDurationInstrument, d.Seconds(), attrs)
	}
}

// samplingAttrs returns the attributes that matter for sampling decisions and
// are therefore set at span creation time.
func (m meta) samplingAttrs(op string) map[string]any {
	return map[string]any{
		"server.address":    m.serverAddr,
		"server.port":       m.serverPort,
		"db.system.name":    "redis",
		"db.operation.name": op,
		"db.namespace":      m.namespace,
	}
}

// attrs builds the OTel attributes for a single command span / metric event.
func (m meta) attrs(op string, cmd redis.Cmder, code, etype string) map[string]any {
	return map[string]any{
		"server.address":          m.serverAddr,
		"server.port":             m.serverPort,
		"db.system.name":          "redis",
		"db.operation.name":       op,
		"db.namespace":            m.namespace,
		"db.query.text":           queryText(cmd),
		"db.response.status_code": code,
		"error.type":              etype,
	}
}

// attrsPipeline builds the OTel attributes for a pipeline span / metric event.
func (m meta) attrsPipeline(op string, cmds []redis.Cmder, code, etype string) map[string]any {
	return map[string]any{
		"server.address":          m.serverAddr,
		"server.port":             m.serverPort,
		"db.system.name":          "redis",
		"db.operation.name":       op,
		"db.namespace":            m.namespace,
		"db.operation.batch.size": len(cmds),
		"db.query.text":           pipelineText(cmds),
		"db.response.status_code": code,
		"error.type":              etype,
	}
}

// commandName returns the upper-cased redis command name, e.g. "GET".
func commandName(cmd redis.Cmder) string {
	return strings.ToUpper(cmd.Name())
}

// queryText renders the sanitized command text: the command name followed by
// one "?" per argument, redacting literal values.
func queryText(cmd redis.Cmder) string {
	op := commandName(cmd)
	if n := len(cmd.Args()) - 1; n > 0 {
		return op + strings.Repeat(" ?", n)
	}
	return op
}

const maxPipelineCommandsInText = 50

// pipelineText renders the sanitized pipeline text as the joined sanitized
// command texts of the queued commands, capped to avoid excessive attribute sizes.
func pipelineText(cmds []redis.Cmder) string {
	limit := len(cmds)
	truncated := false
	if limit > maxPipelineCommandsInText {
		limit = maxPipelineCommandsInText
		truncated = true
	}
	parts := make([]string, 0, limit+1)
	for i := 0; i < limit; i++ {
		parts = append(parts, queryText(cmds[i]))
	}
	if truncated {
		parts = append(parts, fmt.Sprintf("... (%d more commands)", len(cmds)-limit))
	}
	return strings.Join(parts, "; ")
}

// redisErrorCode returns the Redis simple-error prefix of err (e.g. "ERR",
// "WRONGTYPE", "OOM"), or empty string if err is not a Redis protocol error.
func redisErrorCode(err error) string {
	msg := err.Error()
	token := msg
	if i := strings.IndexByte(msg, ' '); i > 0 {
		token = msg[:i]
	}
	if isUpperASCII(token) {
		return token
	}
	return ""
}

func isUpperASCII(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}
	return true
}

// errorMessage returns err's text verbatim.
func errorMessage(err error) string {
	return err.Error()
}
