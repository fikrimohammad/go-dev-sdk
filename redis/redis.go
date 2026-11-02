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
// one client span named after the command and one
// db.client.operation.duration histogram sample are recorded. The metric
// carries db.system.name ("redis"), db.namespace (the database index),
// db.operation.name, server.address and server.port (when known). On failure
// it also carries error.type (the Redis simple-error prefix such as "WRONGTYPE",
// "timeout", "canceled", or "_OTHER") and db.response.status_code (the
// simple-error prefix, when the server reports one). The span also carries
// db.query.text (values redacted as "?"); the metric does not, to keep its
// cardinality bounded. goredis.Nil is treated as a successful lookup and does
// not set error telemetry.
//
// Pipelines are recorded as a single span "PIPELINE". For two or more queued
// commands the span also carries db.operation.batch.size (MULTI and EXEC of a
// transaction are not counted). The metric carries neither.
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
	"maps"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/dbconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "redis.client"
)

var (
	// metricDuration is the OTel metric name emitted per executed command. It
	// comes from dbconv, as db's does, so both modules share one instrument in a
	// metrics client.
	metricDuration           = dbconv.ClientOperationDuration{}.Name()
	metricDurationInstrument = metrics.Instrument{
		Name:        metricDuration,
		Unit:        dbconv.ClientOperationDuration{}.Unit(),
		Description: dbconv.ClientOperationDuration{}.Description(),
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

// metricsClient returns the injected metrics client, or metrics.Default() when
// none was injected. It is resolved per call so a later otel.SetMeterProvider
// still applies.
func (m meta) metricsClient() metrics.Client {
	if m.metrics != nil {
		return m.metrics
	}
	return metrics.Default()
}

// tracerClient returns the injected tracer client, or tracer.Default() when
// none was injected. It is resolved per call so a later otel.SetTracerProvider
// still applies.
func (m meta) tracerClient() tracer.Client {
	if m.tracer != nil {
		return m.tracer
	}
	return tracer.Default()
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

// ProcessHook records one span and one duration histogram sample around the
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

// instrument records one span and one duration histogram sample around fn,
// which is the actual command execution.
func (m meta) instrument(ctx context.Context, cmd redis.Cmder, fn func(context.Context) error) error {
	op := commandName(cmd)
	// The base attributes are set at start so samplers can read them.
	ctx, span := m.tracerClient().Tracer(tracerScope).Start(ctx, op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(tracer.Attrs(m.attrs(op, "", ""))...),
	)
	start := time.Now()
	err := fn(ctx)

	// redis.Nil is a normal "key not found" result, not an error.
	var code, errType string
	if err != nil && !errors.Is(err, redis.Nil) {
		span.SetStatus(codes.Error, err.Error())
		code = redisErrorCode(err)
		errType = errorType(err)
	}

	attrs := m.attrs(op, code, errType)
	spanAttrs := maps.Clone(attrs)
	spanAttrs[string(otelsemconv.DBQueryTextKey)] = queryText(cmd)
	span.SetAttributes(tracer.Attrs(spanAttrs)...)
	span.End()

	_ = m.metricsClient().Record(ctx, metricDurationInstrument, time.Since(start).Seconds(), attrs)
	return err
}

// instrumentPipeline records one span and one duration histogram sample around
// the execution of a pipeline batch.
func (m meta) instrumentPipeline(ctx context.Context, cmds []redis.Cmder, fn func(context.Context) error) error {
	const op = "PIPELINE"
	// The base attributes are set at start so samplers can read them.
	ctx, span := m.tracerClient().Tracer(tracerScope).Start(ctx, op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(tracer.Attrs(m.attrs(op, "", ""))...),
	)
	start := time.Now()
	err := fn(ctx)

	// go-redis returns the first non-nil command error from fn, and that can be
	// redis.Nil. Each command keeps its own result in Err(), so the loop checks
	// every command and a later real failure is still reported.
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

	var code, errType string
	if errToReport != nil {
		span.SetStatus(codes.Error, errToReport.Error())
		code = redisErrorCode(errToReport)
		errType = errorType(errToReport)
	}

	queued := queuedCommands(cmds)
	attrs := m.attrs(op, code, errType)
	spanAttrs := maps.Clone(attrs)
	// A single command is not a batch, so batch.size is set only for two or more.
	if len(queued) >= 2 {
		spanAttrs[string(otelsemconv.DBOperationBatchSizeKey)] = len(queued)
	}
	spanAttrs[string(otelsemconv.DBQueryTextKey)] = pipelineText(queued)
	span.SetAttributes(tracer.Attrs(spanAttrs)...)
	span.End()

	_ = m.metricsClient().Record(ctx, metricDurationInstrument, time.Since(start).Seconds(), attrs)
	return err
}

// attrs builds the metric attributes for one command or pipeline. Empty values
// are omitted, so error.type and db.response.status_code appear only on failure,
// and db.response.status_code only when the server reports a simple-error prefix.
func (m meta) attrs(op, code, errType string) map[string]any {
	a := map[string]any{
		string(otelsemconv.DBSystemNameKey):    "redis",
		string(otelsemconv.DBOperationNameKey): op,
	}
	if m.namespace != "" {
		a[string(otelsemconv.DBNamespaceKey)] = m.namespace
	}
	if m.serverAddr != "" {
		a[string(otelsemconv.ServerAddressKey)] = m.serverAddr
	}
	if m.serverPort > 0 {
		a[string(otelsemconv.ServerPortKey)] = m.serverPort
	}
	if code != "" {
		a[string(otelsemconv.DBResponseStatusCodeKey)] = code
	}
	if errType != "" {
		a[string(otelsemconv.ErrorTypeKey)] = errType
	}
	return a
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

// queuedCommands returns cmds without the MULTI and EXEC that go-redis wraps
// around a transaction pipeline, so only the caller's commands are counted and
// rendered.
func queuedCommands(cmds []redis.Cmder) []redis.Cmder {
	out := make([]redis.Cmder, 0, len(cmds))
	for _, cmd := range cmds {
		if op := commandName(cmd); op != "MULTI" && op != "EXEC" {
			out = append(out, cmd)
		}
	}
	return out
}

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
// "WRONGTYPE", "OOM"), or empty string if err is not a server reply. Only an
// error that go-redis marks with RedisError is parsed, so a client-side error
// that happens to start with an upper-case word is not mistaken for one.
func redisErrorCode(err error) string {
	server, ok := errors.AsType[redis.Error](err)
	if !ok {
		return ""
	}
	msg := server.Error()
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

// errorType returns the error.type value for a failed command: "timeout" for a
// deadline, a net timeout, or a pool timeout; "canceled"; the Redis simple-error
// prefix (ERR, WRONGTYPE, ...); or "_OTHER".
func errorType(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
		return "timeout"
	}
	if errors.Is(err, redis.ErrPoolTimeout) {
		return "timeout"
	}
	if code := redisErrorCode(err); code != "" {
		return code
	}
	return "_OTHER"
}
