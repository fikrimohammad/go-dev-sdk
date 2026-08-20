// Package redis wraps github.com/redis/go-redis/v9 with a small, standardized
// API and records OpenTelemetry traces and metrics per executed command.
//
// New builds a go-redis client from a Config, attaches an instrumentation
// hook, pings the server to verify the connection, and returns an
// instrumented Client. Metrics and tracing clients are injectable via
// WithMetrics / WithTracer and fall back to the package-level defaults.
//
// The exported surface is minimal and consistent with the db package: the
// ReadCommands, WriteCommands, and ScriptCommands interfaces group the commands,
// the Client interface describes the connection contract (embedding
// ReadCommands, WriteCommands, ScriptCommands plus Close), and the Pipeline interface
// batches those commands into a single round trip
// flushed by Exec. All are backed by unexported implementations over
// *redis.Client and *redis.Pipeliner.
//
// Instrumentation rides go-redis' hook pipeline, so every command — current
// and future — is covered without per-method wrapping. Per executed command
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
// Tracing and metrics use the package-level defaults of the observability
// packages (tracer.Tracer / metrics.Count & metrics.Histogram).
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

// meta carries the attributes held by every instrumented command.
type meta struct {
	serverAddr string // server.address
	serverPort int    // server.port
	namespace  string // db.namespace, the database index as a string

	// metrics and tracer override the package-level defaults when non-nil.
	metrics metrics.Client
	tracer  tracer.Client
}

// Option configures a Client returned by New.
type Option func(*options)

type options struct {
	metrics             metrics.Client
	tracer              tracer.Client
	poolMetricsInterval time.Duration
}

// WithMetrics injects a metrics client for the instrumented commands. A nil
// client falls back to the package-level default (metrics.SetDefault).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented commands. A nil
// client falls back to the package-level default (tracer.SetDefault).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// WithPoolMetrics enables background collection of connection pool metrics (hits, misses, timeouts, conns) at the given interval.
func WithPoolMetrics(interval time.Duration) Option {
	return func(o *options) { o.poolMetricsInterval = interval }
}

// New applies cfg.SetDefaults and cfg.Validate, opens a go-redis client from
// the resulting config, attaches the instrumentation hook, pings it, and
// returns an instrumented Client. server.address and server.port are derived
// from the host/port settings; db.namespace is the database index. Metrics and
// tracing use the package-level defaults unless overridden via
// WithMetrics/WithTracer.
func New(cfg Config, opts ...Option) (Client, error) {
	return NewWithContext(context.Background(), cfg, opts...)
}

// NewWithContext is like New, but accepts a context for the initial
// connection verification ping (bounded by cfg.ConnectTimeout).
func NewWithContext(ctx context.Context, cfg Config, opts ...Option) (Client, error) {
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

	cli := redis.NewClient(&redis.Options{
		Addr:            net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
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
	cli.AddHook(&instrumentHook{
		meta: meta{
			serverAddr: cfg.Host,
			serverPort: cfg.Port,
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
		stopCh = make(chan struct{})
		baseAttrs := map[string]any{
			"server.address": cfg.Host,
			"server.port":    cfg.Port,
			"db.system.name": "redis",
			"db.namespace":   strconv.Itoa(cfg.DB),
		}
		go func(c *redis.Client, m metrics.Client, interval time.Duration, stop <-chan struct{}, attrs map[string]any) {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			var prevHits, prevMisses, prevTimeouts, prevStale uint32
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					stats := c.PoolStats()
					mCtx := context.Background()

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
							_ = m.Count(mCtx, "db.client.connections.hits", hitsDelta, attrs)
						}
						if missesDelta > 0 {
							_ = m.Count(mCtx, "db.client.connections.misses", missesDelta, attrs)
						}
						if timeoutsDelta > 0 {
							_ = m.Count(mCtx, "db.client.connections.timeouts", timeoutsDelta, attrs)
						}
						if staleDelta > 0 {
							_ = m.Count(mCtx, "db.client.connections.stale", staleDelta, attrs)
						}
						_ = m.Histogram(mCtx, "db.client.connections.total", float64(stats.TotalConns), attrs)
						_ = m.Histogram(mCtx, "db.client.connections.idle", float64(stats.IdleConns), attrs)
					} else {
						if hitsDelta > 0 {
							_ = metrics.Count(mCtx, "db.client.connections.hits", hitsDelta, attrs)
						}
						if missesDelta > 0 {
							_ = metrics.Count(mCtx, "db.client.connections.misses", missesDelta, attrs)
						}
						if timeoutsDelta > 0 {
							_ = metrics.Count(mCtx, "db.client.connections.timeouts", timeoutsDelta, attrs)
						}
						if staleDelta > 0 {
							_ = metrics.Count(mCtx, "db.client.connections.stale", staleDelta, attrs)
						}
						_ = metrics.Histogram(mCtx, "db.client.connections.total", float64(stats.TotalConns), attrs)
						_ = metrics.Histogram(mCtx, "db.client.connections.idle", float64(stats.IdleConns), attrs)
					}
				}
			}
		}(cli, mClient, o.poolMetricsInterval, stopCh, baseAttrs)
	}

	return &client{
		Client:          cli,
		stopPoolMetrics: stopCh,
	}, nil
}

// buildTLSConfig converts the TLS settings in Config into a *tls.Config.
func buildTLSConfig(cfg Config) (*tls.Config, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
		ServerName:         cfg.TLSServerName,
		MinVersion:         tls.VersionTLS12,
	}
	if tlsCfg.ServerName == "" {
		tlsCfg.ServerName = cfg.Host
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

// client implements Client by embedding the hooked go-redis client, so the
// exported methods are promoted and need no re-wrapping.
type client struct {
	*redis.Client
	stopPoolMetrics chan struct{}
	stopOnce        sync.Once
}

// Pipeline returns a pipeliner that batches commands for a single round trip.
func (c *client) Pipeline() Pipeline {
	return &pipeline{Pipeliner: c.Client.Pipeline()}
}

// TxPipeline returns a pipeliner that executes commands inside a MULTI...EXEC
// transaction block in a single round trip.
func (c *client) TxPipeline() Pipeline {
	return &pipeline{Pipeliner: c.Client.TxPipeline()}
}

// Pipelined executes fn inside a pipeline and flushes it on completion.
func (c *client) Pipelined(ctx context.Context, fn func(Pipeline) error) error {
	p := c.Pipeline()
	if err := fn(p); err != nil {
		return err
	}
	return p.Exec(ctx)
}

// TxPipelined executes fn inside a transaction pipeline and flushes it on completion.
func (c *client) TxPipelined(ctx context.Context, fn func(Pipeline) error) error {
	p := c.TxPipeline()
	if err := fn(p); err != nil {
		return err
	}
	return p.Exec(ctx)
}

// Close releases the underlying connection pool and stops background pool workers.
func (c *client) Close() error {
	if c.stopPoolMetrics != nil {
		c.stopOnce.Do(func() {
			close(c.stopPoolMetrics)
		})
	}
	return c.Client.Close()
}

var _ Client = (*client)(nil)

// pipeline implements Pipeline by embedding redis.Pipeliner.
// All command methods (ReadCommands, WriteCommands, ScriptCommands)
// are automatically promoted from the underlying Pipeliner.
type pipeline struct {
	redis.Pipeliner
}

// Exec sends all the queued commands to redis in a single round trip.
func (p *pipeline) Exec(ctx context.Context) error {
	_, err := p.Pipeliner.Exec(ctx)
	return err
}

var _ Pipeline = (*pipeline)(nil)

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
		_ = m.metrics.Count(ctx, metricCount, 1, attrs)
		_ = m.metrics.Histogram(ctx, metricDuration, d.Seconds(), attrs)
	} else {
		_ = metrics.Count(ctx, metricCount, 1, attrs)
		_ = metrics.Histogram(ctx, metricDuration, d.Seconds(), attrs)
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
