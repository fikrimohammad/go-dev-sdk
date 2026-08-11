// Package redis wraps github.com/redis/go-redis/v9 with a small, standardized
// API and records OpenTelemetry traces and metrics per executed command.
//
// New builds a go-redis client from a Config, attaches an instrumentation
// hook, pings the server to verify the connection, and returns an
// instrumented Client. Metrics and tracing clients are injectable via
// WithMetrics / WithTracer and fall back to the package-level defaults.
//
// The exported surface is minimal and consistent with the db package: the
// ReadCommands and WriteCommands interfaces group the read and write commands,
// the Client interface describes the connection contract (embedding
// ReadCommands and WriteCommands plus Close), and the Pipeline interface
// batches ReadCommands and WriteCommands commands into a single round trip
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
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
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
	metrics metrics.Client
	tracer  tracer.Client
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

// New applies cfg.SetDefaults and cfg.Validate, opens a go-redis client from
// the resulting config, attaches the instrumentation hook, pings it, and
// returns an instrumented Client. server.address and server.port are derived
// from the host/port settings; db.namespace is the database index. Metrics and
// tracing use the package-level defaults unless overridden via
// WithMetrics/WithTracer.
func New(cfg Config, opts ...Option) (Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cli := redis.NewClient(&redis.Options{
		Addr:            net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Username:        cfg.Username,
		Password:        cfg.Password,
		DB:              cfg.DB,
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

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}

	return &client{Client: cli}, nil
}

// client implements Client by embedding the hooked go-redis client, so the
// exported methods are promoted and need no re-wrapping.
type client struct {
	*redis.Client
}

// Pipeline returns a pipeliner that batches commands for a single round trip.
func (c *client) Pipeline() Pipeline {
	return &pipeline{pipe: c.Client.Pipeline()}
}

var _ Client = (*client)(nil)

// pipeline implements Pipeline over a go-redis Pipeliner. Commands queue on
// the underlying pipeliner and are flushed on Exec, which the instrumentation
// hook records as a single "PIPELINE" span.
type pipeline struct {
	pipe redis.Pipeliner
}

// Get queues a GET command.
func (p *pipeline) Get(ctx context.Context, key string) *redis.StringCmd {
	return p.pipe.Get(ctx, key)
}

// Ping queues a PING command.
func (p *pipeline) Ping(ctx context.Context) *redis.StatusCmd {
	return p.pipe.Ping(ctx)
}

// Set queues a SET command.
func (p *pipeline) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return p.pipe.Set(ctx, key, value, expiration)
}

// SetNX queues a SET NX command.
func (p *pipeline) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return p.pipe.SetNX(ctx, key, value, expiration)
}

// Del queues a DEL command.
func (p *pipeline) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return p.pipe.Del(ctx, keys...)
}

// Exec sends all the queued commands to redis in a single round trip.
func (p *pipeline) Exec(ctx context.Context) error {
	_, err := p.pipe.Exec(ctx)
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

	span.SetAttributes(tracer.Attrs(m.attrs(op, cmd, code, etype))...)
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

	code, etype := "", ""
	if err != nil && !errors.Is(err, redis.Nil) {
		span.SetStatus(codes.Error, err.Error())
		code = redisErrorCode(err)
		etype = errorMessage(err)
	}

	span.SetAttributes(tracer.Attrs(m.attrsPipeline(op, cmds, code, etype))...)
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
	a := m.samplingAttrs(op)
	a["db.query.text"] = queryText(cmd)
	a["db.response.status_code"] = code
	a["error.type"] = etype
	return a
}

// attrsPipeline builds the OTel attributes for a pipeline span / metric event.
func (m meta) attrsPipeline(op string, cmds []redis.Cmder, code, etype string) map[string]any {
	a := m.samplingAttrs(op)
	a["db.operation.batch.size"] = len(cmds)
	a["db.query.text"] = pipelineText(cmds)
	a["db.response.status_code"] = code
	a["error.type"] = etype
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

// pipelineText renders the sanitized pipeline text as the joined sanitized
// command texts of the queued commands.
func pipelineText(cmds []redis.Cmder) string {
	parts := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		parts = append(parts, queryText(cmd))
	}
	return strings.Join(parts, "; ")
}

// redisErrorCode returns the Redis simple-error prefix of err (e.g. "ERR",
// "WRONGTYPE"), or the whole error text when it has no prefix.
func redisErrorCode(err error) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, ' '); i > 0 {
		return msg[:i]
	}
	return msg
}

// errorMessage returns err's text verbatim.
func errorMessage(err error) string {
	return err.Error()
}
