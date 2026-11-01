// Package redis wraps github.com/redis/go-redis/v9 with a small, standardized
// API and records OpenTelemetry traces and metrics per executed command.
//
// New builds a go-redis client from a Config, attaches an instrumentation
// hook, pings the server to verify the connection, and returns an
// instrumented Client. Metrics and tracing clients are injectable via
// WithMetrics / WithTracer and fall back to the OTel global providers.
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

// Nil is an alias for redis.Nil, returned when a key does not exist.
const Nil = redis.Nil //nolint:errname // mirrors go-redis's redis.Nil so callers keep its familiar name

// TxFailedErr is an alias for redis.TxFailedErr, returned when a WATCH transaction fails.
var TxFailedErr = redis.TxFailedErr //nolint:errname // mirrors go-redis's redis.TxFailedErr

// Script is an alias for redis.Script for managing reusable Lua scripts with automatic EVALSHA fallback.
type Script = redis.Script

// NewScript creates a new Redis Lua script instance with automatic EVALSHA fallback.
var NewScript = redis.NewScript

type (
	// Z represents a sorted set member and its score.
	Z = redis.Z

	// ZRangeBy represents options for score-based range queries on sorted sets.
	ZRangeBy = redis.ZRangeBy

	// PoolStats contains connection pool statistics.
	PoolStats = redis.PoolStats

	// Tx is an alias for redis.Tx used in Watch transactions.
	Tx = redis.Tx

	// PubSub is an alias for redis.PubSub for managing pub/sub channels.
	PubSub = redis.PubSub

	// Message is an alias for redis.Message received from a pub/sub subscription.
	Message = redis.Message

	// Subscription is an alias for redis.Subscription representing a pub/sub subscription state change.
	Subscription = redis.Subscription
)

// ReadCommands groups the commands that read from redis.
type ReadCommands interface { //nolint:interfacebloat // curated go-redis subset; replaced by redis.UniversalClient in PR 2 (backlog item R6)
	// Ping verifies that the connection is alive.
	Ping(ctx context.Context) *redis.StatusCmd
	// Exists returns the number of existing keys from the specified list.
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	// TTL returns the remaining time to live of a key.
	TTL(ctx context.Context, key string) *redis.DurationCmd
	// Type returns the string representation of the type of value stored at key.
	Type(ctx context.Context, key string) *redis.StatusCmd
	// Scan incrementally iterates over keys in the database matching pattern.
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd

	// Get returns the value of key. The command result reports redis.Nil when
	// the key does not exist.
	Get(ctx context.Context, key string) *redis.StringCmd
	// MGet returns the values of all specified keys.
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd

	// HGet returns the value associated with field in the hash stored at key.
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	// HGetAll returns all fields and values of the hash stored at key.
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	// HMGet returns the values associated with the specified fields in the hash stored at key.
	HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd
	// HKeys returns all field names in the hash stored at key.
	HKeys(ctx context.Context, key string) *redis.StringSliceCmd
	// HVals returns all values in the hash stored at key.
	HVals(ctx context.Context, key string) *redis.StringSliceCmd
	// HLen returns the number of fields contained in the hash stored at key.
	HLen(ctx context.Context, key string) *redis.IntCmd
	// HExists returns whether field exists in the hash stored at key.
	HExists(ctx context.Context, key, field string) *redis.BoolCmd

	// LLen returns the length of the list stored at key.
	LLen(ctx context.Context, key string) *redis.IntCmd
	// LRange returns the specified elements of the list stored at key.
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	// LIndex returns the element at index in the list stored at key.
	LIndex(ctx context.Context, key string, index int64) *redis.StringCmd

	// SMembers returns all the members of the set value stored at key.
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	// SInter returns the members of the set resulting from the intersection of all the given sets.
	SInter(ctx context.Context, keys ...string) *redis.StringSliceCmd
	// SUnion returns the members of the set resulting from the union of all the given sets.
	SUnion(ctx context.Context, keys ...string) *redis.StringSliceCmd
	// SDiff returns the members of the set resulting from the difference between the first set and all successive sets.
	SDiff(ctx context.Context, keys ...string) *redis.StringSliceCmd
	// SIsMember returns if member is a member of the set stored at key.
	SIsMember(ctx context.Context, key string, member any) *redis.BoolCmd
	// SMIsMember returns whether each member is a member of the set stored at key.
	SMIsMember(ctx context.Context, key string, members ...any) *redis.BoolSliceCmd
	// SCard returns the set cardinality (number of elements) of the set stored at key.
	SCard(ctx context.Context, key string) *redis.IntCmd
	// SRandMember returns one random member from the set stored at key.
	SRandMember(ctx context.Context, key string) *redis.StringCmd
	// SRandMemberN returns count random members from the set stored at key.
	SRandMemberN(ctx context.Context, key string, count int64) *redis.StringSliceCmd

	// ZScore returns the score of member in the sorted set at key.
	ZScore(ctx context.Context, key, member string) *redis.FloatCmd
	// ZCard returns the sorted set cardinality (number of elements) of the sorted set at key.
	ZCard(ctx context.Context, key string) *redis.IntCmd
	// ZCount returns the number of elements in the sorted set at key with a score between minScore and maxScore.
	ZCount(ctx context.Context, key, minScore, maxScore string) *redis.IntCmd
	// ZRange returns the specified range of elements in the sorted set stored at key.
	ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	// ZRangeWithScores returns the specified range of elements with their scores from the sorted set at key.
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd
	// ZRangeByScore returns all elements in the sorted set at key with a score between min and max.
	ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd
	// ZRangeByScoreWithScores returns all elements with their scores in the sorted set at key with a score between min and max.
	ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd
	// ZRevRange returns the specified range of elements in the sorted set stored at key, ordered from highest to lowest score.
	ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	// ZRevRangeWithScores returns the specified range of elements with their scores from the sorted set at key, ordered from highest to lowest score.
	ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd
	// ZRevRangeByScore returns all elements in the sorted set at key with a score between max and min, ordered from highest to lowest score.
	ZRevRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd
	// ZRevRangeByScoreWithScores returns all elements with their scores in the sorted set at key with a score between max and min, ordered from highest to lowest score.
	ZRevRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd
	// ZRank returns the rank of member in the sorted set stored at key, with scores ordered from low to high.
	ZRank(ctx context.Context, key, member string) *redis.IntCmd
	// ZRevRank returns the rank of member in the sorted set stored at key, with scores ordered from high to low.
	ZRevRank(ctx context.Context, key, member string) *redis.IntCmd
}

// WriteCommands groups the commands that write to redis.
type WriteCommands interface { //nolint:interfacebloat // curated go-redis subset; replaced by redis.UniversalClient in PR 2 (backlog item R6)
	// Del removes one or more keys.
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	// Unlink removes one or more keys asynchronously in a non-blocking manner.
	Unlink(ctx context.Context, keys ...string) *redis.IntCmd
	// Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted.
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	// ExpireAt sets an expiration timestamp on key.
	ExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd
	// Persist removes the existing timeout on key, turning the key from volatile to persistent.
	Persist(ctx context.Context, key string) *redis.BoolCmd

	// Set binds value to key with the given expiration. An expiration <= 0
	// keeps the key without a time-to-live.
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	// SetNX atomically binds value to key only when key does not already exist,
	// with the given expiration. The command result reports false when the key
	// already exists.
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	// SetXX atomically binds value to key only when key already exists,
	// with the given expiration. The command result reports false when the key
	// does not exist.
	SetXX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	// GetDel atomically returns the value of key and removes key.
	GetDel(ctx context.Context, key string) *redis.StringCmd
	// GetEx atomically returns the value of key and sets its expiration. An
	// expiration of zero persists the key.
	GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd
	// MSet sets the given keys to their respective values.
	MSet(ctx context.Context, values ...any) *redis.StatusCmd
	// Incr increments the number stored at key by one.
	Incr(ctx context.Context, key string) *redis.IntCmd
	// IncrBy increments the number stored at key by value.
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd
	// IncrByFloat increments the float value stored at key by increment.
	IncrByFloat(ctx context.Context, key string, increment float64) *redis.FloatCmd
	// Decr decrements the number stored at key by one.
	Decr(ctx context.Context, key string) *redis.IntCmd
	// DecrBy decrements the number stored at key by decrement.
	DecrBy(ctx context.Context, key string, decrement int64) *redis.IntCmd

	// HSet sets the specified fields to their respective values in the hash stored at key.
	HSet(ctx context.Context, key string, values ...any) *redis.IntCmd
	// HSetNX sets field in the hash stored at key to value, only if field does not yet exist.
	HSetNX(ctx context.Context, key, field string, value any) *redis.BoolCmd
	// HDel removes the specified fields from the hash stored at key.
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	// HIncrBy increments the number stored at field in the hash stored at key by incr.
	HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd
	// HIncrByFloat increments the float value stored at field in the hash stored at key by incr.
	HIncrByFloat(ctx context.Context, key, field string, incr float64) *redis.FloatCmd

	// LPush inserts all the specified values at the head of the list stored at key.
	LPush(ctx context.Context, key string, values ...any) *redis.IntCmd
	// RPush inserts all the specified values at the tail of the list stored at key.
	RPush(ctx context.Context, key string, values ...any) *redis.IntCmd
	// LPop removes and returns the first element of the list stored at key.
	LPop(ctx context.Context, key string) *redis.StringCmd
	// RPop removes and returns the last element of the list stored at key.
	RPop(ctx context.Context, key string) *redis.StringCmd
	// LPopCount removes and returns the first count elements of the list stored at key.
	LPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd
	// RPopCount removes and returns the last count elements of the list stored at key.
	RPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd
	// LTrim trims an existing list so that it will contain only the specified range of elements.
	LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd
	// LRem removes the first count occurrences of elements equal to value from the list stored at key.
	LRem(ctx context.Context, key string, count int64, value any) *redis.IntCmd
	// LSet sets the list element at index to value.
	LSet(ctx context.Context, key string, index int64, value any) *redis.StatusCmd

	// SAdd adds the specified members to the set stored at key.
	SAdd(ctx context.Context, key string, members ...any) *redis.IntCmd
	// SRem removes the specified members from the set stored at key.
	SRem(ctx context.Context, key string, members ...any) *redis.IntCmd
	// SPop removes and returns one random member from the set stored at key.
	SPop(ctx context.Context, key string) *redis.StringCmd
	// SPopN removes and returns count random members from the set stored at key.
	SPopN(ctx context.Context, key string, count int64) *redis.StringSliceCmd

	// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	// ZRem removes the specified members from the sorted set stored at key.
	ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd
	// ZIncrBy increments the score of member in the sorted set stored at key by increment.
	ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd
	// ZRemRangeByRank removes all elements in the sorted set stored at key with rank between start and stop.
	ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd
	// ZRemRangeByScore removes all elements in the sorted set stored at key with a score between minScore and maxScore.
	ZRemRangeByScore(ctx context.Context, key, minScore, maxScore string) *redis.IntCmd

	// Publish posts message to the specified channel.
	Publish(ctx context.Context, channel string, message any) *redis.IntCmd
}

// ScriptCommands groups commands for Redis Lua scripting.
type ScriptCommands interface {
	// Eval evaluates a Lua script server-side.
	Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd
	// EvalSha evaluates a script cached on the server by its SHA1 digest.
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd
	// ScriptExists returns information about the existence of the scripts in the script cache.
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	// ScriptLoad loads a script into the scripts cache without executing it.
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

// Client is the connection contract for the redis client used in this
// project. It exposes only the commands the application relies on; the
// underlying go-redis client is hidden behind the thin wrapper.
type Client interface { //nolint:interfacebloat // curated go-redis subset; replaced by redis.UniversalClient in PR 2 (backlog item R6)
	ReadCommands
	WriteCommands
	ScriptCommands
	// Pipeline returns a pipeliner that batches commands for a single round
	// trip. Call Exec to flush the queued commands.
	Pipeline() Pipeline
	// TxPipeline returns a pipeliner that executes commands inside a MULTI...EXEC
	// transaction block in a single round trip. Call Exec to flush the queued commands.
	TxPipeline() Pipeline
	// Pipelined executes fn inside a pipeline and flushes it on completion.
	Pipelined(ctx context.Context, fn func(pipe Pipeline) error) error
	// TxPipelined executes fn inside a transaction pipeline and flushes it on completion.
	TxPipelined(ctx context.Context, fn func(pipe Pipeline) error) error
	// Watch prepares a transaction and marks keys to be watched for conditional execution (optimistic locking).
	Watch(ctx context.Context, fn func(tx *Tx) error, keys ...string) error
	// Subscribe subscribes the client to the specified channels.
	Subscribe(ctx context.Context, channels ...string) *PubSub
	// PoolStats returns connection pool statistics for health checks and monitoring.
	PoolStats() *PoolStats
	// Close releases the underlying connection pool.
	Close() error
}

// Pipeline batches commands for a single round trip. Queued commands are only
// sent to redis when Exec is called; read their results afterwards.
//
// Note: A Pipeline instance is NOT safe for concurrent use across multiple
// goroutines. Use a distinct pipeline per goroutine or use Client.Pipelined /
// Client.TxPipelined for safe closure-scoped pipelining.
type Pipeline interface {
	ReadCommands
	WriteCommands
	ScriptCommands
	// Exec sends all the queued commands to redis in a single round trip.
	Exec(ctx context.Context) error
}

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

// Option configures a Client returned by New.
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
// the resulting config (standalone, sentinel, or cluster), attaches the instrumentation
// hook, pings it, and returns an instrumented Client.
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
		stopCh = make(chan struct{})
		baseAttrs := map[string]any{
			"server.address": serverAddr,
			"server.port":    serverPort,
			"db.system.name": "redis",
			"db.namespace":   strconv.Itoa(cfg.DB),
		}
		go func(c underlyingClient, m metrics.Client, interval time.Duration, stop <-chan struct{}, attrs map[string]any) {
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
		underlyingClient: cli,
		stopPoolMetrics:  stopCh,
	}, nil
}

// createUnderlyingClient instantiates the concrete go-redis client for the configured topology.
func createUnderlyingClient(cfg Config, tlsCfg *tls.Config) underlyingClient {
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

// underlyingClient abstracts *redis.Client and *redis.ClusterClient so that
// standalone, sentinel, and cluster topologies share the same client implementation.
type underlyingClient interface {
	redis.Cmdable
	AddHook(redis.Hook)
	Pipeline() redis.Pipeliner
	TxPipeline() redis.Pipeliner
	PoolStats() *redis.PoolStats
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	Watch(ctx context.Context, fn func(tx *redis.Tx) error, keys ...string) error
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

// client implements Client by embedding underlyingClient, so the
// exported methods are promoted and need no re-wrapping.
type client struct {
	underlyingClient
	stopPoolMetrics chan struct{}
	stopOnce        sync.Once
}

// Pipeline returns a pipeliner that batches commands for a single round trip.
func (c *client) Pipeline() Pipeline {
	return &pipeline{Pipeliner: c.underlyingClient.Pipeline()}
}

// TxPipeline returns a pipeliner that executes commands inside a MULTI...EXEC
// transaction block in a single round trip.
func (c *client) TxPipeline() Pipeline {
	return &pipeline{Pipeliner: c.underlyingClient.TxPipeline()}
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
	return c.underlyingClient.Close()
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
