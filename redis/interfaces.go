package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReadCommands groups the commands that read from redis.
type ReadCommands interface {
	// Get returns the value of key. The command result reports redis.Nil when
	// the key does not exist.
	Get(ctx context.Context, key string) *redis.StringCmd
	// Ping verifies that the connection is alive.
	Ping(ctx context.Context) *redis.StatusCmd
}

// WriteCommands groups the commands that write to redis.
type WriteCommands interface {
	// Set binds value to key with the given expiration. An expiration <= 0
	// keeps the key without a time-to-live.
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	// SetNX atomically binds value to key only when key does not already exist,
	// with the given expiration. The command result reports false when the key
	// already exists.
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	// Del removes one or more keys.
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// Client is the connection contract for the redis client used in this
// project. It exposes only the commands the application relies on; the
// underlying go-redis client is hidden behind the thin wrapper.
type Client interface {
	ReadCommands
	WriteCommands
	// Pipeline returns a pipeliner that batches commands for a single round
	// trip. Call Exec to flush the queued commands.
	Pipeline() Pipeline
	// Close releases the underlying connection pool.
	Close() error
}

// Pipeline batches commands for a single round trip. Queued commands are only
// sent to redis when Exec is called; read their results afterwards.
type Pipeline interface {
	ReadCommands
	WriteCommands
	// Exec sends all the queued commands to redis in a single round trip.
	Exec(ctx context.Context) error
}
