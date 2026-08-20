package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Nil is an alias for redis.Nil, returned when a key does not exist.
const Nil = redis.Nil

// TxFailedErr is an alias for redis.TxFailedErr, returned when a WATCH transaction fails.
var TxFailedErr = redis.TxFailedErr

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
type ReadCommands interface {
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
	SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd
	// SMIsMember returns whether each member is a member of the set stored at key.
	SMIsMember(ctx context.Context, key string, members ...interface{}) *redis.BoolSliceCmd
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
	// ZCount returns the number of elements in the sorted set at key with a score between min and max.
	ZCount(ctx context.Context, key, min, max string) *redis.IntCmd
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
type WriteCommands interface {
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
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	// SetNX atomically binds value to key only when key does not already exist,
	// with the given expiration. The command result reports false when the key
	// already exists.
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	// SetXX atomically binds value to key only when key already exists,
	// with the given expiration. The command result reports false when the key
	// does not exist.
	SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	// GetDel atomically returns the value of key and removes key.
	GetDel(ctx context.Context, key string) *redis.StringCmd
	// GetEx atomically returns the value of key and sets its expiration. An
	// expiration of zero persists the key.
	GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd
	// MSet sets the given keys to their respective values.
	MSet(ctx context.Context, values ...interface{}) *redis.StatusCmd
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
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	// HSetNX sets field in the hash stored at key to value, only if field does not yet exist.
	HSetNX(ctx context.Context, key, field string, value interface{}) *redis.BoolCmd
	// HDel removes the specified fields from the hash stored at key.
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	// HIncrBy increments the number stored at field in the hash stored at key by incr.
	HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd
	// HIncrByFloat increments the float value stored at field in the hash stored at key by incr.
	HIncrByFloat(ctx context.Context, key, field string, incr float64) *redis.FloatCmd

	// LPush inserts all the specified values at the head of the list stored at key.
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	// RPush inserts all the specified values at the tail of the list stored at key.
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
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
	LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd
	// LSet sets the list element at index to value.
	LSet(ctx context.Context, key string, index int64, value interface{}) *redis.StatusCmd

	// SAdd adds the specified members to the set stored at key.
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	// SRem removes the specified members from the set stored at key.
	SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	// SPop removes and returns one random member from the set stored at key.
	SPop(ctx context.Context, key string) *redis.StringCmd
	// SPopN removes and returns count random members from the set stored at key.
	SPopN(ctx context.Context, key string, count int64) *redis.StringSliceCmd

	// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	// ZRem removes the specified members from the sorted set stored at key.
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	// ZIncrBy increments the score of member in the sorted set stored at key by increment.
	ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd
	// ZRemRangeByRank removes all elements in the sorted set stored at key with rank between start and stop.
	ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd
	// ZRemRangeByScore removes all elements in the sorted set stored at key with a score between min and max.
	ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd

	// Publish posts message to the specified channel.
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
}

// ScriptCommands groups commands for Redis Lua scripting.
type ScriptCommands interface {
	// Eval evaluates a Lua script server-side.
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	// EvalSha evaluates a script cached on the server by its SHA1 digest.
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	// ScriptExists returns information about the existence of the scripts in the script cache.
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	// ScriptLoad loads a script into the scripts cache without executing it.
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

// Client is the connection contract for the redis client used in this
// project. It exposes only the commands the application relies on; the
// underlying go-redis client is hidden behind the thin wrapper.
type Client interface {
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
