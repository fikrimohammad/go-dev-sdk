package redis

import "github.com/redis/go-redis/v9"

// Nil is an alias for redis.Nil, returned when a key does not exist.
const Nil = redis.Nil //nolint:errname // mirrors go-redis's redis.Nil so callers keep its familiar name

// TxFailedErr is an alias for redis.TxFailedErr, returned when a WATCH transaction fails.
var TxFailedErr = redis.TxFailedErr //nolint:errname // mirrors go-redis's redis.TxFailedErr
