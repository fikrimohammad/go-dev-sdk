// This file holds runnable documentation examples for package redis. They are
// compile-only because they need a live Redis server.

package redis_test

import (
	"context"
	"errors"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/fikrimohammad/go-dev-sdk/redis"
)

// ExampleNew stores a session token with a time-to-live and reads it back,
// treating a missing key as an expected result rather than a failure.
func ExampleNew() {
	client, err := redis.New(redis.Config{Addrs: []string{"localhost:6379"}})
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Println(err)
		}
	}()

	ctx := context.Background()
	if err := client.Set(ctx, "session:42", "active", 10*time.Minute).Err(); err != nil {
		log.Println(err)
		return
	}

	val, err := client.Get(ctx, "session:42").Result()
	switch {
	case errors.Is(err, redis.Nil):
		log.Println("session expired")
	case err != nil:
		log.Println(err)
	default:
		log.Println("session:", val)
	}
}

// ExampleClient_Pipelined sends two commands in a single round trip. Command
// results are available once Pipelined returns nil.
func ExampleClient_Pipelined() {
	client, err := redis.New(redis.Config{Addrs: []string{"localhost:6379"}})
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Println(err)
		}
	}()

	ctx := context.Background()
	var views *goredis.IntCmd
	err = client.Pipelined(ctx, func(pipe redis.Pipeline) error {
		views = pipe.Incr(ctx, "page:home:views")
		pipe.Expire(ctx, "page:home:views", time.Hour)
		return nil
	})
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("views:", views.Val())
}
