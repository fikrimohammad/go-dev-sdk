// This file holds compile-only documentation examples for package consumer.
// They need a NATS server, so they have no Output comment and are compiled but
// not run.

package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/nats/consumer"
)

// ExampleNew consumes messages from the "orders" topic as the "billing" group
// until the process receives SIGINT or SIGTERM.
func ExampleNew() {
	c, err := consumer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		log.Println("new consumer:", err)
		return
	}

	err = c.Register(consumer.Config{
		Endpoints: []string{"nats://localhost:4222"},
		Topic:     "orders",
		Group:     "billing",
	}, func(ctx context.Context, body []byte) error {
		var order struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &order); err != nil {
			// Returning an error asks the broker to redeliver the message. A message that
			// can never decode is redelivered until Config.MaxRetryAttempts is reached.
			return fmt.Errorf("decode order: %w", err)
		}
		fmt.Println("billing received order", order.ID)
		return nil
	})
	if err != nil {
		log.Println("register consumer:", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := c.Start(ctx); err != nil {
		log.Println("start consumer:", err)
		return
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Shutdown(shutdownCtx); err != nil {
			log.Println("shutdown consumer:", err)
		}
	}()

	<-ctx.Done()
}
