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

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/consumer"
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
	}, func(ctx context.Context, msg consumer.Message) error {
		var order struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(msg.Body(), &order); err != nil {
			// Returning consumer.ErrNoRetry (wrapped) terms a message that can never
			// decode. Any other error redelivers it, up to Config.MaxRetryAttempts.
			return fmt.Errorf("%w: decode order: %w", consumer.ErrNoRetry, err)
		}
		// Delivery is at least once, so handlers dedupe on the business key. The
		// same key can arrive again after a redelivery, so check it before side effects:
		//
		//	if seen(msg.Key()) { return nil }
		fmt.Println("billing received order", order.ID, "key", msg.Key())
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
