package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/nats/consumer"
)

// ExampleNew consumes messages from the "orders" topic as the "billing" group
// until the process receives SIGINT or SIGTERM.
func ExampleNew() {
	c, err := consumer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		fmt.Println("new consumer:", err)
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
			// Returning an error asks the broker to redeliver the message.
			return fmt.Errorf("decode order: %w", err)
		}
		fmt.Println("billing received order", order.ID)
		return nil
	})
	if err != nil {
		fmt.Println("register consumer:", err)
		return
	}

	if err := c.Start(); err != nil {
		fmt.Println("start consumer:", err)
		return
	}
	defer func() {
		if err := c.Shutdown(); err != nil {
			fmt.Println("shutdown consumer:", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
