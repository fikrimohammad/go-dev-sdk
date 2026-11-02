package producer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/nats/producer"
)

// ExampleNew publishes an "order created" event to the "orders" topic.
func ExampleNew() {
	p, err := producer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		fmt.Println("new producer:", err)
		return
	}

	err = p.Register(producer.Config{
		Endpoints: []string{"nats://localhost:4222"},
		Topic:     "orders",
	})
	if err != nil {
		fmt.Println("register producer:", err)
		return
	}

	if err := p.Start(); err != nil {
		fmt.Println("start producer:", err)
		return
	}
	defer func() {
		if err := p.Shutdown(); err != nil {
			fmt.Println("shutdown producer:", err)
		}
	}()

	body, err := json.Marshal(struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}{ID: "order-42", Amount: 1500})
	if err != nil {
		fmt.Println("marshal order:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.PublishSync(ctx, "orders", "created", "order-42", body); err != nil {
		fmt.Println("publish order-42:", err)
	}
}

// ExampleProducer_PublishAsync publishes an "order paid" event without
// waiting for the broker. The callback reports the acknowledgement or error.
func ExampleProducer_PublishAsync() {
	p, err := producer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		fmt.Println("new producer:", err)
		return
	}

	err = p.Register(producer.Config{
		Endpoints: []string{"nats://localhost:4222"},
		Topic:     "orders",
	})
	if err != nil {
		fmt.Println("register producer:", err)
		return
	}

	if err := p.Start(); err != nil {
		fmt.Println("start producer:", err)
		return
	}
	defer func() {
		if err := p.Shutdown(); err != nil {
			fmt.Println("shutdown producer:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	err = p.PublishAsync(ctx, "orders", "paid", "order-42", []byte(`{"id":"order-42"}`),
		func(_ context.Context, ack *jetstream.PubAck, err error) {
			defer close(done)
			if err != nil {
				fmt.Println("async publish order-42 failed:", err)
				return
			}
			fmt.Println("async publish order-42 acked in stream", ack.Stream, "seq", ack.Sequence)
		})
	if err != nil {
		fmt.Println("enqueue order-42:", err)
		return
	}

	// The callback runs on another goroutine. Wait for it before shutting down.
	<-done
}
