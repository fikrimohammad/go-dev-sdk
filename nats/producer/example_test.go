// This file holds compile-only documentation examples for package producer.
// They need a NATS server, so they have no Output comment and are compiled but
// not run.

package producer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/producer"
)

// ExampleNew publishes an "order created" event to the "orders" topic.
func ExampleNew() {
	p, err := producer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		log.Println("new producer:", err)
		return
	}

	err = p.Register(producer.Config{
		Endpoints: []string{"nats://localhost:4222"},
		Topic:     "orders",
	})
	if err != nil {
		log.Println("register producer:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Start(ctx); err != nil {
		log.Println("start producer:", err)
		return
	}
	defer func() {
		if err := p.Shutdown(ctx); err != nil {
			log.Println("shutdown producer:", err)
		}
	}()

	body, err := json.Marshal(struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}{ID: "order-42", Amount: 1500})
	if err != nil {
		log.Println("marshal order:", err)
		return
	}

	if err := p.PublishSync(ctx, producer.Message{Topic: "orders", Tag: "created", Key: "order-42", Body: body}); err != nil {
		log.Println("publish order-42:", err)
	}
}

// ExampleProducer_PublishAsync publishes an "order paid" event without
// waiting for the broker. The callback reports the acknowledgement or error.
func ExampleProducer_PublishAsync() {
	p, err := producer.New(identity.Info{Name: "orders-service"})
	if err != nil {
		log.Println("new producer:", err)
		return
	}

	err = p.Register(producer.Config{
		Endpoints: []string{"nats://localhost:4222"},
		Topic:     "orders",
	})
	if err != nil {
		log.Println("register producer:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Start(ctx); err != nil {
		log.Println("start producer:", err)
		return
	}
	defer func() {
		if err := p.Shutdown(ctx); err != nil {
			log.Println("shutdown producer:", err)
		}
	}()

	done := make(chan struct{})
	err = p.PublishAsync(ctx, producer.Message{Topic: "orders", Tag: "paid", Key: "order-42", Body: []byte(`{"id":"order-42"}`)}, func(_ context.Context, ack *jetstream.PubAck, err error) {
		defer close(done)
		if err != nil {
			log.Println("async publish order-42 failed:", err)
			return
		}
		fmt.Println("async publish order-42 acked in stream", ack.Stream, "seq", ack.Sequence)
	})
	if err != nil {
		log.Println("enqueue order-42:", err)
		return
	}

	// The callback runs on another goroutine. Wait for it before shutting down.
	<-done
}
