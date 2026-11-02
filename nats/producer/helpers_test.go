// This file holds the shared test helpers for package producer: an in-process
// JetStream server, stub transports, and a registration shortcut.

package producer

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// useTraceContextPropagator makes W3C trace context the global propagator for the
// test and restores the previous one when the test ends.
func useTraceContextPropagator(t *testing.T) {
	t.Helper()

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })
}

// dialNATS returns a connection to ns for reads in the test. The connection
// closes when the test ends.
func dialNATS(t *testing.T, ns *natsserver.Server) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// startJetStreamProducer returns a Producer for topic on ns, configured with opts.
// It is started, and shut down when the test ends.
func startJetStreamProducer(t *testing.T, ns *natsserver.Server, topic string, opts ...Option) *Producer {
	t.Helper()

	p, err := New(testAppInfo, opts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: topic}); err != nil {
		t.Fatalf("Register(%s) error = %v", topic, err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// startJetStreamServer runs an in-process NATS server with JetStream enabled and
// stops it when the test ends.
func startJetStreamServer(t *testing.T) *natsserver.Server {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS test server not ready for connections")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns
}

// testConfig returns a valid Config for topic that points at a local endpoint.
func testConfig(topic string) Config {
	return Config{Endpoints: []string{"localhost:4222"}, Topic: topic}
}

// stubFactory returns a producerFactory that hands out the stub transport
// registered for each topic.
func stubFactory(pds map[string]jsProducer) producerFactory {
	return func(_ context.Context, cfg Config) (jsProducer, error) {
		pd, ok := pds[cfg.Topic]
		if !ok {
			return nil, fmt.Errorf("no stub for topic %s", cfg.Topic)
		}
		return pd, nil
	}
}

// startStubbed registers every topic in pds, starts p with the stub transports,
// and shuts p down when the test ends.
func startStubbed(t *testing.T, p *Producer, pds map[string]jsProducer) {
	t.Helper()

	p.producerFactory = stubFactory(pds)
	for topic := range pds {
		if err := p.Register(testConfig(topic)); err != nil {
			t.Fatalf("Register(%s) error = %v", topic, err)
		}
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
}

// collectMetrics reads every metric that reader has collected, keyed by name.
func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() error = %v, want nil", err)
	}
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// pointStrings returns one canonical "key=value" string per attribute set, sorted,
// so data points can be compared without caring about their order.
func pointStrings(sets ...attribute.Set) []string {
	out := make([]string, 0, len(sets))
	for _, s := range sets {
		parts := make([]string, 0, s.Len())
		for _, kv := range s.ToSlice() {
			parts = append(parts, string(kv.Key)+"="+kv.Value.String())
		}
		slices.Sort(parts)
		out = append(out, strings.Join(parts, ","))
	}
	slices.Sort(out)
	return out
}

// mapStrings is pointStrings for expected attributes given as a map.
func mapStrings(sets ...map[string]string) []string {
	out := make([]string, 0, len(sets))
	for _, m := range sets {
		parts := make([]string, 0, len(m))
		for k, v := range m {
			parts = append(parts, k+"="+v)
		}
		slices.Sort(parts)
		out = append(out, strings.Join(parts, ","))
	}
	slices.Sort(out)
	return out
}
