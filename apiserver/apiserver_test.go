package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

func TestNew_CreatesServer(t *testing.T) {
	mc := newTestMetricsClient(t)
	tc := newTestTracerClient(t)

	srv, err := New(Config{Addr: "127.0.0.1:0"}, mc, tc)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.Hertz() == nil {
		t.Fatal("expected non-nil Hertz server")
	}
}

func TestNew_WithTimeouts(t *testing.T) {
	mc := newTestMetricsClient(t)
	tc := newTestTracerClient(t)

	srv, err := New(Config{
		Addr:         "127.0.0.1:0",
		ReadTimeout:  5 * 1000000000,  // 5s in nanoseconds
		WriteTimeout: 10 * 1000000000, // 10s in nanoseconds
	}, mc, tc)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.Hertz() == nil {
		t.Fatal("expected non-nil Hertz server")
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	mc := newTestMetricsClient(t)
	tc := newTestTracerClient(t)

	if _, err := New(Config{Addr: "127.0.0.1:0", ReadTimeout: -time.Second}, mc, tc); err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestServer_RouteRegistration(t *testing.T) {
	mc := newTestMetricsClient(t)
	tc := newTestTracerClient(t)

	srv, err := New(Config{Addr: "127.0.0.1:0"}, mc, tc)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	srv.Hertz().GET("/test", func(ctx context.Context, c *app.RequestContext) {
		c.JSON(200, map[string]string{"status": "ok"})
	})

	if srv.Hertz().Engine == nil {
		t.Fatal("expected non-nil engine")
	}
}

func TestServer_MetricsClientRequired(t *testing.T) {
	tc := newTestTracerClient(t)

	mc := newTestMetricsClient(t)
	srv, _ := New(Config{Addr: "127.0.0.1:0"}, mc, tc)

	if srv.metricsClient == nil {
		t.Fatal("expected metricsClient to be set")
	}
}

func TestServer_TracerClientRequired(t *testing.T) {
	mc := newTestMetricsClient(t)

	tc := newTestTracerClient(t)
	srv, _ := New(Config{Addr: "127.0.0.1:0"}, mc, tc)

	if srv.tracerClient == nil {
		t.Fatal("expected tracerClient to be set")
	}
}
