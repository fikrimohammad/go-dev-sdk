// This file holds the package examples. They bind a port and need a collector,
// so they are compiled but not run.

package server_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"github.com/fikrimohammad/go-dev-sdk/rest/server"
	"go.opentelemetry.io/otel"
)

// The server reads the OTel global providers, so a service registers them once
// at startup and passes no options.
func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	info := identity.Default()
	tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		logs.Error(ctx, "tracer", "error", err)
		return
	}
	tracer.SetGlobal(tc)
	mc, err := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		logs.Error(ctx, "metrics", "error", err)
		return
	}
	otel.SetMeterProvider(mc.Provider())

	srv, err := server.New(server.Config{Addr: ":8080"})
	if err != nil {
		logs.Error(ctx, "server config", "error", err)
		return
	}
	srv.Hertz().GET("/ping", func(_ context.Context, c *app.RequestContext) {
		c.String(http.StatusOK, "pong")
	})

	// Run blocks until SIGINT or SIGTERM cancels ctx, then drains.
	if err := srv.Run(ctx); err != nil {
		logs.Error(ctx, "server stopped", "error", err)
	}
	_ = tc.Stop(context.WithoutCancel(ctx))
	_ = mc.Stop(context.WithoutCancel(ctx))
}

// A service that keeps its telemetry clients out of the package defaults
// passes them as options.
func ExampleNew() {
	ctx := context.Background()
	info := identity.Default()
	tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		return
	}
	mc, err := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317", Insecure: true})
	if err != nil {
		return
	}
	srv, err := server.New(server.Config{Addr: ":8080"},
		server.WithMetrics(mc),
		server.WithTracer(tc),
	)
	if err != nil {
		return
	}
	_ = srv.Run(ctx)
}

// Error writes an RFC 9457 problem and aborts the chain. The errs code goes
// into the body and the errs.code attribute.
func ExampleError() {
	srv, err := server.New(server.Config{})
	if err != nil {
		return
	}
	srv.Hertz().POST("/orders", func(ctx context.Context, c *app.RequestContext) {
		if c.Query("amount") == "" {
			server.Error(c, http.StatusBadRequest, errs.New(1001, "invalid amount"))
			return
		}
		c.JSON(http.StatusCreated, map[string]string{"request_id": server.RequestIDFrom(ctx)})
	})
}

// Team middleware runs inside recovery, after the standard stack. Attach it to
// a route group, or call Use before registering routes.
func ExampleServer_Hertz() {
	srv, err := server.New(server.Config{})
	if err != nil {
		return
	}
	secret := []byte("webhook-secret")
	verify := func(ctx context.Context, c *app.RequestContext) {
		sig, err := hex.DecodeString(string(c.Request.Header.Peek("X-Signature")))
		mac := hmac.New(sha256.New, secret)
		mac.Write(c.Request.Body())
		if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
			server.Error(c, http.StatusUnauthorized, errs.New(2001, "invalid webhook signature"))
			return
		}
		c.Next(ctx)
	}
	hooks := srv.Hertz().Group("/webhooks", verify)
	hooks.POST("/payments", func(_ context.Context, c *app.RequestContext) {
		c.Status(http.StatusNoContent)
	})
}
