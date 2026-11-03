// This file holds the package examples. They call external vendors, so they
// are compiled but not run.

package client_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"time"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-servicekit/observability/logs"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
	"github.com/fikrimohammad/go-servicekit/rest/client"
)

// Build the client once at startup and share it. Without options it reads
// the package-level telemetry defaults. A timed-out POST is not retried, so
// the caller reconciles before it resubmits.
func ExampleNew() {
	ctx := context.Background()
	c, err := client.New(client.Config{AttemptTimeout: 5 * time.Second})
	if err != nil {
		return
	}

	req := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(req)
	resp := protocol.AcquireResponse()
	defer protocol.ReleaseResponse(resp)
	req.SetMethod(http.MethodPost)
	req.SetRequestURI("https://tax.example.com/v1/invoices")
	req.Header.Set("Content-Type", "application/json")
	req.SetBody([]byte(`{"amount":100}`))

	err = c.Do(ctx, req, resp)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		logs.Warn(ctx, "invoice timed out, reconcile before resubmitting")
	case err != nil:
		logs.Error(ctx, "invoice failed", "error", err)
	case resp.StatusCode() != http.StatusCreated:
		// A final 503 comes back with a nil error, so check the status.
		logs.Error(ctx, "invoice rejected", "status", resp.StatusCode())
	}
}

// Pass telemetry clients as options to keep them out of the package defaults.
func ExampleWithMetrics() {
	var (
		mc metrics.Client // from metrics.New
		tc tracer.Client  // from tracer.New
	)
	c, err := client.New(client.Config{}, client.WithMetrics(mc), client.WithTracer(tc))
	if err != nil {
		return
	}
	_ = c
}

// Team middleware added with Use runs innermost, once per attempt, so a
// timestamped signature is made again on every retry.
func ExampleNew_requestSigning() {
	c, err := client.New(client.Config{})
	if err != nil {
		return
	}
	key := []byte("vendor-key")
	c.Use(func(next hclient.Endpoint) hclient.Endpoint {
		return func(ctx context.Context, req *protocol.Request, resp *protocol.Response) error {
			ts := strconv.FormatInt(time.Now().Unix(), 10)
			mac := hmac.New(sha256.New, key)
			mac.Write([]byte(ts + "\n"))
			mac.Write(req.Body())
			req.Header.Set("X-Timestamp", ts)
			req.Header.Set("X-Signature", hex.EncodeToString(mac.Sum(nil)))
			return next(ctx, req, resp)
		}
	})
}

// A vendor SDK that takes an *http.Client gets the same retry, deadline, and
// telemetry rules through RoundTripper.
func ExampleRoundTripper() {
	c, err := client.New(client.Config{})
	if err != nil {
		return
	}
	httpClient := &http.Client{Transport: client.RoundTripper(c)}
	_ = httpClient // pass to the vendor SDK
}
