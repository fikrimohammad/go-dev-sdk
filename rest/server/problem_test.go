package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/fikrimohammad/go-dev-sdk/errs/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// leakMarkers are texts that must never appear in a problem body. They cover
// the debug, cause, and plain-error values used in the rows below, and the
// file name of this test, which would show up in an errs stack frame.
var leakMarkers = []string{"secret-debug", "secret-cause", "secret-plain", "problem_test.go"}

func TestError_problemBody(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		err        error
		wantStatus int
		wantDetail string
		wantCode   int
	}{
		{
			name:       "errs 4xx with code",
			status:     http.StatusBadRequest,
			err:        errs.Wrap(1001, "invalid amount", errors.New("secret-cause")).WithDebug("secret-debug"),
			wantStatus: http.StatusBadRequest,
			wantDetail: "invalid amount",
			wantCode:   1001,
		},
		{
			name:       "errs 4xx with code 0",
			status:     http.StatusUnprocessableEntity,
			err:        errs.New(0, "bad input"),
			wantStatus: http.StatusUnprocessableEntity,
			wantDetail: "bad input",
		},
		{
			name:       "wrapped errs 4xx",
			status:     http.StatusConflict,
			err:        fmt.Errorf("load: %w", errs.New(1002, "already exists")),
			wantStatus: http.StatusConflict,
			wantDetail: "already exists",
			wantCode:   1002,
		},
		{
			name:       "non-errs 4xx",
			status:     http.StatusBadRequest,
			err:        errors.New("secret-plain"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "errs 5xx",
			status:     http.StatusServiceUnavailable,
			err:        errs.Wrap(2001, "db down", errors.New("secret-cause")).WithDebug("secret-debug"),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "status 200 becomes 500",
			status:     http.StatusOK,
			err:        errs.New(1001, "oops"),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "status 600 becomes 500",
			status:     600,
			err:        nil,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "nil error",
			status:     http.StatusNotFound,
			err:        nil,
			wantStatus: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t)
			s.Hertz().GET("/x", func(_ context.Context, c *app.RequestContext) {
				Error(c, tt.status, tt.err)
			})
			rec := perform(s, "GET", "/x", nil)

			if rec.Code != tt.wantStatus {
				t.Errorf("Error(%d, %v) status = %d, want %d", tt.status, tt.err, rec.Code, tt.wantStatus)
			}
			if ct := rec.Header().Get("Content-Type"); ct != problemContentType {
				t.Errorf("Error(%d, %v) Content-Type = %q, want %q", tt.status, tt.err, ct, problemContentType)
			}
			reqID := rec.Header().Get("X-Request-ID")
			if reqID == "" {
				t.Errorf("Error(%d, %v) X-Request-ID is empty, want a request ID", tt.status, tt.err)
			}
			got := decodeProblem(t, rec)
			want := map[string]any{
				"title":      http.StatusText(tt.wantStatus),
				"status":     float64(tt.wantStatus),
				"request_id": reqID,
			}
			if tt.wantDetail != "" {
				want["detail"] = tt.wantDetail
			}
			if tt.wantCode != 0 {
				want["code"] = float64(tt.wantCode)
			}
			if !maps.Equal(got, want) {
				t.Errorf("Error(%d, %v) body = %v, want %v", tt.status, tt.err, got, want)
			}
			for _, marker := range leakMarkers {
				if strings.Contains(rec.Body.String(), marker) {
					t.Errorf("Error(%d, %v) body = %q, want no %q", tt.status, tt.err, rec.Body.String(), marker)
				}
			}
		})
	}
}

func TestFallbackProblems(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantTitle  string
	}{
		{name: "unknown path is 404", method: "GET", path: "/missing", wantStatus: http.StatusNotFound, wantTitle: "Not Found"},
		{name: "wrong method on a route is 405", method: "POST", path: "/thing", wantStatus: http.StatusMethodNotAllowed, wantTitle: "Method Not Allowed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t)
			s.Hertz().GET("/thing", func(_ context.Context, c *app.RequestContext) {
				c.String(http.StatusOK, "ok")
			})
			rec := perform(s, tt.method, tt.path, nil)

			if rec.Code != tt.wantStatus {
				t.Errorf("%s %s status = %d, want %d", tt.method, tt.path, rec.Code, tt.wantStatus)
			}
			if ct := rec.Header().Get("Content-Type"); ct != problemContentType {
				t.Errorf("%s %s Content-Type = %q, want %q", tt.method, tt.path, ct, problemContentType)
			}
			reqID := rec.Header().Get("X-Request-ID")
			if reqID == "" {
				t.Errorf("%s %s X-Request-ID is empty, want a request ID", tt.method, tt.path)
			}
			got := decodeProblem(t, rec)
			want := map[string]any{
				"title":      tt.wantTitle,
				"status":     float64(tt.wantStatus),
				"request_id": reqID,
			}
			if !maps.Equal(got, want) {
				t.Errorf("%s %s body = %v, want %v", tt.method, tt.path, got, want)
			}
		})
	}
}

func TestError_problemTitle(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		wantTitle string
	}{
		{name: "status without standard text is Client Error", status: 499, wantTitle: "Client Error"},
		{name: "server status without standard text is Server Error", status: 599, wantTitle: "Server Error"},
		{name: "standard text is kept", status: http.StatusNotFound, wantTitle: "Not Found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t)
			s.Hertz().GET("/x", func(_ context.Context, c *app.RequestContext) {
				Error(c, tt.status, nil)
			})
			rec := perform(s, "GET", "/x", nil)

			if got := decodeProblem(t, rec)["title"]; got != tt.wantTitle {
				t.Errorf("Error(%d, nil) title = %v, want %q", tt.status, got, tt.wantTitle)
			}
		})
	}
}

func TestNoRoute_seenByMiddleware(t *testing.T) {
	s := newTestServer(t)
	var route string
	var status int
	// Middleware added with Use runs around the NoRoute handler, so it sees the
	// unmatched route as "" and the 404 status. The requestID layer, which is
	// outside it, must also have run for the 404.
	s.Hertz().Use(func(ctx context.Context, c *app.RequestContext) {
		c.Next(ctx)
		route = c.FullPath()
		status = c.Response.StatusCode()
	})
	rec := perform(s, "GET", "/missing", nil)

	if route != "" {
		t.Errorf("GET /missing FullPath() = %q, want empty", route)
	}
	if status != http.StatusNotFound {
		t.Errorf("GET /missing status seen by middleware = %d, want %d", status, http.StatusNotFound)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Errorf("GET /missing X-Request-ID is empty, want the outer requestID layer to set it")
	}
}

func TestError_typedNilIsNotErrs(t *testing.T) {
	captureLogs(t)
	s, mc, rec := newTelemetryServer(t, Config{})
	s.Hertz().GET("/typed", func(_ context.Context, c *app.RequestContext) {
		var e *errs.Error
		Error(c, http.StatusBadRequest, e)
	})
	recorded := perform(s, "GET", "/typed", nil)

	if recorded.Code != http.StatusBadRequest {
		t.Errorf("GET /typed status = %d, want %d: a typed nil error must not panic", recorded.Code, http.StatusBadRequest)
	}
	want := map[string]any{
		"title":      "Bad Request",
		"status":     float64(http.StatusBadRequest),
		"request_id": recorded.Header().Get("X-Request-ID"),
	}
	if got := decodeProblem(t, recorded); !maps.Equal(got, want) {
		t.Errorf("Error(400, typed nil) body = %v, want %v", got, want)
	}
	checkRequestMetrics(t, mc, []attribute.KeyValue{
		otelsemconv.HTTPRequestMethodKey.String("GET"),
		otelsemconv.URLSchemeKey.String("http"),
		otelsemconv.HTTPRouteKey.String("/typed"),
		otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusBadRequest),
	})
	if got := onlySpan(t, rec).Status().Code; got != codes.Unset {
		t.Errorf("span status = %v, want %v for a 4xx", got, codes.Unset)
	}
}
