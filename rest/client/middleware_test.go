package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/config"
	herrs "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-servicekit/rest/internal/semconv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

func TestDeadline_TimesOutAtTheRightLayer(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		ctxTimeout time.Duration // zero leaves the call context without a deadline
	}{
		{
			name:       "context deadline only",
			cfg:        Config{Timeout: 5 * time.Second, AttemptTimeout: 5 * time.Second},
			ctxTimeout: 50 * time.Millisecond,
		},
		{
			// One attempt, so retry does not add its waits to the bound under test.
			name: "attempt timeout only",
			cfg:  Config{AttemptTimeout: 50 * time.Millisecond, MaxAttempts: 1},
		},
		{
			name: "call timeout only",
			cfg:  Config{Timeout: 50 * time.Millisecond, AttemptTimeout: 5 * time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor, _ := newSlowVendor(t, 500*time.Millisecond)
			c, err := New(tt.cfg)
			if err != nil {
				t.Fatalf("New(%+v) error = %v", tt.cfg, err)
			}
			ctx := t.Context()
			if tt.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.ctxTimeout)
				t.Cleanup(cancel)
			}

			start := time.Now()
			_, _, err = sendGET(ctx, c, vendor.URL+"/slow")
			elapsed := time.Since(start)

			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("GET %s error = %v, want errors.Is DeadlineExceeded", vendor.URL, err)
			}
			if !errors.Is(err, herrs.ErrTimeout) {
				t.Errorf("GET %s error = %v, want errors.Is herrs.ErrTimeout", vendor.URL, err)
			}
			if elapsed >= 300*time.Millisecond {
				t.Errorf("GET %s returned after %v, want under 300ms", vendor.URL, elapsed)
			}
		})
	}
}

func TestDeadline_SlowBodyTimesOut(t *testing.T) {
	// The vendor sends its headers at once and then trickles a 40-byte body
	// one byte every 25ms, so the body takes about a second. The timeout
	// reaches the body read as a connection deadline, not as a Hertz timeout.
	tests := []struct {
		name       string
		cfg        Config
		ctxTimeout time.Duration // zero leaves the call context without a deadline
	}{
		{
			name: "attempt timeout during the body",
			cfg:  Config{AttemptTimeout: 100 * time.Millisecond, MaxAttempts: 1},
		},
		{
			name:       "context deadline during the body",
			cfg:        Config{MaxAttempts: 1},
			ctxTimeout: 100 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newDripVendor(t, 40, 25*time.Millisecond)
			c, err := New(tt.cfg)
			if err != nil {
				t.Fatalf("New(%+v) error = %v", tt.cfg, err)
			}
			ctx := t.Context()
			if tt.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.ctxTimeout)
				t.Cleanup(cancel)
			}

			start := time.Now()
			_, _, err = sendGET(ctx, c, vendor.URL+"/drip")
			elapsed := time.Since(start)

			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("GET %s error = %v, want errors.Is DeadlineExceeded", vendor.URL, err)
			}
			if elapsed >= 400*time.Millisecond {
				t.Errorf("GET %s returned after %v, want under 400ms", vendor.URL, elapsed)
			}
		})
	}
}

func TestDeadline_StopsWithoutSending(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func(parent context.Context) (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "deadline in the past",
			ctx: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithDeadline(parent, time.Now().Add(-time.Second))
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "context cancelled",
			ctx: func(parent context.Context) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(parent)
				cancel()
				return ctx, cancel
			},
			wantErr: context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor, hits := newSlowVendor(t, 500*time.Millisecond)
			buf := captureLogs(t)
			c, err := New(Config{})
			if err != nil {
				t.Fatalf("New(Config{}) error = %v", err)
			}
			ctx, cancel := tt.ctx(t.Context())
			defer cancel()

			_, _, err = sendGET(ctx, c, vendor.URL+"/slow")
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("GET %s error = %v, want errors.Is %v", vendor.URL, err, tt.wantErr)
			}
			if got := hits.Load(); got != 0 {
				t.Errorf("GET %s vendor hits = %d, want 0", vendor.URL, got)
			}
			if got := logRecord(t, buf)["attempts"]; got != float64(0) {
				t.Errorf("GET %s log attempts = %v, want 0", vendor.URL, got)
			}
		})
	}
}

func TestDeadline_CancelDuringAttempt(t *testing.T) {
	// Hertz does not stop an attempt for cancellation, so the call returns
	// when the vendor answers. What must hold is the error and the hit count:
	// the cancellation is reported, and it is not retried.
	vendor, hits := newSlowVendor(t, 300*time.Millisecond)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New(Config{}) error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	timer := time.AfterFunc(50*time.Millisecond, cancel)
	defer timer.Stop()

	_, _, err = sendGET(ctx, c, vendor.URL+"/slow")

	if !errors.Is(err, context.Canceled) {
		t.Errorf("GET %s error = %v, want errors.Is context.Canceled", vendor.URL, err)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("GET %s vendor hits = %d, want 1", vendor.URL, got)
	}
}

func TestDeadline_RestoresCallerTimeout(t *testing.T) {
	// deadline overwrites the Hertz timeout of the request for each attempt.
	// After the call, the caller's own value must be back, or a reused request
	// would bound its next call with this call's attempt timeout.
	tests := []struct {
		name    string
		caller  time.Duration // the caller's request timeout, zero for none
		wantOut time.Duration
	}{
		{name: "no caller timeout is left at zero", caller: 0, wantOut: 0},
		{name: "caller timeout is kept", caller: time.Minute, wantOut: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newVendor(t, okHandler)
			c, err := New(Config{})
			if err != nil {
				t.Fatalf("New(Config{}) error = %v", err)
			}
			req, resp := protocol.AcquireRequest(), protocol.AcquireResponse()
			defer protocol.ReleaseRequest(req)
			defer protocol.ReleaseResponse(resp)
			req.SetRequestURI(vendor.URL + "/x")
			req.SetOptions(config.WithRequestTimeout(tt.caller))

			if err := c.Do(t.Context(), req, resp); err != nil {
				t.Fatalf("Do(caller timeout %v) error = %v", tt.caller, err)
			}
			if got := req.Options().RequestTimeout(); got != tt.wantOut {
				t.Errorf("Do(caller timeout %v) left request timeout %v, want %v", tt.caller, got, tt.wantOut)
			}
		})
	}
}

func TestLogging_DerivesBoundFromCallerTimeout(t *testing.T) {
	// DoTimeout sets the Hertz timeout of the request. logging turns it into a
	// ctx deadline, so the whole call is bounded, not one attempt.
	vendor, _ := newSlowVendor(t, 2*time.Second)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New(Config{}) error = %v", err)
	}
	req, resp := protocol.AcquireRequest(), protocol.AcquireResponse()
	defer protocol.ReleaseRequest(req)
	defer protocol.ReleaseResponse(resp)
	req.SetRequestURI(vendor.URL + "/slow")

	start := time.Now()
	err = c.DoTimeout(t.Context(), req, resp, 50*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("DoTimeout(50ms) on GET %s error = %v, want errors.Is context.DeadlineExceeded", vendor.URL, err)
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("DoTimeout(50ms) on GET %s returned after %v, want under 500ms", vendor.URL, elapsed)
	}
}

func TestLogging_NilResponse(t *testing.T) {
	v := newScriptedVendor(t, vendorStep{status: 503}, vendorStep{status: 200})
	c, err := New(Config{BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	req := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(req)
	req.SetRequestURI(v.URL + "/x")
	req.SetMethod(http.MethodGet)

	if err := c.Do(t.Context(), req, nil); err != nil {
		t.Errorf("Do(GET %s, nil response) error = %v, want nil", v.URL, err)
	}
	if got := v.hits.Load(); got != 2 {
		t.Errorf("Do(GET %s, nil response) vendor hits = %d, want 2", v.URL, got)
	}
}

func TestLogging_RejectsBadURL(t *testing.T) {
	const want = "rest/client: request URL must be http or https with a host"
	tests := []struct {
		name string
		url  string
	}{
		{name: "scheme is not http or https", url: "ftp://x/"},
		{name: "host is empty", url: "http:///x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			c, err := New(Config{})
			if err != nil {
				t.Fatalf("New(Config{}) error = %v", err)
			}

			_, _, err = sendGET(t.Context(), c, tt.url)
			if err == nil || err.Error() != want {
				t.Errorf("GET %s error = %v, want %q", tt.url, err, want)
			}
			if got := logRecord(t, buf)["attempts"]; got != float64(0) {
				t.Errorf("GET %s log attempts = %v, want 0", tt.url, got)
			}
		})
	}
}

func TestLogging_RedactsUserinfo(t *testing.T) {
	vendor := newVendor(t, okHandler)
	refused := closedAddress(t)
	tests := []struct {
		name string
		url  string
	}{
		{name: "success", url: "http://u:secret@" + strings.TrimPrefix(vendor.URL, "http://") + "/x?token=secret"},
		{name: "connection refused", url: "http://u:secret@" + refused + "/x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			c, err := New(Config{MaxAttempts: 1})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			_, _, _ = sendGET(t.Context(), c, tt.url)

			logRecord(t, buf)
			if strings.Contains(buf.String(), "secret") {
				t.Errorf("GET %s log = %q, want no secret", tt.url, buf.String())
			}
			if strings.Contains(buf.String(), "u:") {
				t.Errorf("GET %s log = %q, want no userinfo", tt.url, buf.String())
			}
		})
	}
}

func TestServerAddress(t *testing.T) {
	tests := []struct {
		name      string
		authority string
		want      string
	}{
		{name: "host with port", authority: "vendor.example:443", want: "vendor.example"},
		{name: "host without port", authority: "vendor.example", want: "vendor.example"},
		{name: "IPv6 with port", authority: "[::1]:8080", want: "::1"},
		{name: "bare IPv6 host", authority: "[::1]", want: "::1"},
		{name: "empty", authority: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := serverAddress(tt.authority); got != tt.want {
				t.Errorf("serverAddress(%q) = %q, want %q", tt.authority, got, tt.want)
			}
		})
	}
}

func TestLoggingLine(t *testing.T) {
	vendor := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/missing":
			w.WriteHeader(http.StatusNotFound)
		case "/slow":
			select {
			case <-r.Context().Done():
			case <-time.After(500 * time.Millisecond):
			}
		default:
			okHandler(w, r)
		}
	})
	tests := []struct {
		name       string
		cfg        Config
		target     string // path and query appended to the vendor URL
		wantPath   string
		wantLevel  string
		wantStatus int // zero when no response is expected
		wantErr    bool
	}{
		{
			name:       "success logs at debug with the query left out",
			target:     "/x?api_key=secret&x=1",
			wantPath:   "/x",
			wantLevel:  "DEBUG",
			wantStatus: http.StatusOK,
		},
		{
			name:       "404 logs at warn",
			target:     "/missing",
			wantPath:   "/missing",
			wantLevel:  "WARN",
			wantStatus: http.StatusNotFound,
		},
		{
			name:      "timeout logs at warn with error and no status",
			cfg:       Config{AttemptTimeout: 50 * time.Millisecond, MaxAttempts: 1},
			target:    "/slow",
			wantPath:  "/slow",
			wantLevel: "WARN",
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			c, err := New(tt.cfg)
			if err != nil {
				t.Fatalf("New(%+v) error = %v", tt.cfg, err)
			}
			_, _, err = get(t, c, vendor.URL+tt.target)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GET %s error = %v, wantErr %v", tt.target, err, tt.wantErr)
			}

			rec := logRecord(t, buf)
			if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "api_key") {
				t.Errorf("log output = %q, want no query string", buf.String())
			}

			if got := rec["level"]; got != tt.wantLevel {
				t.Errorf("GET %s log level = %v, want %s", tt.target, got, tt.wantLevel)
			}
			if got := rec["msg"]; got != "request" {
				t.Errorf("GET %s log msg = %v, want %q", tt.target, got, "request")
			}
			wantStrings := map[string]string{
				"http.request.method": "GET",
				"server.address":      "127.0.0.1",
				"url.path":            tt.wantPath,
			}
			for key, want := range wantStrings {
				if got := rec[key]; got != want {
					t.Errorf("GET %s log %s = %v, want %q", tt.target, key, got, want)
				}
			}
			if got, ok := rec["attempts"].(float64); !ok || got != 1 {
				t.Errorf("GET %s log attempts = %v, want 1", tt.target, rec["attempts"])
			}
			if got, ok := rec["duration"].(float64); !ok || got <= 0 {
				t.Errorf("GET %s log duration = %v, want a positive number", tt.target, rec["duration"])
			}
			if tt.wantStatus != 0 {
				if got := rec["http.response.status_code"]; got != float64(tt.wantStatus) {
					t.Errorf("GET %s log status = %v, want %d", tt.target, got, tt.wantStatus)
				}
			}
			if tt.wantErr {
				if got, ok := rec["error"].(string); !ok || got == "" {
					t.Errorf("GET %s log error = %v, want a non-empty string", tt.target, rec["error"])
				}
			}

			wantKeys := []string{"attempts", "duration", "http.request.method", "server.address", "url.path"}
			if tt.wantStatus != 0 {
				wantKeys = append(wantKeys, "http.response.status_code")
			}
			if tt.wantErr {
				wantKeys = append(wantKeys, "error")
			}
			slices.Sort(wantKeys)
			if got := logFieldKeys(rec); !slices.Equal(got, wantKeys) {
				t.Errorf("GET %s log keys = %v, want %v", tt.target, got, wantKeys)
			}
		})
	}
}

// logFieldKeys returns the sorted keys of a decoded log line that belong to
// the request, leaving out the record's own fields and the service identity
// that logs adds to every line.
func logFieldKeys(rec map[string]any) []string {
	var keys []string
	for key := range rec {
		switch {
		case key == "time", key == "level", key == "msg":
		case strings.HasPrefix(key, "service."), strings.HasPrefix(key, "deployment."):
		default:
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

func TestRetry(t *testing.T) {
	idempotent := func(req *protocol.Request) {
		req.SetBody([]byte("payload"))
		req.Header.Set("Idempotency-Key", "key-1")
	}
	stream := func(req *protocol.Request) {
		req.SetBodyStream(strings.NewReader("x"), 1)
	}
	tests := []struct {
		name         string
		maxAttempts  int                     // zero uses the default of 3
		method       string                  // HTTP method of the request
		prepare      func(*protocol.Request) // nil sends the request as it is
		steps        []vendorStep
		wantHits     int64
		wantErr      bool // the call returns an error instead of a response
		wantStatus   int  // final status when wantErr is false
		wantAttempts int  // attempts in the log line
		want         seenRequest
	}{
		{
			name:         "GET 503 three times ends with the 503",
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 503}, {status: 503}, {status: 503}},
			wantHits:     3,
			wantStatus:   503,
			wantAttempts: 3,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET 503 then 200 succeeds",
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 503}, {status: 200}},
			wantHits:     2,
			wantStatus:   200,
			wantAttempts: 2,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET 502 then 200 succeeds",
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 502}, {status: 200}},
			wantHits:     2,
			wantStatus:   200,
			wantAttempts: 2,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET 504 then 200 succeeds",
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 504}, {status: 200}},
			wantHits:     2,
			wantStatus:   200,
			wantAttempts: 2,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "POST 503 is not retried",
			method:       http.MethodPost,
			steps:        []vendorStep{{status: 503}},
			wantHits:     1,
			wantStatus:   503,
			wantAttempts: 1,
			want:         seenRequest{method: http.MethodPost},
		},
		{
			name:         "POST with Idempotency-Key 503 then 200 sends the same body again",
			method:       http.MethodPost,
			prepare:      idempotent,
			steps:        []vendorStep{{status: 503}, {status: 200}},
			wantHits:     2,
			wantStatus:   200,
			wantAttempts: 2,
			want:         seenRequest{method: http.MethodPost, body: "payload", idempotencyKey: "key-1"},
		},
		{
			name:         "GET 500 is not retried",
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 500}},
			wantHits:     1,
			wantStatus:   500,
			wantAttempts: 1,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET 404 is not retried",
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 404}},
			wantHits:     1,
			wantStatus:   404,
			wantAttempts: 1,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET with the connection dropped once then 200 succeeds",
			method:       http.MethodGet,
			steps:        []vendorStep{{drop: true}, {status: 200}},
			wantHits:     2,
			wantStatus:   200,
			wantAttempts: 2,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET with the connection dropped on every attempt returns the error",
			maxAttempts:  2,
			method:       http.MethodGet,
			steps:        []vendorStep{{drop: true}},
			wantHits:     2,
			wantErr:      true,
			wantAttempts: 2,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET 429 twice then 200 succeeds",
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 429}, {status: 429}, {status: 200}},
			wantHits:     3,
			wantStatus:   200,
			wantAttempts: 3,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "GET 503 with one attempt is not retried",
			maxAttempts:  1,
			method:       http.MethodGet,
			steps:        []vendorStep{{status: 503}},
			wantHits:     1,
			wantStatus:   503,
			wantAttempts: 1,
			want:         seenRequest{method: http.MethodGet},
		},
		{
			name:         "PUT with a stream body 503 is not retried",
			method:       http.MethodPut,
			prepare:      stream,
			steps:        []vendorStep{{status: 503}},
			wantHits:     1,
			wantStatus:   503,
			wantAttempts: 1,
			want:         seenRequest{method: http.MethodPut, body: "x"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newScriptedVendor(t, tt.steps...)
			buf := captureLogs(t)
			c, err := New(Config{
				MaxAttempts: tt.maxAttempts,
				BackoffBase: time.Millisecond,
				BackoffMax:  5 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			status, err := send(t.Context(), c, tt.method, vendor.URL+"/x", tt.prepare)
			switch {
			case tt.wantErr && err == nil:
				t.Errorf("%s %s error = nil, want an error", tt.method, vendor.URL)
			case !tt.wantErr && err != nil:
				t.Errorf("%s %s error = %v, want nil", tt.method, vendor.URL, err)
			case !tt.wantErr && status != tt.wantStatus:
				t.Errorf("%s %s status = %d, want %d", tt.method, vendor.URL, status, tt.wantStatus)
			}
			if got := vendor.hits.Load(); got != tt.wantHits {
				t.Errorf("%s %s vendor hits = %d, want %d", tt.method, vendor.URL, got, tt.wantHits)
			}
			if got := logRecord(t, buf)["attempts"]; got != float64(tt.wantAttempts) {
				t.Errorf("%s %s log attempts = %v, want %d", tt.method, vendor.URL, got, tt.wantAttempts)
			}
			for _, got := range vendor.requests() {
				if got != tt.want {
					t.Errorf("vendor received %+v, want %+v", got, tt.want)
				}
			}
		})
	}
}

func TestRetry_ReaderBackedMultipartNotRetried(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*protocol.Request)
		content string // the part content the vendor must receive
	}{
		{
			name: "file reader",
			prepare: func(req *protocol.Request) {
				req.SetFileReader("file", "a.txt", strings.NewReader("file-content"))
			},
			content: "file-content",
		},
		{
			name: "field reader",
			prepare: func(req *protocol.Request) {
				req.SetMultipartField("field", "", "text/plain", strings.NewReader("field-content"))
			},
			content: "field-content",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newScriptedVendor(t, vendorStep{status: 503}, vendorStep{status: 200})
			buf := captureLogs(t)
			c, err := New(Config{BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			status, err := send(t.Context(), c, http.MethodPut, vendor.URL+"/x", tt.prepare)
			if err != nil {
				t.Fatalf("PUT %s error = %v, want nil", vendor.URL, err)
			}
			if status != http.StatusServiceUnavailable {
				t.Errorf("PUT %s status = %d, want %d", vendor.URL, status, http.StatusServiceUnavailable)
			}
			if got := vendor.hits.Load(); got != 1 {
				t.Errorf("PUT %s vendor hits = %d, want 1: a reader cannot be sent twice", vendor.URL, got)
			}
			if got := logRecord(t, buf)["attempts"]; got != float64(1) {
				t.Errorf("PUT %s log attempts = %v, want 1", vendor.URL, got)
			}
			reqs := vendor.requests()
			if len(reqs) != 1 || !strings.Contains(reqs[0].body, tt.content) {
				t.Errorf("PUT %s vendor received %q, want a body with %q", vendor.URL, reqs, tt.content)
			}
		})
	}
}

func TestRetry_ErrorClassification(t *testing.T) {
	unknownCA := httptest.NewTLSServer(http.HandlerFunc(okHandler))
	t.Cleanup(unknownCA.Close)
	plain := newVendor(t, okHandler)
	drop := newScriptedVendor(t, vendorStep{drop: true})
	slow := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
		okHandler(w, r)
	})

	tests := []struct {
		name         string
		url          string
		cfg          Config
		wantAttempts int
	}{
		{
			name:         "unknown certificate authority is not retried",
			url:          unknownCA.URL + "/x",
			wantAttempts: 1,
		},
		{
			name:         "TLS to a plain HTTP port is not retried",
			url:          strings.Replace(plain.URL, "http://", "https://", 1) + "/x",
			wantAttempts: 1,
		},
		{
			name:         "unsupported scheme is never sent",
			url:          "ftp://x/",
			wantAttempts: 0,
		},
		{
			name:         "empty host is never sent",
			url:          "http:///x",
			wantAttempts: 0,
		},
		{
			name:         "refused connection is retried",
			url:          "http://" + closedAddress(t) + "/x",
			wantAttempts: 3,
		},
		{
			name:         "dropped connection is retried",
			url:          drop.URL + "/x",
			wantAttempts: 3,
		},
		{
			name:         "per-attempt timeout is retried",
			url:          slow.URL + "/x",
			cfg:          Config{AttemptTimeout: 50 * time.Millisecond},
			wantAttempts: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			cfg := tt.cfg
			cfg.BackoffBase, cfg.BackoffMax = time.Millisecond, 5*time.Millisecond
			c, err := New(cfg)
			if err != nil {
				t.Fatalf("New(%+v) error = %v", cfg, err)
			}

			_, _, err = sendGET(t.Context(), c, tt.url)
			if err == nil {
				t.Errorf("GET %s error = nil, want an error", tt.url)
			}
			if got := logRecord(t, buf)["attempts"]; got != float64(tt.wantAttempts) {
				t.Errorf("GET %s log attempts = %v, want %d", tt.url, got, tt.wantAttempts)
			}
		})
	}
}

func TestRetryErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool // whether the error may be retried
	}{
		{name: "certificate verification", err: &tls.CertificateVerificationError{}, want: false},
		{name: "TLS record header", err: tls.RecordHeaderError{Msg: "not TLS"}, want: false},
		{name: "TLS alert from the peer", err: &net.OpError{Op: "remote error", Err: errors.New("bad certificate")}, want: false},
		{name: "DNS name not found", err: &net.DNSError{Name: "x", IsNotFound: true}, want: false},
		{name: "body too large", err: fmt.Errorf("read: %w", herrs.ErrBodyTooLarge), want: false},
		{name: "protocol not supported", err: herrs.ErrNotSupportProtocol, want: false},
		{name: "DNS timeout", err: &net.DNSError{Name: "x", IsTimeout: true}, want: true},
		{name: "connection reset", err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}, want: true},
		{name: "dropped connection", err: io.EOF, want: true},
		{name: "informational response", err: fmt.Errorf("%w 103", ErrInformationalResponse), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryErr(tt.err); got != tt.want {
				t.Errorf("retryErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestDeadline_InformationalResponseIsAnError(t *testing.T) {
	// The vendor answers /hints with a 103 before its 200, on a keep-alive
	// connection. Hertz returns the 103 as the final response and leaves the
	// 200 on the connection. The second call must not read that leftover
	// response, so its body must be its own.
	base := newEarlyHintsVendor(t)
	buf := captureLogs(t)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New(Config{}) error = %v", err)
	}

	_, _, err = get(t, c, base+"/hints")
	if !errors.Is(err, ErrInformationalResponse) {
		t.Errorf("GET %s/hints error = %v, want errors.Is ErrInformationalResponse", base, err)
	}
	if got := logRecord(t, buf)["attempts"]; got != float64(1) {
		t.Errorf("GET %s/hints log attempts = %v, want 1: the error is not retried", base, got)
	}

	status, body, err := get(t, c, base+"/second")
	if err != nil {
		t.Fatalf("GET %s/second after a 1xx error = %v, want nil", base, err)
	}
	if status != http.StatusOK {
		t.Errorf("GET %s/second status = %d, want %d", base, status, http.StatusOK)
	}
	if want := "for /second"; body != want {
		t.Errorf("GET %s/second body = %q, want %q: the next call read the leftover response", base, body, want)
	}
}

func TestRetry_KeepsSkipBody(t *testing.T) {
	v := newScriptedVendor(t, vendorStep{status: 503}, vendorStep{status: 200})
	c, err := New(Config{BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	req, resp := protocol.AcquireRequest(), protocol.AcquireResponse()
	defer protocol.ReleaseRequest(req)
	defer protocol.ReleaseResponse(resp)
	req.SetRequestURI(v.URL + "/x")
	req.SetMethod(http.MethodGet)
	resp.SkipBody = true

	if err := c.Do(t.Context(), req, resp); err != nil {
		t.Fatalf("Do(GET %s, SkipBody) error = %v", v.URL, err)
	}
	if got := v.hits.Load(); got != 2 {
		t.Errorf("Do(GET %s, SkipBody) vendor hits = %d, want 2", v.URL, got)
	}
	if !resp.SkipBody {
		t.Errorf("Do(GET %s, SkipBody) left SkipBody = false, want true", v.URL)
	}
	if got := resp.Body(); len(got) != 0 {
		t.Errorf("Do(GET %s, SkipBody) body = %q, want none", v.URL, got)
	}
}

func TestRetry_ResetsFailedAttempt(t *testing.T) {
	// The failed attempt sends a Retry-After header and a body. Reset must
	// clear both, so the final response carries only what the last attempt
	// sent.
	v := newScriptedVendor(t,
		vendorStep{status: 503, retryAfter: "0"},
		vendorStep{status: 200},
	)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	req, resp := protocol.AcquireRequest(), protocol.AcquireResponse()
	defer protocol.ReleaseRequest(req)
	defer protocol.ReleaseResponse(resp)
	req.SetRequestURI(v.URL + "/x")
	req.SetMethod(http.MethodGet)

	if err := c.Do(t.Context(), req, resp); err != nil {
		t.Fatalf("Do(GET %s) error = %v", v.URL, err)
	}
	if got := string(resp.Header.Peek("Retry-After")); got != "" {
		t.Errorf("Do(GET %s) final Retry-After = %q, want none", v.URL, got)
	}
	if got, want := string(resp.Body()), "vendor body"; got != want {
		t.Errorf("Do(GET %s) body = %q, want %q", v.URL, got, want)
	}
}

func TestRetryAfter(t *testing.T) {
	tests := []struct {
		name          string
		steps         []vendorStep
		maxRetryAfter time.Duration // zero uses the default of 10s
		wantHits      int64
		wantStatus    int
		maxElapsed    time.Duration // zero means no upper bound
		minElapsed    time.Duration // zero means no lower bound
	}{
		{
			name: "seconds header delays the retry",
			steps: []vendorStep{
				{status: 429, retryAfter: "1"},
				{status: 200},
			},
			wantHits:   2,
			wantStatus: 200,
			minElapsed: 900 * time.Millisecond,
		},
		{
			name: "header longer than MaxRetryAfter returns the 429 at once",
			steps: []vendorStep{
				{status: 429, retryAfter: "30"},
				{status: 200},
			},
			maxRetryAfter: 10 * time.Second,
			wantHits:      1,
			wantStatus:    429,
			maxElapsed:    500 * time.Millisecond,
		},
		{
			name: "header of 5s with MaxRetryAfter of 1s returns the 429 at once",
			steps: []vendorStep{
				{status: 429, retryAfter: "5"},
				{status: 200},
			},
			maxRetryAfter: time.Second,
			wantHits:      1,
			wantStatus:    429,
			maxElapsed:    500 * time.Millisecond,
		},
		{
			name: "Retry-After is ignored on 502, so jitter is used",
			steps: []vendorStep{
				{status: 502, retryAfter: "5"},
				{status: 200},
			},
			wantHits:   2,
			wantStatus: 200,
			maxElapsed: time.Second,
		},
		{
			name: "seconds too large for an int stop the retry",
			steps: []vendorStep{
				{status: 503, retryAfter: "99999999999999999999"},
				{status: 200},
			},
			wantHits:   1,
			wantStatus: 503,
			maxElapsed: 500 * time.Millisecond,
		},
		{
			name: "negative seconds fall back to jitter",
			steps: []vendorStep{
				{status: 503, retryAfter: "-5"},
				{status: 200},
			},
			wantHits:   2,
			wantStatus: 200,
			maxElapsed: time.Second,
		},
		{
			name: "garbage falls back to jitter",
			steps: []vendorStep{
				{status: 429, retryAfter: "soon"},
				{status: 200},
			},
			wantHits:   2,
			wantStatus: 200,
			maxElapsed: time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newScriptedVendor(t, tt.steps...)
			c, err := New(Config{MaxRetryAfter: tt.maxRetryAfter})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			start := time.Now()
			status, err := send(t.Context(), c, http.MethodGet, vendor.URL+"/x", nil)
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("GET %s error = %v, want nil", vendor.URL, err)
			}
			if status != tt.wantStatus {
				t.Errorf("GET %s status = %d, want %d", vendor.URL, status, tt.wantStatus)
			}
			if got := vendor.hits.Load(); got != tt.wantHits {
				t.Errorf("GET %s vendor hits = %d, want %d", vendor.URL, got, tt.wantHits)
			}
			if tt.minElapsed > 0 && elapsed < tt.minElapsed {
				t.Errorf("GET %s returned after %v, want at least %v", vendor.URL, elapsed, tt.minElapsed)
			}
			if tt.maxElapsed > 0 && elapsed > tt.maxElapsed {
				t.Errorf("GET %s returned after %v, want at most %v", vendor.URL, elapsed, tt.maxElapsed)
			}
		})
	}
}

func TestRetryAfterHTTPDate(t *testing.T) {
	// The date is built inside the test, so it is one second ahead of the
	// call. The HTTP date has one second of resolution, so the delay is at
	// most one second and only the upper bound is tight.
	date := time.Now().Add(time.Second).UTC().Format(http.TimeFormat)
	vendor := newScriptedVendor(t,
		vendorStep{status: 503, retryAfter: date},
		vendorStep{status: 200},
	)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	start := time.Now()
	status, err := send(t.Context(), c, http.MethodGet, vendor.URL+"/x", nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("GET %s error = %v, want nil", vendor.URL, err)
	}
	if status != http.StatusOK {
		t.Errorf("GET %s status = %d, want %d", vendor.URL, status, http.StatusOK)
	}
	if got := vendor.hits.Load(); got != 2 {
		t.Errorf("GET %s vendor hits = %d, want 2", vendor.URL, got)
	}
	if elapsed <= 0 || elapsed > 2*time.Second {
		t.Errorf("GET %s returned after %v, want a delay of up to 1s", vendor.URL, elapsed)
	}
}

func TestRetryStopsWhenDelayPassesDeadline(t *testing.T) {
	vendor := newScriptedVendor(t,
		vendorStep{status: 503, retryAfter: "1"},
		vendorStep{status: 200},
	)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	status, err := send(ctx, c, http.MethodGet, vendor.URL+"/x", nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("GET %s error = %v, want nil", vendor.URL, err)
	}
	if status != http.StatusServiceUnavailable {
		t.Errorf("GET %s status = %d, want %d", vendor.URL, status, http.StatusServiceUnavailable)
	}
	if got := vendor.hits.Load(); got != 1 {
		t.Errorf("GET %s vendor hits = %d, want 1", vendor.URL, got)
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("GET %s returned after %v, want well before the 1s Retry-After", vendor.URL, elapsed)
	}
}

func TestRetryWaitEndsWhenCancelled(t *testing.T) {
	// Retry-After makes the wait exactly 2s. Full jitter would make it random,
	// so a short jitter could finish the call before the cancel.
	vendor := newScriptedVendor(t, vendorStep{status: 503, retryAfter: "2"})
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()

	start := time.Now()
	_, err = send(ctx, c, http.MethodGet, vendor.URL+"/x", nil)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("GET %s error = %v, want errors.Is context.Canceled", vendor.URL, err)
	}
	if elapsed >= time.Second {
		t.Errorf("GET %s returned after %v, want under 1s", vendor.URL, elapsed)
	}
	if got := vendor.hits.Load(); got != 1 {
		t.Errorf("GET %s vendor hits = %d, want 1", vendor.URL, got)
	}
}

func TestRetryRetriesAttemptTimeout(t *testing.T) {
	vendor := newScriptedVendor(t,
		vendorStep{status: 200, delay: 500 * time.Millisecond},
		vendorStep{status: 200},
	)
	c, err := New(Config{
		AttemptTimeout: 50 * time.Millisecond,
		BackoffBase:    time.Millisecond,
		BackoffMax:     5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	status, err := send(t.Context(), c, http.MethodGet, vendor.URL+"/x", nil)

	if err != nil {
		t.Fatalf("GET %s error = %v, want nil", vendor.URL, err)
	}
	if status != http.StatusOK {
		t.Errorf("GET %s status = %d, want %d", vendor.URL, status, http.StatusOK)
	}
	if got := vendor.hits.Load(); got != 2 {
		t.Errorf("GET %s vendor hits = %d, want 2", vendor.URL, got)
	}
}

func TestBackoff_Range(t *testing.T) {
	const samples = 1000
	tests := []struct {
		name    string
		base    time.Duration
		maxWait time.Duration
		n       int
		wantCap time.Duration // delays must fall in [0, wantCap)
	}{
		{name: "first retry is below the base", base: 100 * time.Millisecond, maxWait: 2 * time.Second, n: 1, wantCap: 100 * time.Millisecond},
		{name: "third retry doubles twice", base: 100 * time.Millisecond, maxWait: 2 * time.Second, n: 3, wantCap: 400 * time.Millisecond},
		{name: "later retries are capped at the max", base: 100 * time.Millisecond, maxWait: 2 * time.Second, n: 10, wantCap: 2 * time.Second},
		{name: "a shift past the width is capped at the max", base: 100 * time.Millisecond, maxWait: 2 * time.Second, n: 100, wantCap: 2 * time.Second},
		{name: "base equal to max gives max on the first retry", base: time.Second, maxWait: time.Second, n: 1, wantCap: time.Second},
		{name: "base equal to max gives max on later retries", base: time.Second, maxWait: time.Second, n: 5, wantCap: time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{BackoffBase: tt.base, BackoffMax: tt.maxWait}
			for range samples {
				if got := backoff(cfg, tt.n); got < 0 || got >= tt.wantCap {
					t.Fatalf("backoff(base %v, max %v, n %d) = %v, want in [0, %v)", tt.base, tt.maxWait, tt.n, got, tt.wantCap)
				}
			}
		})
	}
}

func TestBackoff_NeverPanics(t *testing.T) {
	cfg := Config{BackoffBase: 100 * time.Millisecond, BackoffMax: 2 * time.Second}
	for n := 1; n <= 100; n++ {
		if got := backoff(cfg, n); got < 0 || got >= cfg.BackoffMax {
			t.Errorf("backoff(n %d) = %v, want in [0, %v)", n, got, cfg.BackoffMax)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	const maxDuration = time.Duration(maxRetryAfterSeconds) * time.Second
	tests := []struct {
		name   string
		in     string
		want   time.Duration
		wantOK bool
	}{
		{name: "seconds", in: "1", want: time.Second, wantOK: true},
		{name: "seconds with a plus sign", in: "+5", want: 5 * time.Second, wantOK: true},
		{name: "negative seconds fall back", in: "-5", want: 0, wantOK: false},
		{name: "garbage falls back", in: "abc", want: 0, wantOK: false},
		{name: "empty falls back", in: "", want: 0, wantOK: false},
		{name: "past HTTP date means no wait", in: "Mon, 02 Jan 2006 15:04:05 GMT", want: 0, wantOK: true},
		{name: "seconds past int64 clamp to the largest duration", in: "99999999999999999999", want: maxDuration, wantOK: true},
		{name: "seconds past the duration range clamp to the largest duration", in: "9999999999999", want: maxDuration, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseRetryAfter(tt.in)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("parseRetryAfter(%q) = %v, %v, want %v, %v", tt.in, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestParseRetryAfter_FutureDate(t *testing.T) {
	date := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	got, ok := parseRetryAfter(date)
	if !ok || got <= 59*time.Minute || got > time.Hour+time.Second {
		t.Errorf("parseRetryAfter(%q) = %v, %v, want about an hour, true", date, got, ok)
	}
}

func TestTelemetry_OneSpanAndRecordPerAttempt(t *testing.T) {
	vendor := newScriptedVendor(t, vendorStep{status: 503})
	c, tel := newTelemetryClient(t, Config{MaxAttempts: 3, BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	ctx, caller := tel.provider.Tracer("test").Start(t.Context(), "caller")

	status, err := send(ctx, c, http.MethodGet, vendor.URL+"/x", nil)
	caller.End()

	if err != nil {
		t.Fatalf("GET %s error = %v, want nil", vendor.URL, err)
	}
	if status != http.StatusServiceUnavailable {
		t.Errorf("GET %s status = %d, want %d", vendor.URL, status, http.StatusServiceUnavailable)
	}
	if got := vendor.hits.Load(); got != 3 {
		t.Fatalf("GET %s vendor hits = %d, want 3", vendor.URL, got)
	}

	spans := clientSpans(tel.spans)
	if len(spans) != 3 {
		t.Fatalf("client spans = %d, want 3", len(spans))
	}
	for i, s := range spans {
		if got, want := s.Parent().SpanID(), caller.SpanContext().SpanID(); got != want {
			t.Errorf("span %d parent = %s, want the caller span %s", i, got, want)
		}
		if got := s.SpanKind(); got != trace.SpanKindClient {
			t.Errorf("span %d kind = %v, want %v", i, got, trace.SpanKindClient)
		}
		if got := s.Name(); got != http.MethodGet {
			t.Errorf("span %d name = %q, want %q", i, got, http.MethodGet)
		}
		if got := s.Status(); got.Code != codes.Error || got.Description != "" {
			t.Errorf("span %d status = %v %q, want Error with no description", i, got.Code, got.Description)
		}
		attrs := spanAttributes(s.Attributes())
		got, present := attrs[otelsemconv.HTTPRequestResendCountKey]
		if i == 0 && present {
			t.Errorf("span 0 resend_count = %v, want absent on the first attempt", got)
		}
		if i > 0 && got != i {
			t.Errorf("span %d resend_count = %v, want %d", i, got, i)
		}
		if got := attrs[otelsemconv.HTTPResponseStatusCodeKey]; got != http.StatusServiceUnavailable {
			t.Errorf("span %d status code = %v, want %d", i, got, http.StatusServiceUnavailable)
		}
		if got := attrs[otelsemconv.ErrorTypeKey]; got != "503" {
			t.Errorf("span %d error.type = %v, want %q", i, got, "503")
		}
	}

	records := tel.metrics.Records()
	if len(records) != 3 {
		t.Fatalf("duration records = %d, want 3", len(records))
	}
	for i, r := range records {
		if r.instrument != semconv.ClientRequestDuration {
			t.Errorf("record %d instrument = %+v, want %+v", i, r.instrument, semconv.ClientRequestDuration)
		}
		if r.instrument.Unit != "s" {
			t.Errorf("record %d unit = %q, want %q", i, r.instrument.Unit, "s")
		}
	}
}

func TestTelemetry_Contract(t *testing.T) {
	// Every row calls an httptest vendor at 127.0.0.1 over http, for /x. So the
	// server address, the port, the URL, and the scheme are the same in every
	// row, and each row lists only the other attributes. The test adds the
	// common ones, so each expected set is exact.
	keep := func(parent context.Context) context.Context { return parent }
	cancelled := func(parent context.Context) context.Context {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx
	}
	okOnly := func(w http.ResponseWriter, r *http.Request) { okHandler(w, r) }
	notFound := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }
	unavailable := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }
	slow := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
		okHandler(w, r)
	}

	tests := []struct {
		name       string
		method     string
		cfg        Config
		handler    http.HandlerFunc
		ctx        func(parent context.Context) context.Context // the call context, built from the test context
		attempts   int                                          // attempts, and so spans and records, each attempt makes
		spanName   string                                       // span name
		spanStatus codes.Code                                   // span status code
		span       map[attribute.Key]any                        // span attributes besides the common ones
		metric     map[attribute.Key]any                        // metric attributes besides the common ones
	}{
		{
			name:       "200",
			method:     http.MethodGet,
			handler:    okOnly,
			ctx:        keep,
			attempts:   1,
			spanName:   "GET",
			spanStatus: codes.Unset,
			span: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusOK,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
			},
			metric: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusOK,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
			},
		},
		{
			name:       "404 is an error with its status as error.type",
			method:     http.MethodGet,
			handler:    notFound,
			ctx:        keep,
			attempts:   1,
			spanName:   "GET",
			spanStatus: codes.Error,
			span: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusNotFound,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
				otelsemconv.ErrorTypeKey:              "404",
			},
			metric: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusNotFound,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
				otelsemconv.ErrorTypeKey:              "404",
			},
		},
		{
			name:       "503 after retries is measured on every attempt",
			method:     http.MethodGet,
			cfg:        Config{MaxAttempts: 3, BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond},
			handler:    unavailable,
			ctx:        keep,
			attempts:   3,
			spanName:   "GET",
			spanStatus: codes.Error,
			span: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusServiceUnavailable,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
				otelsemconv.ErrorTypeKey:              "503",
			},
			metric: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusServiceUnavailable,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
				otelsemconv.ErrorTypeKey:              "503",
			},
		},
		{
			name:       "attempt timeout has no status or protocol",
			method:     http.MethodGet,
			cfg:        Config{AttemptTimeout: 50 * time.Millisecond, MaxAttempts: 1},
			handler:    slow,
			ctx:        keep,
			attempts:   1,
			spanName:   "GET",
			spanStatus: codes.Error,
			span: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey: "GET",
				otelsemconv.ErrorTypeKey:         "timeout",
			},
			metric: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey: "GET",
				otelsemconv.ErrorTypeKey:         "timeout",
			},
		},
		{
			name:       "cancelled call has error.type canceled and no attempt sent",
			method:     http.MethodGet,
			handler:    okOnly,
			ctx:        cancelled,
			attempts:   1,
			spanName:   "GET",
			spanStatus: codes.Error,
			span: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey: "GET",
				otelsemconv.ErrorTypeKey:         "canceled",
			},
			metric: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey: "GET",
				otelsemconv.ErrorTypeKey:         "canceled",
			},
		},
		{
			name:       "unknown method is _OTHER with the original and span name HTTP",
			method:     "PROPFIND",
			handler:    okOnly,
			ctx:        keep,
			attempts:   1,
			spanName:   "HTTP",
			spanStatus: codes.Unset,
			span: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:         "_OTHER",
				otelsemconv.HTTPRequestMethodOriginalKey: "PROPFIND",
				otelsemconv.HTTPResponseStatusCodeKey:    http.StatusOK,
				otelsemconv.NetworkProtocolVersionKey:    "1.1",
			},
			metric: map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "_OTHER",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusOK,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newVendor(t, tt.handler)
			c, tel := newTelemetryClient(t, tt.cfg)
			port := vendor.Listener.Addr().(*net.TCPAddr).Port

			// The status of the call is not checked here. The telemetry is the
			// contract, and a wrong status would show in it.
			_, _ = send(tt.ctx(t.Context()), c, tt.method, vendor.URL+"/x", nil)

			spans := clientSpans(tel.spans)
			if len(spans) != tt.attempts {
				t.Fatalf("%s client spans = %d, want %d", tt.method, len(spans), tt.attempts)
			}
			for i, s := range spans {
				want := map[attribute.Key]any{
					otelsemconv.ServerAddressKey: "127.0.0.1",
					otelsemconv.ServerPortKey:    port,
					otelsemconv.URLFullKey:       vendor.URL + "/x",
				}
				maps.Copy(want, tt.span)
				if i > 0 {
					want[otelsemconv.HTTPRequestResendCountKey] = i
				}
				checkAttributeSet(t, fmt.Sprintf("span %d", i), spanAttributes(s.Attributes()), want)
				if got := s.Name(); got != tt.spanName {
					t.Errorf("span %d name = %q, want %q", i, got, tt.spanName)
				}
				if got := s.SpanKind(); got != trace.SpanKindClient {
					t.Errorf("span %d kind = %v, want %v", i, got, trace.SpanKindClient)
				}
				if got := s.Status(); got.Code != tt.spanStatus || got.Description != "" {
					t.Errorf("span %d status = %v %q, want %v with no description", i, got.Code, got.Description, tt.spanStatus)
				}
			}

			records := tel.metrics.Records()
			if len(records) != tt.attempts {
				t.Fatalf("%s duration records = %d, want %d", tt.method, len(records), tt.attempts)
			}
			for i, r := range records {
				want := map[attribute.Key]any{
					otelsemconv.ServerAddressKey: "127.0.0.1",
					otelsemconv.ServerPortKey:    port,
					otelsemconv.URLSchemeKey:     "http",
				}
				maps.Copy(want, tt.metric)
				checkAttributeSet(t, fmt.Sprintf("record %d", i), metricAttributes(r.attrs), want)
				if r.instrument != semconv.ClientRequestDuration {
					t.Errorf("record %d instrument = %+v, want %+v", i, r.instrument, semconv.ClientRequestDuration)
				}
			}
		})
	}
}

func TestTracing_ReportsVendorBehindProxy(t *testing.T) {
	// The proxy answers every request, so the vendor host in the URL is never
	// resolved. The telemetry must name the vendor, not the proxy.
	proxy, _ := newProxy(t)
	tests := []struct {
		name     string
		target   string
		wantPort int
	}{
		{name: "explicit port", target: "http://vendor.example:8080/x", wantPort: 8080},
		{name: "default port", target: "http://vendor.example/x", wantPort: 80},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, tel := newTelemetryClient(t, Config{Proxy: proxy.URL, MaxAttempts: 1})
			status, err := send(t.Context(), c, http.MethodGet, tt.target, nil)
			if err != nil || status != http.StatusOK {
				t.Fatalf("GET %s through proxy = %d, %v, want 200 and nil", tt.target, status, err)
			}

			spans := clientSpans(tel.spans)
			if len(spans) != 1 {
				t.Fatalf("client spans = %d, want 1", len(spans))
			}
			checkAttributeSet(t, "span", spanAttributes(spans[0].Attributes()), map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.ServerAddressKey:          "vendor.example",
				otelsemconv.ServerPortKey:             tt.wantPort,
				otelsemconv.URLFullKey:                tt.target,
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusOK,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
			})

			records := tel.metrics.Records()
			if len(records) != 1 {
				t.Fatalf("duration records = %d, want 1", len(records))
			}
			checkAttributeSet(t, "record", metricAttributes(records[0].attrs), map[attribute.Key]any{
				otelsemconv.HTTPRequestMethodKey:      "GET",
				otelsemconv.ServerAddressKey:          "vendor.example",
				otelsemconv.ServerPortKey:             tt.wantPort,
				otelsemconv.URLSchemeKey:              "http",
				otelsemconv.HTTPResponseStatusCodeKey: http.StatusOK,
				otelsemconv.NetworkProtocolVersionKey: "1.1",
			})
		})
	}
}

func TestTracing_SendsNoPropagationHeaders(t *testing.T) {
	// The global propagator would inject trace context and baggage, and the
	// caller's span is sampled, so anything the client did would show here.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	received := make(chan http.Header, 1)
	vendor := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case received <- r.Header.Clone():
		default:
		}
		okHandler(w, r)
	})
	c, tel := newTelemetryClient(t, Config{})
	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatalf("baggage.NewMember error = %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage.New error = %v", err)
	}
	ctx, caller := tel.provider.Tracer("test").Start(t.Context(), "caller")
	ctx = baggage.ContextWithBaggage(ctx, bag)
	if !caller.SpanContext().IsSampled() {
		t.Fatal("caller span is not sampled, so the test would prove nothing")
	}

	_, err = send(ctx, c, http.MethodGet, vendor.URL+"/x", nil)
	caller.End()
	if err != nil {
		t.Fatalf("GET %s error = %v, want nil", vendor.URL, err)
	}

	select {
	case h := <-received:
		for _, name := range []string{"traceparent", "tracestate", "baggage", "X-Request-ID"} {
			if got := h.Get(name); got != "" {
				t.Errorf("GET %s sent %s = %q, want no such header", vendor.URL, name, got)
			}
		}
	default:
		t.Fatalf("GET %s: vendor received no request", vendor.URL)
	}
}

func TestTracing_RedactsSecretsInURL(t *testing.T) {
	// The userinfo and the query values carry secrets, and telemetry must hold
	// none of them. Hertz lowercases the host when it parses the URL, so the
	// expected host is lowercase. The proxy keeps the vendor host unresolved.
	proxy, _ := newProxy(t)
	c, tel := newTelemetryClient(t, Config{Proxy: proxy.URL, MaxAttempts: 1})
	const target = "http://u:secret@HOST/p?api_key=SECRET&x=1&x=2#frag"

	if _, err := send(t.Context(), c, http.MethodGet, target, nil); err != nil {
		t.Fatalf("GET %s through proxy error = %v, want nil", target, err)
	}

	spans := clientSpans(tel.spans)
	if len(spans) != 1 {
		t.Fatalf("client spans = %d, want 1", len(spans))
	}
	attrs := spanAttributes(spans[0].Attributes())
	checkAttributeSet(t, "span", attrs, map[attribute.Key]any{
		otelsemconv.HTTPRequestMethodKey:      "GET",
		otelsemconv.ServerAddressKey:          "host",
		otelsemconv.ServerPortKey:             80,
		otelsemconv.URLFullKey:                "http://host/p?api_key=REDACTED&x=REDACTED&x=REDACTED",
		otelsemconv.HTTPResponseStatusCodeKey: http.StatusOK,
		otelsemconv.NetworkProtocolVersionKey: "1.1",
	})
	sets := []map[attribute.Key]any{attrs}
	for _, r := range tel.metrics.Records() {
		sets = append(sets, metricAttributes(r.attrs))
	}
	for _, set := range sets {
		for k, v := range set {
			if text := fmt.Sprint(k, " ", v); strings.Contains(strings.ToLower(text), "secret") {
				t.Errorf("telemetry attribute %s = %v holds a secret, want none", k, v)
			}
			if strings.HasPrefix(string(k), "http.request.header.") {
				t.Errorf("telemetry has attribute %s, want no request headers", k)
			}
		}
	}
	if strings.Contains(strings.ToLower(spans[0].Name()), "secret") {
		t.Errorf("span name = %q holds a secret, want none", spans[0].Name())
	}
}

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "no query", raw: "http://h/p", want: "http://h/p"},
		{name: "empty path becomes a slash", raw: "http://h", want: "http://h/"},
		{name: "keys keep their order and repeats, values are redacted", raw: "http://h/p?b=1&a=2&b=3", want: "http://h/p?b=REDACTED&a=REDACTED&b=REDACTED"},
		{name: "empty value is redacted", raw: "http://h/p?k=", want: "http://h/p?k=REDACTED"},
		{name: "segment with no equals sign is replaced whole", raw: "http://h/p?flag", want: "http://h/p?REDACTED"},
		{name: "bare token next to a key keeps the key", raw: "http://h/p?a=1&TOKEN", want: "http://h/p?a=REDACTED&REDACTED"},
		{name: "encoded key and value are kept as sent", raw: "http://h/p?a%20b=c%26d", want: "http://h/p?a%20b=REDACTED"},
		{name: "encoded path is kept as sent", raw: "http://h/a%2Fb?x=1", want: "http://h/a%2Fb?x=REDACTED"},
		{name: "userinfo and fragment are dropped", raw: "http://u:p@h/p?k=v#frag", want: "http://h/p?k=REDACTED"},
		{name: "IPv6 host with port", raw: "http://[::1]:8080/p?k=v", want: "http://[::1]:8080/p?k=REDACTED"},
		{name: "empty segments are dropped", raw: "http://h/p?&k=v&", want: "http://h/p?k=REDACTED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactURL(protocol.ParseURI(tt.raw)); got != tt.want {
				t.Errorf("redactURL(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestServerPort(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{name: "explicit port", raw: "http://h:8080/", want: 8080},
		{name: "http without a port is 80", raw: "http://h/", want: 80},
		{name: "https without a port is 443", raw: "https://h/", want: 443},
		{name: "https with an explicit 80", raw: "https://h:80/", want: 80},
		{name: "IPv6 with a port", raw: "http://[::1]:8081/", want: 8081},
		{name: "IPv6 without a port over https", raw: "https://[::1]/", want: 443},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := serverPort(protocol.ParseURI(tt.raw)); got != tt.want {
				t.Errorf("serverPort(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}
