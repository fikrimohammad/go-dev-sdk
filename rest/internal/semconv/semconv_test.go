package semconv_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/rest/internal/semconv"
)

func TestInstruments(t *testing.T) {
	tests := []struct {
		name string
		got  metrics.Instrument
		want metrics.Instrument
	}{
		{
			name: "server request duration",
			got:  semconv.ServerRequestDuration,
			want: metrics.Instrument{Name: "http.server.request.duration", Unit: "s", Description: "Duration of HTTP server requests."},
		},
		{
			name: "server active requests",
			got:  semconv.ServerActiveRequests,
			want: metrics.Instrument{Name: "http.server.active_requests", Unit: "{request}", Description: "Number of active HTTP server requests."},
		},
		{
			name: "client request duration",
			got:  semconv.ClientRequestDuration,
			want: metrics.Instrument{Name: "http.client.request.duration", Unit: "s", Description: "Duration of HTTP client requests."},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("instrument = %+v, want %+v", tt.got, tt.want)
			}
		})
	}
}

func TestErrsCodeKey(t *testing.T) {
	if got, want := semconv.ErrsCodeKey, "errs.code"; got != want {
		t.Errorf("ErrsCodeKey = %q, want %q", got, want)
	}
}

func TestMethod(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		wantMethod   string
		wantOriginal string
	}{
		{name: "GET passes through", raw: "GET", wantMethod: "GET"},
		{name: "HEAD passes through", raw: "HEAD", wantMethod: "HEAD"},
		{name: "POST passes through", raw: "POST", wantMethod: "POST"},
		{name: "PUT passes through", raw: "PUT", wantMethod: "PUT"},
		{name: "DELETE passes through", raw: "DELETE", wantMethod: "DELETE"},
		{name: "CONNECT passes through", raw: "CONNECT", wantMethod: "CONNECT"},
		{name: "OPTIONS passes through", raw: "OPTIONS", wantMethod: "OPTIONS"},
		{name: "TRACE passes through", raw: "TRACE", wantMethod: "TRACE"},
		{name: "PATCH passes through", raw: "PATCH", wantMethod: "PATCH"},
		{name: "lowercase is not known", raw: "get", wantMethod: "_OTHER", wantOriginal: "get"},
		{name: "unknown extension method", raw: "PROPFIND", wantMethod: "_OTHER", wantOriginal: "PROPFIND"},
		{name: "empty method", raw: "", wantMethod: "_OTHER", wantOriginal: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method, original := semconv.Method(tt.raw)
			if method != tt.wantMethod || original != tt.wantOriginal {
				t.Errorf("Method(%q) = (%q, %q), want (%q, %q)", tt.raw, method, original, tt.wantMethod, tt.wantOriginal)
			}
		})
	}
}

func TestSpanName(t *testing.T) {
	tests := []struct {
		name   string
		method string
		route  string
		want   string
	}{
		{name: "method and route", method: "GET", route: "/users/:id", want: "GET /users/:id"},
		{name: "method without route", method: "GET", route: "", want: "GET"},
		{name: "other method with route", method: "_OTHER", route: "/hook", want: "HTTP /hook"},
		{name: "other method without route", method: "_OTHER", route: "", want: "HTTP"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := semconv.SpanName(tt.method, tt.route); got != tt.want {
				t.Errorf("SpanName(%q, %q) = %q, want %q", tt.method, tt.route, got, tt.want)
			}
		})
	}
}

func TestProtocolVersion(t *testing.T) {
	tests := []struct {
		name  string
		proto string
		want  string
	}{
		{name: "HTTP 1.1 gives 1.1", proto: "HTTP/1.1", want: "1.1"},
		{name: "HTTP 2 gives 2", proto: "HTTP/2", want: "2"},
		{name: "HTTP 2.0 gives 2", proto: "HTTP/2.0", want: "2"},
		{name: "HTTP 3 gives 3", proto: "HTTP/3", want: "3"},
		{name: "HTTP 3.0 gives 3", proto: "HTTP/3.0", want: "3"},
		{name: "empty input", proto: "", want: ""},
		{name: "prefix without version", proto: "HTTP/", want: ""},
		{name: "version without prefix", proto: "1.1", want: ""},
		{name: "other protocol", proto: "SPDY/3", want: ""},
		{name: "prefix is case-sensitive", proto: "http/1.1", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := semconv.ProtocolVersion(tt.proto); got != tt.want {
				t.Errorf("ProtocolVersion(%q) = %q, want %q", tt.proto, got, tt.want)
			}
		})
	}
}

func TestScheme(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "https gives https", raw: "https", want: "https"},
		{name: "HTTPS gives https", raw: "HTTPS", want: "https"},
		{name: "mixed case gives https", raw: "HtTpS", want: "https"},
		{name: "http gives http", raw: "http", want: "http"},
		{name: "unknown scheme folds to http", raw: "zzz9", want: "http"},
		{name: "empty scheme folds to http", raw: "", want: "http"},
		{name: "scheme with punctuation folds to http", raw: "x-a.b+c", want: "http"},
		{name: "wss folds to http", raw: "wss", want: "http"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := semconv.Scheme(tt.raw); got != tt.want {
				t.Errorf("Scheme(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestErrsCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantOK   bool
	}{
		{name: "nil error", err: nil, wantCode: 0, wantOK: false},
		{name: "errs error", err: errs.New(404, "not found"), wantCode: 404, wantOK: true},
		{name: "wrapped errs error", err: fmt.Errorf("load order: %w", errs.New(409, "conflict")), wantCode: 409, wantOK: true},
		{name: "errs error with zero code", err: errs.New(0, "unknown"), wantCode: 0, wantOK: false},
		{name: "plain error", err: errors.New("boom"), wantCode: 0, wantOK: false},
		{name: "typed nil errs error", err: (*errs.Error)(nil), wantCode: 0, wantOK: false},
		{name: "wrapped typed nil errs error", err: fmt.Errorf("load order: %w", (*errs.Error)(nil)), wantCode: 0, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, ok := semconv.ErrsCode(tt.err)
			if code != tt.wantCode || ok != tt.wantOK {
				t.Errorf("ErrsCode(%v) = (%d, %t), want (%d, %t)", tt.err, code, ok, tt.wantCode, tt.wantOK)
			}
		})
	}
}

func TestServerErrorType(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   string
	}{
		{name: "200 has no error type", status: 200, want: ""},
		{name: "404 has no error type", status: 404, want: ""},
		{name: "499 has no error type", status: 499, want: ""},
		{name: "500 is status string", status: 500, want: "500"},
		{name: "503 is status string", status: 503, want: "503"},
		{name: "recovered panic reports 500", status: 500, want: "500"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := semconv.ServerErrorType(tt.status); got != tt.want {
				t.Errorf("ServerErrorType(%d) = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestClientErrorType(t *testing.T) {
	other := errors.New("connection refused")
	tests := []struct {
		name   string
		status int
		err    error
		want   string
	}{
		{name: "200 without error", status: 200, err: nil, want: ""},
		{name: "399 without error", status: 399, err: nil, want: ""},
		{name: "no response and no error", status: 0, err: nil, want: ""},
		{name: "400 is status string", status: 400, err: nil, want: "400"},
		{name: "404 is status string", status: 404, err: nil, want: "404"},
		{name: "503 is status string", status: 503, err: nil, want: "503"},
		{name: "deadline exceeded is timeout", status: 0, err: context.DeadlineExceeded, want: "timeout"},
		{name: "wrapped deadline is timeout", status: 0, err: fmt.Errorf("rest/client: %w", context.DeadlineExceeded), want: "timeout"},
		{name: "deadline wrapped with other error is timeout", status: 0, err: fmt.Errorf("%w: %w", context.DeadlineExceeded, other), want: "timeout"},
		{name: "canceled is canceled", status: 0, err: context.Canceled, want: "canceled"},
		{name: "other failure is _OTHER", status: 0, err: other, want: "_OTHER"},
		{name: "failure beats status", status: 503, err: other, want: "_OTHER"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := semconv.ClientErrorType(tt.status, tt.err); got != tt.want {
				t.Errorf("ClientErrorType(%d, %v) = %q, want %q", tt.status, tt.err, got, tt.want)
			}
		})
	}
}
