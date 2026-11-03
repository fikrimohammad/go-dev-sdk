package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fikrimohammad/go-servicekit/rest/internal/semconv"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// installDefaultTelemetry installs a recording tracer provider and a manual
// metric reader as the OTel global providers until the test ends. The test
// reads the measurements from the reader and the spans from the recorder.
func installDefaultTelemetry(t *testing.T) (*sdkmetric.ManualReader, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevTracer, prevMeter := otel.GetTracerProvider(), otel.GetMeterProvider()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevTracer {
			otel.SetTracerProvider(prevTracer)
		}
		if otel.GetMeterProvider() != prevMeter {
			otel.SetMeterProvider(prevMeter)
		}
		_ = tp.Shutdown(context.WithoutCancel(t.Context()))
		_ = mp.Shutdown(context.WithoutCancel(t.Context()))
	})
	return reader, rec
}

// requestDurationCount returns the number of measurements recorded in
// http.client.request.duration so far. It fails the test if the metrics cannot
// be collected.
func requestDurationCount(t *testing.T, reader *sdkmetric.ManualReader) uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	var count uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != semconv.ClientRequestDuration.Name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s data = %T, want Histogram[float64]", m.Name, m.Data)
			}
			for _, dp := range h.DataPoints {
				count += dp.Count
			}
		}
	}
	return count
}

func TestNewUnsetTelemetryUsesDefaults(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{name: "no options"},
		{name: "nil clients", opts: []Option{WithMetrics(nil), WithTracer(nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newVendor(t, okHandler)
			c, err := New(Config{}, tt.opts...)
			if err != nil {
				t.Fatalf("New(Config{}) error = %v", err)
			}
			// The defaults are installed after New, so this also checks that
			// they are read per attempt, not captured by New.
			reader, rec := installDefaultTelemetry(t)
			if _, _, err := get(t, c, vendor.URL+"/x"); err != nil {
				t.Fatalf("GET error = %v", err)
			}
			if got := requestDurationCount(t, reader); got != 1 {
				t.Errorf("default meter got %d http.client.request.duration records, want 1", got)
			}
			if got := len(clientSpans(rec)); got != 1 {
				t.Errorf("default tracer got %d client spans, want 1", got)
			}
		})
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	c, err := New(Config{MaxAttempts: -1})
	if err == nil {
		t.Fatalf("New(MaxAttempts: -1) error = nil, want an error")
	}
	if !strings.HasPrefix(err.Error(), "rest/client: ") {
		t.Errorf("New(MaxAttempts: -1) error = %q, want prefix %q", err.Error(), "rest/client: ")
	}
	if c != nil {
		t.Errorf("New(MaxAttempts: -1) client = %v, want nil", c)
	}
}

func TestNewRequestReachesVendor(t *testing.T) {
	vendor := newVendor(t, okHandler)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New(Config{}) error = %v", err)
	}
	status, body, err := get(t, c, vendor.URL+"/x")
	if err != nil {
		t.Fatalf("GET %s error = %v", vendor.URL, err)
	}
	if status != http.StatusOK {
		t.Errorf("GET %s status = %d, want %d", vendor.URL, status, http.StatusOK)
	}
	if want := "vendor body"; body != want {
		t.Errorf("GET %s body = %q, want %q", vendor.URL, body, want)
	}
}

func TestNew_SendsPathAsGiven(t *testing.T) {
	for _, tt := range sentPaths {
		t.Run(tt.name, func(t *testing.T) {
			vendor, uris := newRequestURIVendor(t)
			c, tel := newTelemetryClient(t, Config{MaxAttempts: 1})

			status, err := send(t.Context(), c, http.MethodGet, vendor.URL+tt.path, nil)
			if err != nil || status != http.StatusOK {
				t.Fatalf("GET %s%s = %d, %v, want 200 and nil", vendor.URL, tt.path, status, err)
			}
			select {
			case got := <-uris:
				if got != tt.path {
					t.Errorf("GET %s%s vendor RequestURI = %q, want %q", vendor.URL, tt.path, got, tt.path)
				}
			default:
				t.Fatalf("GET %s%s: vendor received no request", vendor.URL, tt.path)
			}

			spans := clientSpans(tel.spans)
			if len(spans) != 1 {
				t.Fatalf("client spans = %d, want 1", len(spans))
			}
			if got, want := spanAttributes(spans[0].Attributes())[otelsemconv.URLFullKey], vendor.URL+tt.path; got != want {
				t.Errorf("GET %s%s url.full = %v, want %q", vendor.URL, tt.path, got, want)
			}
		})
	}
}

func TestNewProxy(t *testing.T) {
	proxy, targets := newProxy(t)
	c, err := New(Config{Proxy: proxy.URL})
	if err != nil {
		t.Fatalf("New(Proxy: %q) error = %v", proxy.URL, err)
	}
	// vendor.invalid does not resolve, so the request can only succeed if it
	// is sent to the proxy.
	const target = "http://vendor.invalid/x"
	status, _, err := get(t, c, target)
	if err != nil {
		t.Fatalf("GET %s through proxy %s error = %v", target, proxy.URL, err)
	}
	if status != http.StatusOK {
		t.Errorf("GET %s through proxy status = %d, want %d", target, status, http.StatusOK)
	}
	select {
	case got := <-targets:
		if got != target {
			t.Errorf("proxy saw request target %q, want %q", got, target)
		}
	default:
		t.Errorf("proxy saw no request for %s", target)
	}
}

func TestNew_ProxyCredentials(t *testing.T) {
	proxy, auths := newHeaderProxy(t)
	// The password has an escaped @, so the value the proxy sees is only right
	// when the userinfo is decoded.
	c, err := New(Config{Proxy: strings.Replace(proxy.URL, "http://", "http://user:p%40ss@", 1)})
	if err != nil {
		t.Fatalf("New(Proxy: user:p%%40ss) error = %v", err)
	}
	const target = "http://vendor.invalid/x"
	if _, _, err := get(t, c, target); err != nil {
		t.Fatalf("GET %s through proxy error = %v", target, err)
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:p@ss"))
	select {
	case got := <-auths:
		if got != want {
			t.Errorf("proxy saw Proxy-Authorization %q, want %q", got, want)
		}
	default:
		t.Errorf("proxy saw no request for %s", target)
	}
}

func TestNew_TLSConfigTrustsServer(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(okHandler))
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	c, err := New(Config{TLS: &tls.Config{RootCAs: pool}})
	if err != nil {
		t.Fatalf("New(TLS: pool) error = %v", err)
	}
	status, body, err := get(t, c, srv.URL+"/")
	if err != nil {
		t.Fatalf("GET %s error = %v, want nil", srv.URL, err)
	}
	if status != http.StatusOK {
		t.Errorf("GET %s status = %d, want %d", srv.URL, status, http.StatusOK)
	}
	if want := "vendor body"; body != want {
		t.Errorf("GET %s body = %q, want %q", srv.URL, body, want)
	}
}

func TestNew_NilTLSFailsWithCertificateError(t *testing.T) {
	// A zero Config has no TLS configuration, so the system roots apply and
	// the test certificate is unknown. The failure must be the certificate
	// check. The "not support tls" error of the netpoll dialer would mean New
	// left the standard dialer out.
	srv := httptest.NewTLSServer(http.HandlerFunc(okHandler))
	t.Cleanup(srv.Close)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New(Config{}) error = %v", err)
	}
	_, _, err = get(t, c, srv.URL+"/")
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); !ok {
		t.Errorf("GET %s error = %v, want a *tls.CertificateVerificationError", srv.URL, err)
	}
	if err != nil && strings.Contains(err.Error(), "not support tls") {
		t.Errorf("GET %s error = %q, want no %q", srv.URL, err, "not support tls")
	}
}

func TestNew_ProxyConnectBoundedByDeadline(t *testing.T) {
	// The proxy accepts TCP and never answers the CONNECT. Hertz runs the
	// CONNECT under its own 60s context, so only the connection deadline set
	// by deadlineDialer keeps this call within the caller's 300ms.
	c, err := New(Config{Proxy: newStallingProxy(t)})
	if err != nil {
		t.Fatalf("New(Proxy: stalling) error = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err = sendGET(ctx, c, "https://vendor.invalid/x")
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("GET https://vendor.invalid/x through a stalled proxy error = %v, want errors.Is context.DeadlineExceeded", err)
	}
	if elapsed >= time.Second {
		t.Errorf("GET https://vendor.invalid/x through a stalled proxy returned after %v, want under 1s", elapsed)
	}
}
