package apiserver

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type testServer struct {
	addr   string
	engine *server.Hertz
}

func newTestMetricsClient(t *testing.T) metrics.Client {
	t.Helper()
	return metrics.Noop()
}

// metricCall records one measurement for assertions.
type metricCall struct {
	instrument metrics.Instrument
	value      float64
	attrs      map[string]any
}

// recordingMetrics captures Add and Record calls. The embedded noop Client
// supplies the remaining Client methods.
type recordingMetrics struct {
	metrics.Client
	mu      sync.Mutex
	adds    []metricCall
	records []metricCall
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{Client: metrics.Noop()}
}

func (m *recordingMetrics) Add(_ context.Context, instrument metrics.Instrument, value int64, attrs map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.adds = append(m.adds, metricCall{instrument: instrument, value: float64(value), attrs: attrs})
	return nil
}

func (m *recordingMetrics) Record(_ context.Context, instrument metrics.Instrument, value float64, attrs map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, metricCall{instrument: instrument, value: value, attrs: attrs})
	return nil
}

func (m *recordingMetrics) lastAdd() (metricCall, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.adds) == 0 {
		return metricCall{}, false
	}
	return m.adds[len(m.adds)-1], true
}

func (m *recordingMetrics) lastRecord() (metricCall, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.records) == 0 {
		return metricCall{}, false
	}
	return m.records[len(m.records)-1], true
}

func newTestTracerClient(t *testing.T) tracer.Client {
	t.Helper()
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return tracer.Wrap(provider)
}

func setupTest(t *testing.T, middlewares []app.HandlerFunc, routes map[string]app.HandlerFunc) *testServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	hz := server.New(server.WithListener(ln), server.WithExitWaitTime(10*time.Millisecond))

	if len(middlewares) > 0 {
		hz.Use(middlewares...)
	}

	for path, handler := range routes {
		parts := strings.SplitN(path, " ", 2)
		if len(parts) == 2 {
			hz.Handle(parts[0], parts[1], handler)
		}
	}

	go func() { _ = hz.Run() }()

	for range 100 {
		if hz.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	ts := &testServer{addr: ln.Addr().String(), engine: hz}
	t.Cleanup(func() {
		_ = hz.Shutdown(context.Background())
	})

	return ts
}

func (ts *testServer) doRequest(t *testing.T, method, path, body string, headers map[string]string) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != "" {
		reqBody = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+ts.addr+path, reqBody)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp
}

func (ts *testServer) get(t *testing.T, path string, headers map[string]string) *http.Response {
	return ts.doRequest(t, http.MethodGet, path, "", headers)
}

func (ts *testServer) post(t *testing.T, path, body string, headers map[string]string) *http.Response {
	return ts.doRequest(t, http.MethodPost, path, body, headers)
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	_ = resp.Body.Close()
	return string(b)
}

// logReader captures the logger's stdout/stderr output and reads it on demand.
type logReader struct {
	stdout string
	stderr string
}

func (l *logReader) String() string {
	var sb strings.Builder
	for _, path := range []string{l.stdout, l.stderr} {
		data, err := os.ReadFile(path)
		if err == nil {
			sb.Write(data)
		}
	}
	return sb.String()
}

func (l *logReader) Len() int {
	return len(l.String())
}

// initTestLogger configures a logger writing to captured stdout/stderr streams.
func initTestLogger(t *testing.T) *logReader {
	t.Helper()
	dir := t.TempDir()
	stdoutFile, err := os.CreateTemp(dir, "stdout-*.log")
	if err != nil {
		t.Fatalf("create stdout capture file: %v", err)
	}
	stderrFile, err := os.CreateTemp(dir, "stderr-*.log")
	if err != nil {
		t.Fatalf("create stderr capture file: %v", err)
	}
	log, err := logs.New(identity.Info{Name: "test"}, logs.Config{
		Format:      "text",
		Writer:      stdoutFile,
		ErrorWriter: stderrFile,
	})
	if err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		t.Fatalf("initialize test logger: %v", err)
	}
	logs.SetDefault(log)
	t.Cleanup(func() {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
	})
	return &logReader{stdout: stdoutFile.Name(), stderr: stderrFile.Name()}
}
