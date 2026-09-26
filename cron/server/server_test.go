package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-dev-sdk/apiserver"
	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

type noopMetrics struct{}

func (noopMetrics) Count(context.Context, string, int64, map[string]any) error       { return nil }
func (noopMetrics) Histogram(context.Context, string, float64, map[string]any) error { return nil }
func (noopMetrics) Stop(context.Context) error                                       { return nil }

type mockJobRunner struct {
	jobs        map[string]cron.JobInfo
	triggerFunc func(ctx context.Context, name string) error
}

func (m *mockJobRunner) Jobs() []cron.JobInfo {
	res := make([]cron.JobInfo, 0, len(m.jobs))
	for _, j := range m.jobs {
		res = append(res, j)
	}
	return res
}

func (m *mockJobRunner) Job(name string) (cron.JobInfo, bool) {
	j, ok := m.jobs[name]
	return j, ok
}

func (m *mockJobRunner) Trigger(ctx context.Context, name string) error {
	if m.triggerFunc != nil {
		return m.triggerFunc(ctx, name)
	}
	return nil
}

func setupTestServer(t *testing.T, runner cron.JobRunner, prefix string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	hz := hertzserver.New(hertzserver.WithListener(ln), hertzserver.WithExitWaitTime(10*time.Millisecond))
	RegisterRoutes(hz, runner, prefix)

	go func() { _ = hz.Run() }()

	for i := 0; i < 100; i++ {
		if hz.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cleanup := func() {
		_ = hz.Shutdown(context.Background())
	}
	return ln.Addr().String(), cleanup
}

func TestServer_Endpoints(t *testing.T) {
	runner := &mockJobRunner{
		jobs: map[string]cron.JobInfo{
			"test-job": {
				Name:       "test-job",
				Schedule:   "0 * * * *",
				LastStatus: "idle",
			},
		},
		triggerFunc: func(ctx context.Context, name string) error {
			if name == "fail-job" {
				return errors.New("db error")
			}
			if name == "locked-job" {
				return cron.ErrLockHeld
			}
			if name == "running-job" {
				return cron.ErrJobRunning
			}
			if name == "disabled-job" {
				return cron.ErrJobDisabled
			}
			if name == "stopped-job" {
				return cron.ErrEngineStopped
			}
			if name == "timeout-job" {
				return context.DeadlineExceeded
			}
			return nil
		},
	}
	runner.jobs["fail-job"] = cron.JobInfo{Name: "fail-job"}
	runner.jobs["locked-job"] = cron.JobInfo{Name: "locked-job"}
	runner.jobs["running-job"] = cron.JobInfo{Name: "running-job"}
	runner.jobs["disabled-job"] = cron.JobInfo{Name: "disabled-job"}
	runner.jobs["stopped-job"] = cron.JobInfo{Name: "stopped-job"}
	runner.jobs["timeout-job"] = cron.JobInfo{Name: "timeout-job"}

	addr, cleanup := setupTestServer(t, runner, "")
	defer cleanup()

	client := http.DefaultClient
	baseURL := "http://" + addr

	// 1. GET /healthz
	resp, err := client.Get(baseURL + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz failed: code=%v, err=%v", resp.StatusCode, err)
	}
	_ = resp.Body.Close()

	// 2. GET /jobs
	resp, err = client.Get(baseURL + "/jobs")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /jobs failed: code=%v, err=%v", resp.StatusCode, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), "test-job") {
		t.Fatalf("expected jobs list to contain test-job, got %s", string(body))
	}

	// 3. GET /jobs/test-job
	resp, err = client.Get(baseURL + "/jobs/test-job")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /jobs/test-job failed: code=%v, err=%v", resp.StatusCode, err)
	}
	_ = resp.Body.Close()

	// 4. GET /jobs/unknown
	resp, err = client.Get(baseURL + "/jobs/unknown")
	if err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown job, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 5. POST /jobs/test-job/run (sync success)
	resp, err = client.Post(baseURL+"/jobs/test-job/run", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /jobs/test-job/run failed: code=%v, err=%v", resp.StatusCode, err)
	}
	_ = resp.Body.Close()

	// 6. POST /jobs/locked-job/run (409 Conflict)
	resp, err = client.Post(baseURL+"/jobs/locked-job/run", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for locked-job, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 7. POST /jobs/running-job/run (409 Conflict)
	resp, err = client.Post(baseURL+"/jobs/running-job/run", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for running-job, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 8. POST /jobs/disabled-job/run (422 Unprocessable)
	resp, err = client.Post(baseURL+"/jobs/disabled-job/run", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable for disabled-job, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 9. POST /jobs/stopped-job/run (503 Service Unavailable)
	resp, err = client.Post(baseURL+"/jobs/stopped-job/run", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for stopped-job, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 10. POST /jobs/timeout-job/run (504 Gateway Timeout)
	resp, err = client.Post(baseURL+"/jobs/timeout-job/run", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 for timeout-job, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 11. POST /jobs/fail-job/run (500 Server Error)
	resp, err = client.Post(baseURL+"/jobs/fail-job/run", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 for fail-job, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 12. POST /jobs/test-job/run?async=true (202 Accepted)
	resp, err = client.Post(baseURL+"/jobs/test-job/run?async=true", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for async run, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 13. POST /jobs/test-job/run?async=1 (202 Accepted)
	resp, err = client.Post(baseURL+"/jobs/test-job/run?async=1", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for async=1, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 14. POST /jobs/test-job/run?async=TRUE (202 Accepted)
	resp, err = client.Post(baseURL+"/jobs/test-job/run?async=TRUE", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for async=TRUE, got %v", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestServer_Prefix(t *testing.T) {
	runner := &mockJobRunner{
		jobs: map[string]cron.JobInfo{
			"admin-job": {Name: "admin-job"},
		},
	}

	addr, cleanup := setupTestServer(t, runner, "/admin")
	defer cleanup()

	client := http.DefaultClient
	resp, err := client.Get("http://" + addr + "/admin/jobs")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for prefixed /admin/jobs, got code=%v, err=%v", resp.StatusCode, err)
	}
	_ = resp.Body.Close()
}

func TestServer_NewConstructor(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()
	tc := tracer.Wrap(provider)
	mc := noopMetrics{}

	// nil runner
	if _, err := New(Config{}, nil, mc, tc); err == nil {
		t.Fatal("expected error on nil runner")
	}

	// invalid apiserver config
	if _, err := New(Config{Config: apiserver.Config{Addr: "invalid-addr"}}, &mockJobRunner{}, mc, tc); err == nil {
		t.Fatal("expected error on invalid addr")
	}

	// valid
	srv, err := New(Config{Config: apiserver.Config{Addr: "127.0.0.1:0"}}, &mockJobRunner{}, mc, tc)
	if err != nil {
		t.Fatalf("unexpected error creating server: %v", err)
	}
	if srv.Hertz() == nil {
		t.Fatal("expected non-nil Hertz instance")
	}
}
