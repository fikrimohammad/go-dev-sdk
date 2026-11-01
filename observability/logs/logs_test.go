package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

func testInfo() identity.Info {
	return identity.Info{Name: "testapp", Version: "1.0.0", Environment: "test"}
}

// fakeLogger implements Logger and records nothing.
type fakeLogger struct{}

func (*fakeLogger) Debug(context.Context, string, ...any) {}
func (*fakeLogger) Info(context.Context, string, ...any)  {}
func (*fakeLogger) Warn(context.Context, string, ...any)  {}
func (*fakeLogger) Error(context.Context, string, ...any) {}

// mustLogger builds a Logger, panicking on error (test configs are valid).
func mustLogger(info identity.Info, cfg Config) Logger {
	l, err := New(info, cfg)
	if err != nil {
		panic(err)
	}
	return l
}

// withDefault installs a debug-level default logger writing to w for the
// duration of the test and restores the previous default afterward, isolating
// the package facade from other tests.
func withDefault(t *testing.T, w io.Writer) {
	t.Helper()
	prev := Default()
	SetDefault(newDefaultLogger(w))
	t.Cleanup(func() { SetDefault(prev) })
}

func TestLogger_LevelRouting_Text(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	l := mustLogger(testInfo(), Config{Format: FormatText, Writer: &outBuf, ErrorWriter: &errBuf})
	l.Debug(context.Background(), "debug_msg", "key", "val")
	l.Info(context.Background(), "info_msg")
	l.Warn(context.Background(), "warn_msg")
	l.Error(context.Background(), "error_msg")

	stdoutText, stderrText := outBuf.String(), errBuf.String()
	for _, want := range []string{"debug_msg", "info_msg", "key=val"} {
		if !strings.Contains(stdoutText, want) {
			t.Errorf("Debug/Info stdout = %q, want substring %q", stdoutText, want)
		}
	}
	for _, want := range []string{"warn_msg", "error_msg"} {
		if !strings.Contains(stderrText, want) {
			t.Errorf("Warn/Error stderr = %q, want substring %q", stderrText, want)
		}
	}
	for _, unwanted := range []string{"warn_msg", "error_msg"} {
		if strings.Contains(stdoutText, unwanted) {
			t.Errorf("Warn/Error stdout = %q, want no substring %q", stdoutText, unwanted)
		}
	}
}

func TestLogger_LevelRouting_JSON(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	l := mustLogger(testInfo(), Config{Format: FormatJSON, Writer: &outBuf, ErrorWriter: &errBuf})
	l.Info(context.Background(), "info_json")
	l.Error(context.Background(), "error_json")

	stdoutText, stderrText := outBuf.String(), errBuf.String()
	for _, want := range []string{`"level":"INFO"`, `"msg":"info_json"`} {
		if !strings.Contains(stdoutText, want) {
			t.Errorf("Info stdout = %q, want substring %q", stdoutText, want)
		}
	}
	for _, want := range []string{`"level":"ERROR"`, `"msg":"error_json"`} {
		if !strings.Contains(stderrText, want) {
			t.Errorf("Error stderr = %q, want substring %q", stderrText, want)
		}
	}
}

func TestLogger_LevelFilter(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	l := mustLogger(testInfo(), Config{Level: "warn", Writer: &outBuf, ErrorWriter: &errBuf})
	l.Debug(context.Background(), "filtered debug")
	l.Info(context.Background(), "filtered info")
	l.Warn(context.Background(), "kept warn")
	l.Error(context.Background(), "kept error")

	if got := outBuf.String() + errBuf.String(); strings.Contains(got, "filtered") {
		t.Errorf("Debug/Info with Level=warn output = %q, want no record below warn", got)
	}
	tests := []struct {
		name   string
		substr string
	}{
		{name: "warn is kept on stderr", substr: "kept warn"},
		{name: "error is kept on stderr", substr: "kept error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errBuf.String(); !strings.Contains(got, tt.substr) {
				t.Errorf("Warn/Error stderr = %q, want substring %q", got, tt.substr)
			}
		})
	}
}

func TestLogger_NormalizeArgs(t *testing.T) {
	t.Run("attribute keys are normalized in log args", func(t *testing.T) {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Writer: &outBuf})
		l.Info(context.Background(), "with_args",
			"order_id", 123, "shop_id", 456, slog.String("user_id", "u1"))

		text := outBuf.String()
		for _, want := range []string{"order.id=123", "shop.id=456", "user.id=u1"} {
			if !strings.Contains(text, want) {
				t.Errorf("l.Info(...) output = %q, want substring %q", text, want)
			}
		}
		for _, notWant := range []string{"order_id", "user_id"} {
			if strings.Contains(text, notWant) {
				t.Errorf("l.Info(...) output = %q, want no substring %q", text, notWant)
			}
		}
	})

	t.Run("handles mixed pairs and attrs", func(t *testing.T) {
		out := normalizeArgs([]any{"order_id", 1, slog.String("user_id", "u1")})
		if len(out) != 3 {
			t.Fatalf("normalizeArgs(order_id, 1, user_id attr) len = %d, want 3", len(out))
		}
		if out[0] != "order.id" {
			t.Errorf("normalizeArgs(mixed)[0] = %v, want %q", out[0], "order.id")
		}
		if out[1] != 1 {
			t.Errorf("normalizeArgs(mixed)[1] = %v, want %d", out[1], 1)
		}
		attr, ok := out[2].(slog.Attr)
		if !ok {
			t.Fatalf("normalizeArgs(mixed)[2] = %T, want slog.Attr", out[2])
		}
		if attr.Key != "user.id" {
			t.Errorf("normalizeArgs(mixed)[2].Key = %q, want %q", attr.Key, "user.id")
		}
	})

	t.Run("preserves semantic-convention and correlation keys", func(t *testing.T) {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Writer: &outBuf})
		l.Info(context.Background(), "preserved_keys",
			"trace_id", "trace",
			"span_id", "span",
			"http.response.status_code", 200,
		)

		text := outBuf.String()
		for _, want := range []string{"trace_id=trace", "span_id=span", "http.response.status_code=200"} {
			if !strings.Contains(text, want) {
				t.Errorf("l.Info(...) output = %q, want substring %q", text, want)
			}
		}
	})
}

func TestNew_InvalidInfo(t *testing.T) {
	_, err := New(identity.Info{}, Config{})
	if !errors.Is(err, attributes.ErrMissingServiceName) {
		t.Errorf("New(identity.Info{}) error = %v, want %v", err, attributes.ErrMissingServiceName)
	}
}

func TestNew_NilWritersDefaultToStreams(t *testing.T) {
	// Level error keeps the test from writing to the real stdout or stderr;
	// the check only needs New to accept nil writers.
	l, err := New(testInfo(), Config{Level: "error"})
	if err != nil {
		t.Fatalf("New(Config{Level: error}) error = %v, want nil", err)
	}
	if l == nil {
		t.Errorf("New(Config{Level: error}) = nil, want non-nil logger")
	}
}

func TestHandler_WithAttrsAndGroup(t *testing.T) {
	var outBuf bytes.Buffer
	h := traceHandler{next: &streamHandler{
		stdout: slog.NewJSONHandler(&outBuf, nil),
		stderr: slog.NewJSONHandler(io.Discard, nil),
	}}.WithAttrs([]slog.Attr{slog.String("span_id", "manual-span")}).WithGroup("req")
	slog.New(h).Info("grouped", "k", "v")

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(outBuf.Bytes()), &record); err != nil {
		t.Fatalf("json.Unmarshal(record) error = %v, want nil", err)
	}
	if record["span_id"] != "manual-span" {
		t.Errorf("WithAttrs(span_id).WithGroup(req) record[span_id] = %v, want %q", record["span_id"], "manual-span")
	}
	group, ok := record["req"].(map[string]any)
	if !ok {
		t.Fatalf("WithGroup(req) record[req] = %T, want group object", record["req"])
	}
	if group["k"] != "v" {
		t.Errorf("WithGroup(req) record[req][k] = %v, want %q", group["k"], "v")
	}
}

func TestLogger_GlobalKV(t *testing.T) {
	t.Run("GlobalKV and identity attrs are attached and normalized", func(t *testing.T) {
		var outBuf, errBuf bytes.Buffer
		l := mustLogger(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"},
			Config{GlobalKV: map[string]any{"order_id": "x", "env": "test"}, Writer: &outBuf, ErrorWriter: &errBuf})
		l.Info(context.Background(), "with_global")
		l.Warn(context.Background(), "warn_global")

		streams := []struct {
			name string
			text string
		}{
			{"stdout", outBuf.String()},
			{"stderr", errBuf.String()},
		}
		wants := []string{
			"order.id=x",
			"env=test",
			"service.name=svc",
			"service.version=1.0",
			"deployment.environment.name=prod",
		}
		for _, s := range streams {
			for _, want := range wants {
				if !strings.Contains(s.text, want) {
					t.Errorf("l.Info(with_global)/l.Warn(warn_global) %s = %q, want substring %q", s.name, s.text, want)
				}
			}
		}
	})

	t.Run("identity wins over colliding GlobalKV keys", func(t *testing.T) {
		var outBuf bytes.Buffer
		l := mustLogger(identity.Info{Name: "svc", Version: "1.0", Environment: "prod"},
			Config{GlobalKV: map[string]any{"service.name": "spoofed", "service.version": "0.0.0"}, Writer: &outBuf})
		l.Info(context.Background(), "identity_wins")

		text := outBuf.String()
		for _, notWant := range []string{"spoofed", "0.0.0"} {
			if strings.Contains(text, notWant) {
				t.Errorf("l.Info(...) output = %q, want no substring %q", text, notWant)
			}
		}
		for _, want := range []string{"service.name=svc", "service.version=1.0"} {
			if !strings.Contains(text, want) {
				t.Errorf("l.Info(...) output = %q, want substring %q", text, want)
			}
		}
	})
}

func TestLogger_NilContext(t *testing.T) {
	var outBuf bytes.Buffer
	l := mustLogger(testInfo(), Config{Writer: &outBuf})
	l.Info(nil, "nil_ctx_ok") //nolint:staticcheck // intentionally verifying nil handling

	if got := outBuf.String(); !strings.Contains(got, "nil_ctx_ok") {
		t.Errorf("l.Info(nil, nil_ctx_ok) output = %q, want substring %q", got, "nil_ctx_ok")
	}
}

func TestLogger_DefaultLogger(t *testing.T) {
	var errBuf bytes.Buffer
	l := newDefaultLogger(&errBuf)
	l.Info(context.Background(), "default_msg", "k", "v")

	got := errBuf.String()
	for _, want := range []string{"default_msg", "level=INFO"} {
		if !strings.Contains(got, want) {
			t.Errorf("newDefaultLogger().Info(default_msg, k, v) stderr = %q, want substring %q", got, want)
		}
	}
}

func TestDefault_NeverNil(t *testing.T) {
	if got := Default(); got == nil {
		t.Errorf("Default() = nil, want non-nil before any SetDefault")
	}
}

func TestDefault_BeforeSetDefault(t *testing.T) {
	var errBuf bytes.Buffer
	withDefault(t, &errBuf)
	Info(context.Background(), "pre_set_msg", "k", "v")

	if got := errBuf.String(); !strings.Contains(got, "pre_set_msg") {
		t.Errorf("Info(pre_set_msg) stderr = %q, want substring %q", got, "pre_set_msg")
	}
}

func TestFacade_SetDefault(t *testing.T) {
	withDefault(t, io.Discard)
	fakeLog := &fakeLogger{}
	SetDefault(fakeLog)
	if got := Default(); got != fakeLog {
		t.Errorf("Default() = %v, want %v", got, fakeLog)
	}

	// The fake records nothing, so the calls only need to route without panicking.
	Debug(context.Background(), "x")
	Info(context.Background(), "x")
	Warn(context.Background(), "x")
	Error(context.Background(), "x")
}

func TestFacade_LastWins(t *testing.T) {
	withDefault(t, io.Discard)
	first, second := &fakeLogger{}, &fakeLogger{}
	SetDefault(first)
	if got := Default(); got != first {
		t.Errorf("after SetDefault(first), Default() = %v, want %v", got, first)
	}
	SetDefault(second)
	if got := Default(); got != second {
		t.Errorf("after SetDefault(second), Default() = %v, want %v", got, second)
	}
}

func TestFacade_NewAndSetDefault(t *testing.T) {
	withDefault(t, io.Discard)
	var outBuf bytes.Buffer

	SetDefault(mustLogger(testInfo(), Config{Writer: &outBuf}))
	Info(context.Background(), "installed_msg")

	if got := outBuf.String(); !strings.Contains(got, "installed_msg") {
		t.Errorf("Info(installed_msg) stdout = %q, want substring %q", got, "installed_msg")
	}
}

func TestLogger_OmitEmptyIdentity(t *testing.T) {
	var outBuf bytes.Buffer
	l := mustLogger(identity.Info{Name: "svc"}, Config{Writer: &outBuf})
	l.Info(context.Background(), "identity")

	text := outBuf.String()
	if !strings.Contains(text, "service.name=svc") {
		t.Errorf("l.Info(identity) output = %q, want substring %q", text, "service.name=svc")
	}
	for _, notWant := range []string{"service.version", "deployment.environment.name"} {
		if strings.Contains(text, notWant) {
			t.Errorf("l.Info(...) output = %q, want no substring %q", text, notWant)
		}
	}
}

func TestLogger_ConfiguredWriters(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	l := mustLogger(testInfo(), Config{Writer: &outBuf, ErrorWriter: &errBuf})

	l.Info(context.Background(), "info_msg")
	l.Warn(context.Background(), "warn_msg")
	l.Error(context.Background(), "error_msg")

	if got := outBuf.String(); !strings.Contains(got, "info_msg") {
		t.Errorf("Info(info_msg) Writer output = %q, want substring %q", got, "info_msg")
	}
	if got := outBuf.String(); strings.Contains(got, "warn_msg") {
		t.Errorf("Warn(warn_msg) Writer output = %q, want no substring %q", got, "warn_msg")
	}
	for _, want := range []string{"warn_msg", "error_msg"} {
		if got := errBuf.String(); !strings.Contains(got, want) {
			t.Errorf("Warn/Error(...) ErrorWriter output = %q, want substring %q", got, want)
		}
	}
}

func TestLogger_TraceCorrelation(t *testing.T) {
	t.Run("records with a span context carry trace_id and span_id", func(t *testing.T) {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Format: FormatJSON, Writer: &outBuf, ErrorWriter: io.Discard})

		provider := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
		ctx, span := provider.Tracer("test").Start(context.Background(), "op")
		defer span.End()

		l.Info(ctx, "correlated")

		var record map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(outBuf.Bytes()), &record); err != nil {
			t.Fatalf("json.Unmarshal(record) error = %v, want nil", err)
		}
		if wantTrace := span.SpanContext().TraceID().String(); record["trace_id"] != wantTrace {
			t.Errorf("Info(ctx) record[trace_id] = %v, want %q", record["trace_id"], wantTrace)
		}
		if wantSpan := span.SpanContext().SpanID().String(); record["span_id"] != wantSpan {
			t.Errorf("Info(ctx) record[span_id] = %v, want %q", record["span_id"], wantSpan)
		}
	})

	t.Run("records without a span context omit correlation keys", func(t *testing.T) {
		var outBuf bytes.Buffer
		l := mustLogger(testInfo(), Config{Format: FormatJSON, Writer: &outBuf, ErrorWriter: io.Discard})

		l.Info(context.Background(), "uncorrelated")

		var record map[string]any
		if err := json.Unmarshal(bytes.TrimSpace(outBuf.Bytes()), &record); err != nil {
			t.Fatalf("json.Unmarshal(record) error = %v, want nil", err)
		}
		if _, ok := record["trace_id"]; ok {
			t.Errorf("Info(uncorrelated) record has trace_id = %v, want no trace_id key", record["trace_id"])
		}
		if _, ok := record["span_id"]; ok {
			t.Errorf("Info(uncorrelated) record has span_id = %v, want no span_id key", record["span_id"])
		}
	})
}

func TestLogger_ExplicitCorrelationWins(t *testing.T) {
	var outBuf bytes.Buffer
	l := mustLogger(testInfo(), Config{Writer: &outBuf, ErrorWriter: io.Discard})

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	l.Info(ctx, "explicit", "trace_id", "manual-trace", "span_id", "manual-span")

	text := outBuf.String()
	if got := strings.Count(text, "trace_id="); got != 1 {
		t.Errorf("Info(explicit, trace_id=manual-trace) count(trace_id=) in %q = %d, want 1", text, got)
	}
	if got := strings.Count(text, "span_id="); got != 1 {
		t.Errorf("Info(explicit, span_id=manual-span) count(span_id=) in %q = %d, want 1", text, got)
	}
	for _, want := range []string{"trace_id=manual-trace", "span_id=manual-span"} {
		if !strings.Contains(text, want) {
			t.Errorf("l.Info(...) output = %q, want substring %q", text, want)
		}
	}
}

func TestLogger_Concurrent(t *testing.T) {
	l := mustLogger(testInfo(), Config{Writer: io.Discard, ErrorWriter: io.Discard})

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			l.Info(context.Background(), "info", "user_id", "u1")
			l.Error(context.Background(), "error", "order_id", "o1")
		})
	}
	wg.Wait()
}

func TestFacade_SetDefaultNilResets(t *testing.T) {
	prev := Default()
	t.Cleanup(func() { SetDefault(prev) })

	// SetDefault(nil) binds the default logger to os.Stderr at call time, so
	// point os.Stderr at a file for the duration of the call to capture output.
	errFile, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	if err != nil {
		t.Fatalf("os.Create(stderr) error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = errFile.Close() })
	func() {
		prevStderr := os.Stderr
		os.Stderr = errFile
		defer func() { os.Stderr = prevStderr }()
		SetDefault(nil)
	}()

	if got := Default(); got == nil {
		t.Errorf("SetDefault(nil) then Default() = nil, want non-nil")
	}

	Debug(context.Background(), "after_nil")

	got, err := os.ReadFile(errFile.Name())
	if err != nil {
		t.Fatalf("os.ReadFile(stderr) error = %v, want nil", err)
	}
	if !strings.Contains(string(got), "after_nil") {
		t.Errorf("Debug(after_nil) stderr = %q, want substring %q", got, "after_nil")
	}
}

func TestLogger_GlobalCorrelationWins(t *testing.T) {
	var outBuf bytes.Buffer
	l := mustLogger(testInfo(), Config{
		Format:      FormatJSON,
		GlobalKV:    map[string]any{"trace_id": "fixed-trace"},
		Writer:      &outBuf,
		ErrorWriter: io.Discard,
	})

	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	l.Info(ctx, "global_correlation")

	if got := strings.Count(outBuf.String(), `"trace_id"`); got != 1 {
		t.Errorf("Info(global_correlation) count(\"trace_id\") in %q = %d, want 1", outBuf.String(), got)
	}

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(outBuf.Bytes()), &record); err != nil {
		t.Fatalf("json.Unmarshal(record) error = %v, want nil", err)
	}
	if record["trace_id"] != "fixed-trace" {
		t.Errorf("Info(global_correlation) record[trace_id] = %v, want %q", record["trace_id"], "fixed-trace")
	}
	if wantSpan := span.SpanContext().SpanID().String(); record["span_id"] != wantSpan {
		t.Errorf("Info(ctx) record[span_id] = %v, want %q", record["span_id"], wantSpan)
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	_, err := New(testInfo(), Config{Format: "xml"})
	if !errors.Is(err, ErrInvalidLogFormat) {
		t.Errorf("New(Format: xml) error = %v, want %v", err, ErrInvalidLogFormat)
	}

	_, err = New(testInfo(), Config{Level: "verbose"})
	if !errors.Is(err, ErrInvalidLogLevel) {
		t.Errorf("New(Level: verbose) error = %v, want %v", err, ErrInvalidLogLevel)
	}
}

func BenchmarkLogger_Info(b *testing.B) {
	logger := mustLogger(testInfo(), Config{Writer: io.Discard, ErrorWriter: io.Discard})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		logger.Info(ctx, "benchmark", "user_id", "u1", "order_id", "o-1")
	}
}

func TestParseLevel_Invalid(t *testing.T) {
	if _, err := parseLevel("verbose"); err == nil {
		t.Errorf("parseLevel(%q) error = nil, want non-nil", "verbose")
	}
}
