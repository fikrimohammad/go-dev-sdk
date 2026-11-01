package errs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"testing"
)

func TestLogValue_BasicFields(t *testing.T) {
	err := &Error{code: 4004, message: "job not found"}
	val := err.LogValue()

	group := val.Group()
	m := slogGroupToMap(group)

	if m["code"].Int64() != 4004 {
		t.Errorf("code = %v, want %v", m["code"].Int64(), 4004)
	}
	if m["message"].String() != "job not found" {
		t.Errorf("message = %q, want %q", m["message"].String(), "job not found")
	}
	if _, ok := m["debug"]; ok {
		t.Error("debug should not be present")
	}
	if _, ok := m["meta"]; ok {
		t.Error("meta should not be present")
	}
	if _, ok := m["cause"]; ok {
		t.Error("cause should not be present")
	}
	if _, ok := m["stack"]; ok {
		t.Error("stack should not be present")
	}
}

func TestLogValue_AllFields(t *testing.T) {
	err := New(4004, "not found").
		WithMeta("job_id", float64(123)).
		WithDebug("query returned 0 rows")

	val := err.LogValue()
	group := val.Group()
	m := slogGroupToMap(group)

	if m["code"].Int64() != 4004 {
		t.Errorf("code = %v, want %v", m["code"].Int64(), 4004)
	}
	if m["message"].String() != "not found" {
		t.Errorf("message = %q, want %q", m["message"].String(), "not found")
	}
	if m["debug"].String() != "query returned 0 rows" {
		t.Errorf("debug = %q, want %q", m["debug"].String(), "query returned 0 rows")
	}
	if _, ok := m["meta"]; !ok {
		t.Error("meta should be present")
	}
	if _, ok := m["stack"]; !ok {
		t.Error("stack should be present")
	}
}

func TestLogValue_WithCause(t *testing.T) {
	cause := errors.New("connection refused")
	err := Wrap(5001, "upload failed", cause)

	val := err.LogValue()
	group := val.Group()
	m := slogGroupToMap(group)

	if m["cause"].String() != "connection refused" {
		t.Errorf("cause = %q, want %q", m["cause"].String(), "connection refused")
	}
}

func TestLogValue_WithNestedCause(t *testing.T) {
	inner := New(4004, "inner")
	outer := Wrap(5001, "outer", inner)

	val := outer.LogValue()
	group := val.Group()
	m := slogGroupToMap(group)

	causeVal := m["cause"]
	if causeVal.Kind() != slog.KindGroup {
		t.Fatalf("cause should be a group, got kind %v", causeVal.Kind())
	}

	innerGroup := causeVal.Group()
	innerMap := slogGroupToMap(innerGroup)

	if innerMap["code"].Int64() != 4004 {
		t.Errorf("inner code = %v, want %v", innerMap["code"].Int64(), 4004)
	}
	if innerMap["message"].String() != "inner" {
		t.Errorf("inner message = %q, want %q", innerMap["message"].String(), "inner")
	}
}

func TestLogValue_EmptyOptionalFields(t *testing.T) {
	err := &Error{code: 4004, message: "x"}
	val := err.LogValue()

	group := val.Group()
	m := slogGroupToMap(group)

	if len(m) != 2 {
		t.Errorf("LogValue() fields = %d %v, want 2", len(m), fieldKeys(m))
	}
}

func TestSlogAttr_WithError(t *testing.T) {
	err := New(4004, "job not found").
		WithMeta("job_id", float64(123)).
		WithDebug("query returned 0 rows")

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Error("export failed", SlogAttr(err))

	var logEntry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to unmarshal log output: %v", err)
	}

	errGroup, ok := logEntry["err"].(map[string]any)
	if !ok {
		t.Fatalf("SlogAttr(err) output err = %v, want nested group", logEntry["err"])
	}
	if errGroup["code"] != float64(4004) {
		t.Errorf("err.code = %v, want %v", errGroup["code"], 4004)
	}
	if errGroup["message"] != "job not found" {
		t.Errorf("err.message = %v, want 'job not found'", errGroup["message"])
	}
	if errGroup["debug"] != "query returned 0 rows" {
		t.Errorf("err.debug = %v, want 'query returned 0 rows'", errGroup["debug"])
	}
}

func TestSlogAttr_PlainError(t *testing.T) {
	err := errors.New("plain error")

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Error("export failed", SlogAttr(err))

	var logEntry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to unmarshal log output: %v", err)
	}

	if logEntry["err"] != "plain error" {
		t.Errorf("err = %v, want 'plain error'", logEntry["err"])
	}
}

func slogGroupToMap(attrs []slog.Attr) map[string]slog.Value {
	m := make(map[string]slog.Value, len(attrs))
	for _, a := range attrs {
		m[a.Key] = a.Value
	}
	return m
}

func fieldKeys(m map[string]slog.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestError_LogValue(t *testing.T) {
	tests := []struct {
		name      string
		err       *Error
		wantDebug string
		wantMeta  map[string]any
	}{
		{
			name:      "debug and meta",
			err:       New(4004, "not found").WithDebug("d").WithMeta("k", "v"),
			wantDebug: "d",
			wantMeta:  map[string]any{"k": "v"},
		},
		{
			name:      "only debug omits meta",
			err:       New(4004, "not found").WithDebug("d"),
			wantDebug: "d",
			wantMeta:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))
			logger.Error("export failed", SlogAttr(tt.err))

			var logEntry map[string]any
			if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
				t.Fatalf("failed to unmarshal log output: %v", err)
			}
			errObj, ok := logEntry["err"].(map[string]any)
			if !ok {
				t.Fatalf("LogValue(%q) err = %v, want map[string]any", tt.name, logEntry["err"])
			}

			if got := errObj["debug"]; got != tt.wantDebug {
				t.Errorf("LogValue(%q) debug = %v, want %v", tt.name, got, tt.wantDebug)
			}
			got, _ := errObj["meta"].(map[string]any)
			if !maps.Equal(got, tt.wantMeta) {
				t.Errorf("LogValue(%q) meta = %v, want %v", tt.name, got, tt.wantMeta)
			}
		})
	}
}

func TestSlogAttr(t *testing.T) {
	err := fmt.Errorf("wrap: %w", New(42, "boom"))

	attr := SlogAttr(err)
	if attr.Key != "err" {
		t.Errorf("SlogAttr(%v).Key = %q, want %q", err, attr.Key, "err")
	}
	if attr.Value.Kind() != slog.KindGroup {
		t.Fatalf("SlogAttr(%v).Value.Kind() = %v, want %v", err, attr.Value.Kind(), slog.KindGroup)
	}
	code := slogGroupToMap(attr.Value.Group())["code"]
	if code.Int64() != 42 {
		t.Errorf("SlogAttr(%v) code = %d, want %d", err, code.Int64(), 42)
	}
}
