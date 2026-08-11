package errs

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

func TestLogValue_BasicFields(t *testing.T) {
	err := &Error{code: NotFound, message: "job not found"}
	val := err.LogValue()

	group := val.Group()
	m := slogGroupToMap(group)

	if m["code"].String() != "NOT_FOUND" {
		t.Errorf("code = %q, want %q", m["code"].String(), "NOT_FOUND")
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
	err := New(NotFound, "not found").
		WithMeta("job_id", float64(123)).
		WithDebug("query returned 0 rows")

	val := err.LogValue()
	group := val.Group()
	m := slogGroupToMap(group)

	if m["code"].String() != "NOT_FOUND" {
		t.Errorf("code = %q, want %q", m["code"].String(), "NOT_FOUND")
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
	err := Wrap(Internal, "upload failed", cause)

	val := err.LogValue()
	group := val.Group()
	m := slogGroupToMap(group)

	if m["cause"].String() != "connection refused" {
		t.Errorf("cause = %q, want %q", m["cause"].String(), "connection refused")
	}
}

func TestLogValue_WithNestedCause(t *testing.T) {
	inner := New(NotFound, "inner")
	outer := Wrap(Internal, "outer", inner)

	val := outer.LogValue()
	group := val.Group()
	m := slogGroupToMap(group)

	causeVal := m["cause"]
	if causeVal.Kind() != slog.KindGroup {
		t.Fatalf("cause should be a group, got kind %v", causeVal.Kind())
	}

	innerGroup := causeVal.Group()
	innerMap := slogGroupToMap(innerGroup)

	if innerMap["code"].String() != "NOT_FOUND" {
		t.Errorf("inner code = %q, want %q", innerMap["code"].String(), "NOT_FOUND")
	}
	if innerMap["message"].String() != "inner" {
		t.Errorf("inner message = %q, want %q", innerMap["message"].String(), "inner")
	}
}

func TestLogValue_EmptyOptionalFields(t *testing.T) {
	err := &Error{code: NotFound, message: "x"}
	val := err.LogValue()

	group := val.Group()
	m := slogGroupToMap(group)

	if len(m) != 2 {
		t.Errorf("expected 2 fields, got %d: %v", len(m), fieldKeys(m))
	}
}

func TestSlogAttr_WithError(t *testing.T) {
	err := New(NotFound, "job not found").
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
		t.Fatalf("expected nested group under 'err', got: %v", logEntry["err"])
	}
	if errGroup["code"] != "NOT_FOUND" {
		t.Errorf("err.code = %v, want NOT_FOUND", errGroup["code"])
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
