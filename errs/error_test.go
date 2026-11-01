package errs

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *Error
		contains []string
	}{
		{
			name:     "message only",
			err:      &Error{code: 4004, message: "job not found"},
			contains: []string{"job not found"},
		},
		{
			name:     "message with cause",
			err:      &Error{code: 5001, message: "upload failed", cause: errors.New("timeout")},
			contains: []string{"upload failed", "timeout"},
		},
		{
			name:     "message with debug in meta",
			err:      &Error{code: 4004, message: "not found", meta: map[string]any{"debug": "query returned 0 rows"}},
			contains: []string{"not found"},
		},
		{
			name:     "message with meta",
			err:      &Error{code: 4004, message: "not found", meta: map[string]any{"job_id": int64(123)}},
			contains: []string{"not found"},
		},
		{
			name: "nested Error cause",
			err: &Error{
				code:    5001,
				message: "outer",
				cause:   &Error{code: 4004, message: "inner"},
			},
			contains: []string{"outer", "inner"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			for _, s := range tt.contains {
				if !strings.Contains(got, s) {
					t.Errorf("Error() = %q, want to contain %q", got, s)
				}
			}
		})
	}
}

func TestError_Accessors(t *testing.T) {
	err := &Error{
		code:    4004,
		message: "not found",
		meta:    map[string]any{"job_id": int64(123), "debug": "detail"},
	}

	if err.Code() != 4004 {
		t.Errorf("Code() = %v, want %v", err.Code(), 4004)
	}
	if err.Message() != "not found" {
		t.Errorf("Message() = %q, want %q", err.Message(), "not found")
	}
	if err.Debug() != "detail" {
		t.Errorf("Debug() = %q, want %q", err.Debug(), "detail")
	}
	if err.Cause() != nil {
		t.Errorf("Cause() = %v, want nil", err.Cause())
	}
}

func TestError_Meta_ReturnsCopy(t *testing.T) {
	err := &Error{code: 4004, message: "not found", meta: map[string]any{"key": "value"}}

	meta := err.Meta()
	meta["key"] = "mutated"

	if err.meta["key"] != "value" {
		t.Errorf("Meta() internal meta[\"key\"] = %v, want %q", err.meta["key"], "value")
	}
}

func TestError_Meta_Nil(t *testing.T) {
	err := &Error{code: 4004, message: "not found"}
	if err.Meta() != nil {
		t.Errorf("Meta() = %v, want nil", err.Meta())
	}
}

func TestError_Debug_Empty(t *testing.T) {
	err := &Error{code: 4004, message: "not found"}
	if err.Debug() != "" {
		t.Errorf("Debug() = %q, want empty", err.Debug())
	}
}

func TestError_Unwrap(t *testing.T) {
	cause := errors.New("original error")
	err := Wrap(5001, "wrapped", cause)

	if err.Unwrap() != cause {
		t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), cause)
	}

	errWithoutCause := New(4004, "not found")
	if errWithoutCause.Unwrap() != nil {
		t.Errorf("Unwrap() = %v, want nil", errWithoutCause.Unwrap())
	}
}

func TestError_Is(t *testing.T) {
	tests := []struct {
		name     string
		err      *Error
		target   error
		matching bool
	}{
		{
			name:     "same code matches",
			err:      &Error{code: 4004},
			target:   &Error{code: 4004},
			matching: true,
		},
		{
			name:     "different code does not match",
			err:      &Error{code: 4004},
			target:   &Error{code: 5001},
			matching: false,
		},
		{
			name:     "non-Error target does not match",
			err:      &Error{code: 4004},
			target:   errors.New("not found"),
			matching: false,
		},
		{
			name:     "two code 0 errors do not match",
			err:      AsError(io.EOF),
			target:   AsError(errors.New("other")),
			matching: false,
		},
		{
			name:     "code 0 does not match New(0)",
			err:      AsError(io.EOF),
			target:   New(0, "unknown"),
			matching: false,
		},
		{
			name:     "same non-zero code matches",
			err:      New(4004, "not found"),
			target:   New(4004, "other message"),
			matching: true,
		},
		{
			name:     "different non-zero codes do not match",
			err:      New(4004, "not found"),
			target:   New(5001, "not found"),
			matching: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Is(tt.target); got != tt.matching {
				t.Errorf("Is() = %v, want %v", got, tt.matching)
			}
		})
	}
}

func TestError_WithMeta_Immutable(t *testing.T) {
	original := New(4004, "not found")
	second := original.WithMeta("job_id", int64(123))
	third := second.WithMeta("bucket", "reports")

	if original.meta != nil {
		t.Errorf("WithMeta() modified original: meta = %v, want nil", original.meta)
	}

	if second.meta["job_id"] != int64(123) {
		t.Errorf("WithMeta() second job_id = %v, want %v", second.meta["job_id"], int64(123))
	}
	if _, ok := second.meta["bucket"]; ok {
		t.Errorf("WithMeta() second bucket = %v, want absent", second.meta["bucket"])
	}

	if third.meta["job_id"] != int64(123) {
		t.Errorf("WithMeta() third job_id = %v, want %v", third.meta["job_id"], int64(123))
	}
	if third.meta["bucket"] != "reports" {
		t.Errorf("WithMeta() third bucket = %v, want %q", third.meta["bucket"], "reports")
	}
}

func TestError_WithDebug_Immutable(t *testing.T) {
	original := New(5001, "error")
	modified := original.WithDebug("detail")

	if original.Debug() != "" {
		t.Errorf("WithDebug() modified original: Debug() = %q, want empty", original.Debug())
	}
	if modified.Debug() != "detail" {
		t.Errorf("modified.Debug() = %q, want %q", modified.Debug(), "detail")
	}
}

func TestStackCapture(t *testing.T) {
	err := New(5001, "error")

	if len(err.stack) == 0 {
		t.Fatalf("New() stack len = %d, want > 0", len(err.stack))
	}

	for i, pc := range err.stack {
		if pc == 0 {
			t.Errorf("stack[%d] = 0, want non-zero PC", i)
		}
	}
}

func TestStackFrames(t *testing.T) {
	err := New(5001, "error")
	frames := err.StackFrames()

	if len(frames) == 0 {
		t.Fatalf("StackFrames() len = %d, want > 0", len(frames))
	}

	for i, frame := range frames {
		if !strings.Contains(frame, "\n    ") {
			t.Errorf("StackFrames()[%d] = %q, want format 'function\\n    file:line'", i, frame)
		}
	}
}

func TestStackFrames_Empty(t *testing.T) {
	err := &Error{code: 5001, message: "error"}
	if frames := err.StackFrames(); frames != nil {
		t.Errorf("StackFrames() = %v, want nil for empty stack", frames)
	}
}

func TestFmtErrorf_Wrapping(t *testing.T) {
	original := New(4004, "not found")
	wrapped := errors.Join(original, nil)

	target, ok := errors.AsType[*Error](wrapped)
	if !ok {
		t.Fatalf("errors.AsType[*Error](%v) = false, want true", wrapped)
	}

	if target.Code() != 4004 {
		t.Errorf("Code() = %v, want %v", target.Code(), 4004)
	}
}

func TestErrorsIs_CodeMatching(t *testing.T) {
	err := New(4004, "job not found")
	wrapped := fmt.Errorf("handler: %w", err)

	if !errors.Is(err, &Error{code: 4004}) {
		t.Errorf("errors.Is(%v, code 4004) = false, want true", err)
	}
	if errors.Is(err, &Error{code: 5001}) {
		t.Errorf("errors.Is(%v, code 5001) = true, want false", err)
	}

	if !errors.Is(wrapped, &Error{code: 4004}) {
		t.Errorf("errors.Is(%v, code 4004) = false, want true", wrapped)
	}
	if errors.Is(wrapped, &Error{code: 5001}) {
		t.Errorf("errors.Is(%v, code 5001) = true, want false", wrapped)
	}

	// Code 0 escape hatches: pointer identity and the Unwrap chain.
	e0 := New(0, "x")
	if !errors.Is(e0, e0) {
		t.Errorf("errors.Is(%v, itself) = false, want true", e0)
	}
	if !errors.Is(AsError(io.EOF), io.EOF) {
		t.Error("errors.Is(AsError(io.EOF), io.EOF) = false, want true")
	}
}

func TestErrorChain_Unwrap(t *testing.T) {
	inner := New(4004, "inner")
	outer := Wrap(5001, "outer", inner)

	if !errors.Is(outer, &Error{code: 4004}) {
		t.Errorf("errors.Is(%v, code 4004) = false, want true", outer)
	}
}

func TestChainOfErrors(t *testing.T) {
	s3Err := errors.New("access denied")
	repoErr := Wrap(5001, "upload report file", s3Err).
		WithMeta("bucket", "reports").
		WithMeta("key", "report.csv")
	usecaseErr := Wrap(5001, "process export report", repoErr).
		WithMeta("job_id", int64(123))

	target, ok := errors.AsType[*Error](usecaseErr)
	if !ok {
		t.Fatalf("errors.AsType[*Error](%v) = false, want true", usecaseErr)
	}

	if target.Code() != 5001 {
		t.Errorf("Code() = %v, want %v", target.Code(), 5001)
	}

	if target.Meta()["job_id"] != int64(123) {
		t.Errorf("Meta()[job_id] = %v, want %v", target.Meta()["job_id"], int64(123))
	}

	if inner, ok := errors.AsType[*Error](target.Unwrap()); ok {
		if inner.Meta()["bucket"] != "reports" {
			t.Errorf("inner Meta()[bucket] = %v, want %q", inner.Meta()["bucket"], "reports")
		}
	}
}
