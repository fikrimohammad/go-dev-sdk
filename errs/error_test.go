package errs

import (
	"errors"
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
					t.Errorf("Error() = %q, should contain %q", got, s)
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
		t.Error("Meta() should return a copy, not the internal map")
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
		expected bool
	}{
		{
			name:     "same code matches",
			err:      &Error{code: 4004},
			target:   &Error{code: 4004},
			expected: true,
		},
		{
			name:     "different code does not match",
			err:      &Error{code: 4004},
			target:   &Error{code: 5001},
			expected: false,
		},
		{
			name:     "non-Error target does not match",
			err:      &Error{code: 4004},
			target:   errors.New("not found"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Is(tt.target); got != tt.expected {
				t.Errorf("Is() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestError_WithMeta_Immutable(t *testing.T) {
	original := New(4004, "not found")
	second := original.WithMeta("job_id", int64(123))
	third := second.WithMeta("bucket", "reports")

	if original.meta != nil {
		t.Error("WithMeta should not modify original error")
	}

	if second.meta["job_id"] != int64(123) {
		t.Error("second should have job_id")
	}
	if _, ok := second.meta["bucket"]; ok {
		t.Error("second should not have bucket")
	}

	if third.meta["job_id"] != int64(123) {
		t.Error("third should have job_id")
	}
	if third.meta["bucket"] != "reports" {
		t.Error("third should have bucket")
	}
}

func TestError_WithDebug_Immutable(t *testing.T) {
	original := New(5001, "error")
	modified := original.WithDebug("detail")

	if original.Debug() != "" {
		t.Error("WithDebug should not modify original error")
	}
	if modified.Debug() != "detail" {
		t.Errorf("modified.Debug() = %q, want %q", modified.Debug(), "detail")
	}
}

func TestStackCapture(t *testing.T) {
	err := New(5001, "error")

	if len(err.stack) == 0 {
		t.Fatal("stack should not be empty")
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
		t.Fatal("StackFrames() should not be empty")
	}

	for i, frame := range frames {
		if !strings.Contains(frame, "\n    ") {
			t.Errorf("StackFrames()[%d] = %q, expected format 'function\\n    file:line'", i, frame)
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

	var target *Error
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should find *Error in errors.Join chain")
	}

	if target.Code() != 4004 {
		t.Errorf("Code() = %v, want %v", target.Code(), 4004)
	}
}

func TestErrorsIs_CodeMatching(t *testing.T) {
	err := New(4004, "job not found")

	if !errors.Is(err, &Error{code: 4004}) {
		t.Error("errors.Is should match by code")
	}

	if errors.Is(err, &Error{code: 5001}) {
		t.Error("errors.Is should not match different codes")
	}
}

func TestErrorChain_Unwrap(t *testing.T) {
	inner := New(4004, "inner")
	outer := Wrap(5001, "outer", inner)

	if !errors.Is(outer, &Error{code: 4004}) {
		t.Error("errors.Is should traverse unwrap chain")
	}
}

func TestChainOfErrors(t *testing.T) {
	s3Err := errors.New("access denied")
	repoErr := Wrap(5001, "upload report file", s3Err).
		WithMeta("bucket", "reports").
		WithMeta("key", "report.csv")
	usecaseErr := Wrap(5001, "process export report", repoErr).
		WithMeta("job_id", int64(123))

	var target *Error
	if !errors.As(usecaseErr, &target) {
		t.Fatal("errors.As should find *Error in chain")
	}

	if target.Code() != 5001 {
		t.Errorf("Code() = %v, want %v", target.Code(), 5001)
	}

	if target.Meta()["job_id"] != int64(123) {
		t.Errorf("Meta()[job_id] = %v, want %v", target.Meta()["job_id"], int64(123))
	}

	var inner *Error
	if errors.As(target.Unwrap(), &inner) {
		if inner.Meta()["bucket"] != "reports" {
			t.Errorf("inner Meta()[bucket] = %v, want %q", inner.Meta()["bucket"], "reports")
		}
	}
}
