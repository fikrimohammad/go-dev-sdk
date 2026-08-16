package errs

import (
	"errors"
	"testing"
)

func TestNew(t *testing.T) {
	err := New(4004, "job not found")

	if err.Code() != 4004 {
		t.Errorf("Code() = %v, want %v", err.Code(), 4004)
	}
	if err.Message() != "job not found" {
		t.Errorf("Message() = %q, want %q", err.Message(), "job not found")
	}
	if err.Cause() != nil {
		t.Errorf("Cause() = %v, want nil", err.Cause())
	}
	if len(err.stack) == 0 {
		t.Error("stack should be captured")
	}
}

func TestWrap(t *testing.T) {
	cause := errors.New("connection refused")
	err := Wrap(5001, "upload failed", cause)

	if err.Code() != 5001 {
		t.Errorf("Code() = %v, want %v", err.Code(), 5001)
	}
	if err.Message() != "upload failed" {
		t.Errorf("Message() = %q, want %q", err.Message(), "upload failed")
	}
	if err.Cause() != cause {
		t.Errorf("Cause() = %v, want %v", err.Cause(), cause)
	}
	if len(err.stack) == 0 {
		t.Error("stack should be captured")
	}
}

func TestAsError_Nil(t *testing.T) {
	if got := AsError(nil); got != nil {
		t.Errorf("AsError(nil) = %v, want nil", got)
	}
}

func TestAsError_WithError(t *testing.T) {
	original := New(4004, "not found")

	got := AsError(original)
	if got != original {
		t.Error("AsError should return the same *Error from chain")
	}
}

func TestAsError_PlainError(t *testing.T) {
	plain := errors.New("some error")
	got := AsError(plain)

	if got.Code() != 0 {
		t.Errorf("Code() = %v, want %v", got.Code(), 0)
	}
	if got.Message() != "some error" {
		t.Errorf("Message() = %q, want %q", got.Message(), "some error")
	}
	if got.Cause() != plain {
		t.Error("Cause should be the original error")
	}
}
