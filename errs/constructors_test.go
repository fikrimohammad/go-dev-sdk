package errs

import (
	"errors"
	"testing"
)

func TestNew(t *testing.T) {
	err := New(NotFound, "job not found")

	if err.Code() != NotFound {
		t.Errorf("Code() = %v, want %v", err.Code(), NotFound)
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
	err := Wrap(Internal, "upload failed", cause)

	if err.Code() != Internal {
		t.Errorf("Code() = %v, want %v", err.Code(), Internal)
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
	original := New(NotFound, "not found")

	got := AsError(original)
	if got != original {
		t.Error("AsError should return the same *Error from chain")
	}
}

func TestAsError_PlainError(t *testing.T) {
	plain := errors.New("some error")
	got := AsError(plain)

	if got.Code() != Internal {
		t.Errorf("Code() = %v, want %v", got.Code(), Internal)
	}
	if got.Message() != "some error" {
		t.Errorf("Message() = %q, want %q", got.Message(), "some error")
	}
	if got.Cause() != plain {
		t.Error("Cause should be the original error")
	}
}
