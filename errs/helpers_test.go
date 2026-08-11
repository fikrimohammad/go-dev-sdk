package errs

import (
	"errors"
	"testing"
)

func TestCodeFromError_Nil(t *testing.T) {
	if got := CodeFromError(nil); got != OK {
		t.Errorf("CodeFromError(nil) = %v, want %v", got, OK)
	}
}

func TestCodeFromError_WithError(t *testing.T) {
	err := New(NotFound, "not found")

	if got := CodeFromError(err); got != NotFound {
		t.Errorf("CodeFromError() = %v, want %v", got, NotFound)
	}
}

func TestCodeFromError_PlainError(t *testing.T) {
	err := errors.New("plain error")
	if got := CodeFromError(err); got != Internal {
		t.Errorf("CodeFromError() = %v, want %v", got, Internal)
	}
}

func TestAs_Found(t *testing.T) {
	original := New(NotFound, "not found")

	var target *Error
	if !As(original, &target) {
		t.Fatal("As should return true for *Error")
	}
	if target.Code() != NotFound {
		t.Errorf("Code() = %v, want %v", target.Code(), NotFound)
	}
}

func TestAs_Wrapped(t *testing.T) {
	original := New(NotFound, "not found")
	wrapped := errors.Join(original, nil)

	var target *Error
	if !As(wrapped, &target) {
		t.Fatal("As should find *Error in wrapped chain")
	}
	if target.Code() != NotFound {
		t.Errorf("Code() = %v, want %v", target.Code(), NotFound)
	}
}

func TestAs_PlainError(t *testing.T) {
	err := errors.New("plain error")

	var target *Error
	if As(err, &target) {
		t.Error("As should return false for plain error")
	}
}

func TestAs_Nil(t *testing.T) {
	var target *Error
	if As(nil, &target) {
		t.Error("As should return false for nil error")
	}
}
