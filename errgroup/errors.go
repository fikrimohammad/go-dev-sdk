package errgroup

import "fmt"

// PanicError is returned by Wait when a task panics. Value is the value passed to
// panic, and Stack is the goroutine stack at the point of the panic.
type PanicError struct {
	// Value is the value passed to panic.
	Value any

	// Stack is the goroutine stack captured at the point of the panic.
	Stack []byte
}

// Error reports the panic value.
func (e *PanicError) Error() string {
	return "errgroup: task panicked: " + fmt.Sprint(e.Value)
}

// Unwrap returns Value when it is an error, so errors.Is and errors.As reach it; otherwise nil.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
