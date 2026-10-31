// Package widget is a clean fixture.
package widget

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// ErrNotFound reports a missing widget.
var ErrNotFound = errors.New("widget: not found")

// Widget is a widget.
type Widget struct {
	ID      string
	BaseURL string
}

// New returns a Widget.
func New(id string) *Widget { return &Widget{ID: id} }

// Name returns the name.
func (w *Widget) Name() string { return w.ID }

// Load loads a widget.
func (w *Widget) Load(ctx context.Context, path string) error {
	if _, err := os.ReadFile(path); err != nil {
		return fmt.Errorf("load widget %s: %w", w.ID, err)
	}
	if path == "" {
		return fmt.Errorf("%w: empty path", ErrNotFound)
	}
	var ids []string
	_ = ids
	return ctx.Err()
}

// gostyle:ignore getter-prefix mirrors the HTTP GET verb
func (w *Widget) GetObject() {}
