// Package client defines the shared contract every config backend
// (etcd, infisical, ...) implements. Keeping it separate from the top-level
// confloader package breaks the import cycle that would otherwise form
// between the loaders and the concrete clients.
package client

import (
	"context"
	"errors"
)

// ErrNotFound is returned by Fetch when the requested key is absent in the
// folder (and, after fallback, in the default folder). It is a definitive
// answer and is never retried by the loader.
var ErrNotFound = errors.New("confloader: config not found in folder and in default folder")

// DefaultFolder is the standardized fallback folder. When a key is missing in
// the requested folder, the loader falls back to this folder inside the same
// namespace.
const DefaultFolder = "default"

// Fetched is the result of a single client fetch for a (folder, key) pair.
// Value is the raw string; Revision is an opaque, comparable identifier that
// lets the loader detect changes without re-parsing (etcd mod revision,
// infisical ETag, ...). An empty Value means "not present".
type Fetched struct {
	Value    string
	Revision string
}

// Client abstracts a backend that stores configs/secrets under the
// standardized model: namespace -> folder (no subfolders) -> key.
//
// The loader always uses the Fetch + polling path. Even when a backend is
// capable of push notifications, polling is the canonical refresh strategy so
// staleness behaviour is uniform across providers.
type Client interface {
	// Fetch retrieves the raw value for the given folder/key.
	// It must return (Fetched{}, ErrNotFound) when the key is absent in the
	// folder — never nil-with-no-error for a missing key.
	Fetch(ctx context.Context, folder, key string) (Fetched, error)

	// Close releases any resources held by the client.
	Close() error
}
