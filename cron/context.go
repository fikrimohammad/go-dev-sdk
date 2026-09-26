package cron

import "context"

type fencingTokenKey struct{}

// WithFencingToken attaches a fencing token to the context.
func WithFencingToken(ctx context.Context, token int64) context.Context {
	return context.WithValue(ctx, fencingTokenKey{}, token)
}

// FencingTokenFromContext extracts the fencing token from the context.
func FencingTokenFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	token, ok := ctx.Value(fencingTokenKey{}).(int64)
	return token, ok
}
