package cron

import "context"

type fencingTokenKey struct{}

// WithFencingToken attaches a fencing token to the context.
func WithFencingToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, fencingTokenKey{}, token)
}

// FencingTokenFromContext extracts the fencing token from the context.
func FencingTokenFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	token, ok := ctx.Value(fencingTokenKey{}).(string)
	return token, ok
}
