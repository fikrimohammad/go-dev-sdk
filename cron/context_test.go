package cron

import (
	"context"
	"testing"
)

func TestFencingTokenContext(t *testing.T) {
	ctx := context.Background()
	if _, ok := FencingTokenFromContext(ctx); ok {
		t.Fatal("FencingTokenFromContext(empty) ok = true, want false")
	}

	var nilCtx context.Context
	if _, ok := FencingTokenFromContext(nilCtx); ok {
		t.Fatal("FencingTokenFromContext(nil) ok = true, want false")
	}

	ctx = WithFencingToken(ctx, "token-uuid-12345")
	token, ok := FencingTokenFromContext(ctx)
	if !ok || token != "token-uuid-12345" {
		t.Fatalf("got token %q, ok %v; want \"token-uuid-12345\", true", token, ok)
	}
}
