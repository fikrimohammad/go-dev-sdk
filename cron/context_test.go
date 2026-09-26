package cron

import (
	"context"
	"testing"
)

func TestFencingTokenContext(t *testing.T) {
	ctx := context.Background()
	if _, ok := FencingTokenFromContext(ctx); ok {
		t.Fatal("expected no fencing token on empty context")
	}

	var nilCtx context.Context
	if _, ok := FencingTokenFromContext(nilCtx); ok {
		t.Fatal("expected no fencing token on nil context")
	}

	ctx = WithFencingToken(ctx, 42)
	token, ok := FencingTokenFromContext(ctx)
	if !ok || token != 42 {
		t.Fatalf("got token %d, ok %v; want 42, true", token, ok)
	}
}
