package tool_test

import (
	"context"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/httpclient"
	"github.com/goharness/goharness/pkg/tool"
)

func TestHTTPClientUsesToolContextPoolAndTimeout(t *testing.T) {
	pool := httpclient.NewPool(httpclient.TransportConfig{})
	ctx := tool.WithHTTPOptions(context.Background(), tool.HTTPOptions{
		Pool:    pool,
		Timeout: 7 * time.Second,
	})
	client := tool.HTTPClient(ctx)
	if client.Transport != pool.Transport() {
		t.Fatal("tool client did not use the context pool")
	}
	if client.Timeout != 7*time.Second {
		t.Fatalf("timeout = %s", client.Timeout)
	}
}

func TestHTTPClientFallsBackToSharedPool(t *testing.T) {
	client := tool.HTTPClient(context.Background())
	if client.Transport != httpclient.DefaultPool.Transport() {
		t.Fatal("tool HTTP client did not use the default pool")
	}
	if client.Timeout != tool.DefaultHTTPTimeout {
		t.Fatalf("timeout = %s", client.Timeout)
	}
}
