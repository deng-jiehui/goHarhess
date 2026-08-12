package tool_test

import (
	"context"
	"testing"
	"time"

	"goharness/pkg/httpclient"
	"goharness/pkg/tool"
)

func TestNewHTTPServiceUsesToolContextPoolAndTimeout(t *testing.T) {
	pool := httpclient.NewPool(httpclient.TransportConfig{})
	ctx := tool.WithHTTPOptions(context.Background(), tool.HTTPOptions{
		Pool:    pool,
		Timeout: 7 * time.Second,
	})
	service, err := tool.NewHTTPService(ctx, httpclient.ServiceConfig{BaseURL: "https://api.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if service.HTTPClient().Transport != pool.Transport() {
		t.Fatal("tool service did not use the context pool")
	}
	if service.HTTPClient().Timeout != 7*time.Second {
		t.Fatalf("timeout = %s", service.HTTPClient().Timeout)
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
