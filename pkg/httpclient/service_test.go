package httpclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"goharness/pkg/httpclient"
)

func TestServiceUsesSharedPoolAndAppliesPerServiceAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/query" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Api-Key") != "secret" {
			t.Errorf("Api-Key = %q", request.Header.Get("Api-Key"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	pool := httpclient.NewPool(httpclient.TransportConfig{})
	service, err := httpclient.NewService(httpclient.ServiceConfig{
		BaseURL:      server.URL + "/api",
		APIKey:       "secret",
		APIKeyHeader: "Api-Key",
		Pool:         pool,
		Timeout:      5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := httpclient.NewService(httpclient.ServiceConfig{
		BaseURL: server.URL,
		Pool:    pool,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if service.HTTPClient() != peer.HTTPClient() {
		t.Fatal("services with the same pool and timeout should share a client")
	}

	var response struct {
		OK bool `json:"ok"`
	}
	if err := service.DoJSON(context.Background(), http.MethodPost, "query", map[string]any{"q": "go"}, &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK {
		t.Fatalf("response = %#v", response)
	}
}

func TestServiceDefaultsToBoundedTimeout(t *testing.T) {
	service, err := httpclient.NewService(httpclient.ServiceConfig{BaseURL: "https://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if service.HTTPClient().Timeout != httpclient.DefaultServiceTimeout {
		t.Fatalf("timeout = %s", service.HTTPClient().Timeout)
	}
}
