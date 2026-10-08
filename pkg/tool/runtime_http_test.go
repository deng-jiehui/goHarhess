package tool

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/httpclient"
)

func TestRuntimeResolvesPooledHTTPClientFromToolContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	var dials atomic.Int32
	dialer := &net.Dialer{}
	pool := httpclient.NewPool(httpclient.TransportConfig{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return dialer.DialContext(ctx, network, address)
		},
	})
	ctx := WithHTTPOptions(context.Background(), HTTPOptions{Pool: pool, Timeout: time.Second})
	runtime := mustRuntime(t, nil)
	value := NewFunctionTool(Spec{Name: "http", IsReadOnly: true}, func(ctx context.Context, input CallInput) (Result, error) {
		client := HTTPClient(ctx)
		if client == nil {
			t.Fatal("HTTPClient(ctx) returned nil inside a managed call")
		}
		response, err := client.Get(server.URL)
		if err != nil {
			return Result{}, err
		}
		defer response.Body.Close()
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			return Result{}, err
		}
		return TextResult("ok"), nil
	})
	binding := registerTool(t, runtime, value)
	scope := runtime.NewScope(1)
	for range 2 {
		if _, err := scope.Execute(ctx, binding, CallInput{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want one reused connection", got)
	}
}
