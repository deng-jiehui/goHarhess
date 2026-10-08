package httpclient_test

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

func TestPoolSharesTransportAcrossClientTimeouts(t *testing.T) {
	pool := httpclient.NewPool(httpclient.TransportConfig{})
	short := pool.Client(time.Second)
	shortAgain := pool.Client(time.Second)
	long := pool.Client(10 * time.Second)

	if short != shortAgain {
		t.Fatal("same timeout should return the same client")
	}
	if short == long {
		t.Fatal("different timeouts should return distinct clients")
	}
	if short.Transport != long.Transport || short.Transport != pool.Transport() {
		t.Fatal("clients should share one transport and its connection pools")
	}
}

func TestPoolReusesHTTPConnection(t *testing.T) {
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
	client := pool.Client(time.Second)
	for range 2 {
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1", got)
	}
}
