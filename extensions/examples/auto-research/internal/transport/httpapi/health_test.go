package httpapi_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/goharness/autoresearch/internal/infra/sqlite"
	"github.com/goharness/autoresearch/internal/transport/httpapi"
)

func TestReadinessReflectsDatabaseAvailability(t *testing.T) {
	store, err := sqlite.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := httpapi.NewHealthHandler(store)
	request := func(path string, want int) {
		t.Helper()
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest("GET", path, nil))
		if rr.Code != want {
			t.Fatalf("%s = %d, want %d", path, rr.Code, want)
		}
	}
	request("/readyz", 200)
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	request("/readyz", 503)
	request("/healthz", 200)
}
