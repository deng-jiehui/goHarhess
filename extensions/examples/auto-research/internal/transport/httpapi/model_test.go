package httpapi_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/infra/sqlite"
	"github.com/goharness/autoresearch/internal/transport/httpapi"
)

func TestModelSettingsHTTPDoesNotReturnAPIKey(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	turns := app.NewTurnService(store, store, store)
	h := httpapi.NewHandlerWithRuntime(app.NewProjectService(store), app.NewSessionService(store), turns, store, "", slog.New(slog.NewTextHandler(io.Discard, nil)))

	call(t, h, http.MethodPut, "/api/runtime/model", map[string]any{
		"mode": "remote", "endpoint": "http://127.0.0.1:19090/v1", "model": "test-model", "api_key": "secret-key",
	}, http.StatusOK)
	response := call(t, h, http.MethodGet, "/api/runtime/model", nil, http.StatusOK)
	body := response.Body.String()
	if body == "" || contains(body, "secret-key") {
		t.Fatalf("模型状态泄漏 API Key: %s", body)
	}
}

func contains(value, needle string) bool {
	for index := 0; index+len(needle) <= len(value); index++ {
		if value[index:index+len(needle)] == needle {
			return true
		}
	}
	return false
}
