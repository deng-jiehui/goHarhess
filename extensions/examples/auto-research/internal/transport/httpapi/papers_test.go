package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/infra/sqlite"
	"github.com/goharness/autoresearch/internal/transport/httpapi"
)

func TestPaperHTTPImportListAndUpdateNote(t *testing.T) {
	store, err := sqlite.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := NewPaperHandlerForTest(store)
	project := postJSON(t, h, "/api/projects", map[string]any{"title": "论文项目"})
	var created struct {
		ID string `json:"id"`
	}
	decodeJSON(t, project, &created)
	response := postJSON(t, h, "/api/projects/"+created.ID+"/papers", map[string]any{
		"title": "一篇论文", "doi": "https://doi.org/10.1234/ABC", "abstract": "摘要",
	})
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("导入论文状态=%d body=%s", response.StatusCode, body)
	}
	var paper struct {
		ID        string `json:"id"`
		SourceKey string `json:"source_key"`
		Revision  int64  `json:"revision"`
	}
	decodeJSON(t, response, &paper)
	if paper.SourceKey != "doi:10.1234/abc" {
		t.Fatalf("source_key=%q", paper.SourceKey)
	}
	list := get(t, h, "/api/projects/"+created.ID+"/papers")
	var items struct {
		Items []any `json:"items"`
	}
	decodeJSON(t, list, &items)
	if len(items.Items) != 1 {
		t.Fatalf("论文数量=%d", len(items.Items))
	}
	patch := patchJSON(t, h, "/api/projects/"+created.ID+"/papers/"+paper.ID, map[string]any{"note": "阅读笔记", "revision": paper.Revision})
	if patch.StatusCode != http.StatusOK {
		t.Fatalf("更新论文状态=%d", patch.StatusCode)
	}
}

func TestPaperHTTPEvidenceRejectsFabricatedQuote(t *testing.T) {
	store, err := sqlite.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := NewPaperHandlerForTest(store)
	project := postJSON(t, h, "/api/projects", map[string]any{"title": "证据项目"})
	var created struct {
		ID string `json:"id"`
	}
	decodeJSON(t, project, &created)
	paperResponse := postJSON(t, h, "/api/projects/"+created.ID+"/papers", map[string]any{"title": "论文", "arxiv_id": "2401.00001"})
	var paper struct {
		ID string `json:"id"`
	}
	decodeJSON(t, paperResponse, &paper)
	documentResponse := postJSON(t, h, "/api/projects/"+created.ID+"/papers/"+paper.ID+"/documents", map[string]any{"text": "原文片段", "locator": "abstract"})
	var document struct {
		ID string `json:"id"`
	}
	decodeJSON(t, documentResponse, &document)
	evidence := postJSON(t, h, "/api/projects/"+created.ID+"/papers/"+paper.ID+"/evidence", map[string]any{"document_id": document.ID, "quote": "伪造片段", "start_rune": 0, "end_rune": 4})
	if evidence.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("伪造证据状态=%d", evidence.StatusCode)
	}
}

func NewPaperHandlerForTest(store *sqlite.Store) http.Handler {
	return httpapi.NewHandlerWithAcademicAndEvidence(app.NewProjectService(store), app.NewSessionService(store), nil, app.NewPaperService(store), nil, app.NewEvidenceService(store), store, "", testLogger())
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func postJSON(t *testing.T, h http.Handler, path string, value any) *http.Response {
	t.Helper()
	body, _ := json.Marshal(value)
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return recorder.Result()
}

func patchJSON(t *testing.T, h http.Handler, path string, value any) *http.Response {
	t.Helper()
	body, _ := json.Marshal(value)
	request := httptest.NewRequest(http.MethodPatch, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return recorder.Result()
}

func get(t *testing.T, h http.Handler, path string) *http.Response {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return recorder.Result()
}

func decodeJSON(t *testing.T, response *http.Response, value any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}
