package rerank_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/extensions/retrieval/rerank"
	"github.com/goharness/goharness/pkg/llm"
)

func TestRerankFieldMappingAndSort(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &gotBody)
		// 故意乱序返回，验证客户端按分数降序。
		json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"index": 0, "relevance_score": 0.3},
				{"index": 2, "relevance_score": 0.9},
				{"index": 1, "relevance_score": 0.6},
			},
		})
	}))
	defer srv.Close()

	r, err := rerank.New(rerank.Config{Endpoint: srv.URL, Model: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	results, err := r.Invoke(context.Background(), retrieval.RerankRequest{
		Query: "q",
		Documents: []retrieval.RerankDocument{
			{ID: "a", Text: "ta"}, {ID: "b", Text: "tb"}, {ID: "c", Text: "tc"},
		},
		TopK: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("results = %d", len(results))
	}
	if results[0].Index != 2 || results[0].Score != 0.9 || results[0].ID != "c" {
		t.Fatalf("results[0] = %+v", results[0])
	}
	if results[1].Index != 1 || results[2].Index != 0 {
		t.Fatalf("order = %v", results)
	}
	// 请求字段名映射：默认 Cohere 协议。
	if gotBody["model"] != "m1" || gotBody["query"] != "q" || gotBody["top_n"] != float64(2) {
		t.Fatalf("request body = %v", gotBody)
	}
	docs, _ := gotBody["documents"].([]any)
	if len(docs) != 3 {
		t.Fatalf("documents = %v", docs)
	}
}

func TestRerankCustomWire(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		if _, ok := body["docs"]; !ok {
			t.Error("custom field 'docs' missing")
		}
		json.NewEncoder(w).Encode(map[string]any{
			"ranking": []map[string]any{{"position": 0, "relevance": 0.5}},
		})
	}))
	defer srv.Close()
	r, err := rerank.New(rerank.Config{
		Endpoint: srv.URL,
		Wire: rerank.Wire{
			DocumentsField: "docs", ResultsField: "ranking",
			IndexField: "position", ScoreField: "relevance",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := r.Invoke(context.Background(), retrieval.RerankRequest{
		Query: "q", Documents: []retrieval.RerankDocument{{ID: "a", Text: "t"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Index != 0 {
		t.Fatalf("results = %+v", results)
	}
}

func TestRerankEmptyDocuments(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer srv.Close()
	r, _ := rerank.New(rerank.Config{Endpoint: srv.URL})
	out, err := r.Invoke(context.Background(), retrieval.RerankRequest{Query: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 || calls.Load() != 0 {
		t.Fatal("empty documents must not call the endpoint")
	}
}

func TestRerankRetryOn429(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{"index": 0, "relevance_score": 1.0}},
		})
	}))
	defer srv.Close()
	r, _ := rerank.New(rerank.Config{
		Endpoint: srv.URL,
		Retry:    llm.RetryConfig{MaxAttempts: 2, InitialDelay: 1, MaxDelay: 10, NoJitter: true},
	})
	results, err := r.Invoke(context.Background(), retrieval.RerankRequest{
		Query: "q", Documents: []retrieval.RerankDocument{{ID: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || calls.Load() != 2 {
		t.Fatalf("calls = %d results = %+v", calls.Load(), results)
	}
}
