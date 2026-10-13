package embedding_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/extensions/retrieval/embedding"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
)

// fakeServer 回送确定性的嵌入：第 i 条输入得到向量 {i, len(input)}。
type fakeServer struct {
	t         *testing.T
	calls     atomic.Int64
	batchSize atomic.Int64
	failNext  atomic.Int32 // 前 N 次请求返回 500
}

func (f *fakeServer) handler(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	if r.Header.Get("Authorization") != "Bearer k" {
		http.Error(w, `{"error":{"message":"unauthorized"}}`, http.StatusUnauthorized)
		return
	}
	var req struct {
		Input []string `json:"input"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	f.batchSize.Store(int64(len(req.Input)))
	if f.failNext.Add(-1) >= 0 {
		w.Header().Set("Retry-After", "0")
		http.Error(w, `{"error":{"message":"boom"}}`, http.StatusInternalServerError)
		return
	}
	type item struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	}
	data := make([]item, len(req.Input))
	for i, in := range req.Input {
		data[i] = item{Embedding: []float32{float32(len(in)), 1}, Index: i}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "usage": map[string]any{"prompt_tokens": 7}})
}

func newClient(t *testing.T, srv *httptest.Server, cfg embedding.Config) runnable.Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse] {
	t.Helper()
	cfg.Endpoint = srv.URL
	cfg.APIKey = "k"
	c, err := embedding.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestEmbeddingBatchesPreserveOrder(t *testing.T) {
	f := &fakeServer{t: t}
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	defer srv.Close()
	c := newClient(t, srv, embedding.Config{BatchSize: 2})
	inputs := []string{"a", "bb", "ccc", "dddd", "eeeee"}
	resp, err := c.Invoke(context.Background(), retrieval.EmbeddingRequest{Inputs: inputs})
	if err != nil {
		t.Fatal(err)
	}
	if f.calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", f.calls.Load())
	}
	if f.batchSize.Load() != 1 {
		t.Fatalf("last batch size = %d, want 1", f.batchSize.Load())
	}
	for i, in := range inputs {
		if resp.Vectors[i][0] != float32(len(in)) {
			t.Fatalf("vector %d = %v, want len %d", i, resp.Vectors[i], len(in))
		}
	}
	if resp.Usage.InputTokens != 21 {
		t.Fatalf("usage = %d, want 21", resp.Usage.InputTokens)
	}
}

func TestEmbeddingRetriesWithRetryAfter(t *testing.T) {
	f := &fakeServer{t: t}
	f.failNext.Store(1) // 第一次失败，随后成功
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	defer srv.Close()
	c := newClient(t, srv, embedding.Config{
		Retry: llm.RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Second},
	})
	resp, err := c.Invoke(context.Background(), retrieval.EmbeddingRequest{Inputs: []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	if f.calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", f.calls.Load())
	}
	if len(resp.Vectors) != 1 {
		t.Fatal("expected 1 vector")
	}
}

func TestEmbeddingRetryAfterExceededStops(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "slow", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c, err := embedding.New(embedding.Config{
		Endpoint: srv.URL,
		Retry:    llm.RetryConfig{MaxAttempts: 5, InitialDelay: time.Millisecond, MaxDelay: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.Invoke(context.Background(), retrieval.EmbeddingRequest{Inputs: []string{"x"}})
	var se interface{ StatusCode() int }
	if !errors.As(err, &se) || se.StatusCode() != 429 {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("Retry-After 超上限应直接返回而不是等待")
	}
}

func TestEmbeddingCountMismatchFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}}) // 0 条
	}))
	defer srv.Close()
	c, err := embedding.New(embedding.Config{Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Invoke(context.Background(), retrieval.EmbeddingRequest{Inputs: []string{"x", "y"}})
	if err == nil || !strings.Contains(err.Error(), "2 inputs") {
		t.Fatalf("err = %v", err)
	}
}

func TestEmbeddingContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 延迟响应：客户端取消必须先返回，服务端随后正常结束，Close 不阻塞。
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c, err := embedding.New(embedding.Config{Endpoint: srv.URL, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = c.Invoke(ctx, retrieval.EmbeddingRequest{Inputs: []string{"x"}})
	if err == nil {
		t.Fatal("expected context error")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("cancel took %v", elapsed)
	}
}

type countingLimiter struct{ n atomic.Int64 }

func (l *countingLimiter) Wait(context.Context) error { l.n.Add(1); return nil }

func TestEmbeddingLimiterGatePerBatch(t *testing.T) {
	f := &fakeServer{t: t}
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	defer srv.Close()
	limiter := &countingLimiter{}
	c := newClient(t, srv, embedding.Config{BatchSize: 1, Limiter: limiter})
	if _, err := c.Invoke(context.Background(), retrieval.EmbeddingRequest{Inputs: []string{"a", "b", "c"}}); err != nil {
		t.Fatal(err)
	}
	if limiter.n.Load() != 3 {
		t.Fatalf("limiter waits = %d, want 3", limiter.n.Load())
	}
}

func TestEmbeddingConfigValidation(t *testing.T) {
	if _, err := embedding.New(embedding.Config{}); err == nil {
		t.Fatal("empty endpoint should fail")
	}
	if _, err := embedding.New(embedding.Config{Endpoint: "x", BatchSize: -1}); err == nil {
		t.Fatal("negative batch size should fail")
	}
}
