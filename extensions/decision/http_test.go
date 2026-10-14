package decision_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/goharness/goharness/extensions/decision"
	"github.com/goharness/goharness/pkg/llm"
)

func TestHTTPAdapterRoundTrip(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			t.Error("missing bearer auth")
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"answers": map[string]any{
			"route": map[string]any{"type": "choice", "choice": "billing", "probabilities": map[string]float64{"billing": 0.9, "tech": 0.1}, "confidence": 0.95},
		}})
	}))
	defer srv.Close()

	model, err := decision.NewHTTP(decision.HTTPConfig{
		Endpoint: srv.URL, APIKey: "k", Model: "dm-1", Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := model.Invoke(context.Background(), decision.Request{
		State: map[string]any{"text": "退款"},
		Questions: map[string]decision.Question{
			"route": decision.Choice("选哪个", decision.Option{ID: "billing"}, decision.Option{ID: "tech"}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := resp.Answers["route"]
	if a.Choice != "billing" || a.Confidence != 0.95 || a.Probabilities["billing"] != 0.9 {
		t.Fatalf("unexpected answer: %+v", a)
	}
	if gotBody["model"] != "dm-1" {
		t.Fatalf("model field: %v", gotBody["model"])
	}
	qs := gotBody["questions"].(map[string]any)
	q := qs["route"].(map[string]any)
	if q["type"] != "choice" || len(q["options"].([]any)) != 2 {
		t.Fatalf("question wire: %v", q)
	}
	if opt := q["options"].([]any)[0].(map[string]any); opt["id"] != "billing" {
		t.Fatalf("option wire: %v", opt)
	}
}

func TestHTTPAdapterCustomWire(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["m"] != "dm-2" || body["qs"] == nil || body["st"] == nil {
			t.Errorf("custom wire fields missing: %v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"out": map[string]any{
			"ok": map[string]any{"p_true": 0.7, "confidence": 0.8},
		}})
	}))
	defer srv.Close()

	model, err := decision.NewHTTP(decision.HTTPConfig{
		Endpoint: srv.URL, Timeout: 5 * time.Second,
		Wire: decision.Wire{ModelField: "m", StateField: "st", QuestionsField: "qs", AnswersField: "out"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := model.Invoke(context.Background(), decision.Request{
		Model: "dm-2", State: "x",
		Questions: map[string]decision.Question{"ok": decision.Noul("成立吗")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Answers["ok"].PTrue != 0.7 {
		t.Fatalf("p_true: %+v", resp.Answers["ok"])
	}
}

func TestHTTPAdapterMissingAnswerFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"answers": map[string]any{}})
	}))
	defer srv.Close()
	model, _ := decision.NewHTTP(decision.HTTPConfig{Endpoint: srv.URL, Timeout: 5 * time.Second})
	_, err := model.Invoke(context.Background(), decision.Request{
		Questions: map[string]decision.Question{"q1": decision.Noul("x")},
	})
	if err == nil {
		t.Fatal("missing answer should fail")
	}
}

func TestHTTPAdapterRetryOn429(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"answers": map[string]any{
			"q1": map[string]any{"p_true": 0.9},
		}})
	}))
	defer srv.Close()
	model, _ := decision.NewHTTP(decision.HTTPConfig{
		Endpoint: srv.URL, Timeout: 5 * time.Second,
		Retry: llm.RetryConfig{MaxAttempts: 2},
	})
	resp, err := model.Invoke(context.Background(), decision.Request{
		Questions: map[string]decision.Question{"q1": decision.Noul("x")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || resp.Answers["q1"].PTrue != 0.9 {
		t.Fatalf("calls=%d resp=%+v", calls, resp)
	}
}

func TestHTTPAdapterValidatesRequest(t *testing.T) {
	model, _ := decision.NewHTTP(decision.HTTPConfig{Endpoint: "https://x", Timeout: time.Second})
	if _, err := model.Invoke(context.Background(), decision.Request{
		Questions: map[string]decision.Question{"bad id!": decision.Noul("x")},
	}); err == nil {
		t.Fatal("invalid question id should fail before any HTTP call")
	}
	if _, err := decision.NewHTTP(decision.HTTPConfig{Timeout: time.Second}); err == nil {
		t.Fatal("empty endpoint should fail")
	}
}

func TestHTTPAdapterRejectsInvalidAnswer(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"answers": map[string]any{
			"q1": map[string]any{"p_true": 5, "confidence": 0.9},
		}})
	}))
	defer srv.Close()
	model, err := decision.NewHTTP(decision.HTTPConfig{
		Endpoint: srv.URL, Timeout: 5 * time.Second,
		Retry: llm.RetryConfig{MaxAttempts: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = model.Invoke(context.Background(), decision.Request{
		Questions: map[string]decision.Question{"q1": decision.Noul("x")},
	})
	if !errors.Is(err, decision.ErrInvalidResponse) {
		t.Fatalf("expected ErrInvalidResponse, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("invalid response must not be retried, calls=%d", calls)
	}
}

func TestHTTPEndpointScheme(t *testing.T) {
	if _, err := decision.NewHTTP(decision.HTTPConfig{Endpoint: "http://api.example.com/decide"}); err == nil {
		t.Error("non-loopback http should be rejected")
	}
	if _, err := decision.NewHTTP(decision.HTTPConfig{
		Endpoint: "http://api.example.com/decide", AllowInsecureHTTP: true,
	}); err != nil {
		t.Errorf("AllowInsecureHTTP: %v", err)
	}
	if _, err := decision.NewHTTP(decision.HTTPConfig{Endpoint: "https://api.example.com/decide"}); err != nil {
		t.Errorf("https: %v", err)
	}
	if _, err := decision.NewHTTP(decision.HTTPConfig{Endpoint: "http://127.0.0.1:8080/x"}); err != nil {
		t.Errorf("loopback http: %v", err)
	}
	if _, err := decision.NewHTTP(decision.HTTPConfig{Endpoint: "http://localhost:9/x"}); err != nil {
		t.Errorf("localhost http: %v", err)
	}
	if _, err := decision.NewHTTP(decision.HTTPConfig{Endpoint: "ftp://x"}); err == nil {
		t.Error("ftp scheme should be rejected")
	}
	if _, err := decision.NewHTTP(decision.HTTPConfig{Endpoint: "://bad"}); err == nil {
		t.Error("unparseable endpoint should be rejected")
	}
}
