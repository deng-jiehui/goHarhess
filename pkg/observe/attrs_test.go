package observe

import (
	"context"
	"testing"
)

func collectEvents() (*[]Event, Observer) {
	events := &[]Event{}
	return events, ObserverFunc(func(_ context.Context, e Event) {
		*events = append(*events, e)
	})
}

func TestEmitRedactsSensitiveAttrs(t *testing.T) {
	events, observer := collectEvents()
	ctx := With(context.Background(), observer)
	Emit(ctx, Event{Kind: KindNode, Name: "n", Attrs: map[string]any{
		"authorization":   "Bearer abc",
		"access_token":    "tok",
		"api_key":         "k",
		"PRIVATE-KEY":     "pem",
		"db.password":     "pw",
		"client_secret":   "sec",
		"cookie":          "c",
		"aws.credentials": "cred",
	}})
	if len(*events) != 1 {
		t.Fatalf("events=%d", len(*events))
	}
	for _, key := range []string{"authorization", "access_token", "api_key", "PRIVATE-KEY", "db.password", "client_secret", "cookie", "aws.credentials"} {
		if (*events)[0].Attrs[key] != redactedValue {
			t.Errorf("attr %s = %v, want %s", key, (*events)[0].Attrs[key], redactedValue)
		}
	}
}

func TestEmitKeepsNonSensitiveAttrs(t *testing.T) {
	events, observer := collectEvents()
	ctx := With(context.Background(), observer)
	Emit(ctx, Event{Kind: KindNode, Name: "n", Attrs: map[string]any{
		"input_tokens":    10,
		"max_tokens":      100,
		"tool.read_only":  true,
		"tenant_id":       "t1",
		"conversation_id": "c1",
		"tokenize":        "not-a-secret-word",
		"keynote":         "adjacent-not-matched",
	}})
	attrs := (*events)[0].Attrs
	for key, value := range attrs {
		if value == redactedValue {
			t.Errorf("attr %s unexpectedly redacted", key)
		}
	}
	if attrs["input_tokens"] != 10 || attrs["tenant_id"] != "t1" {
		t.Errorf("attrs changed: %v", attrs)
	}
}

func TestEmitCustomSensitiveKeys(t *testing.T) {
	events, observer := collectEvents()
	ctx := WithSensitiveKeys(context.Background(), "X-Internal-ID", "session.handle")
	ctx = With(ctx, observer)
	Emit(ctx, Event{Kind: KindNode, Name: "n", Attrs: map[string]any{
		"x_internal_id":   "id-1",
		"session_handle":  "h-1",
		"SESSION_HANDLE2": "keep",
		"conversation_id": "c1",
	}})
	attrs := (*events)[0].Attrs
	if attrs["x_internal_id"] != redactedValue || attrs["session_handle"] != redactedValue {
		t.Errorf("custom keys not redacted: %v", attrs)
	}
	if attrs["SESSION_HANDLE2"] != "keep" || attrs["conversation_id"] != "c1" {
		t.Errorf("unrelated attrs changed: %v", attrs)
	}
}

func TestSanitizeDoesNotMutateOriginal(t *testing.T) {
	events, observer := collectEvents()
	ctx := With(context.Background(), observer)
	original := map[string]any{"password": "pw", "ok": 1}
	Emit(ctx, Event{Kind: KindNode, Name: "n", Attrs: original})
	if original["password"] != "pw" {
		t.Error("original attrs mutated")
	}
	if (*events)[0].Attrs["password"] != redactedValue {
		t.Error("emitted attrs not redacted")
	}
}

func TestEmitAttrsNilSafe(t *testing.T) {
	events, observer := collectEvents()
	ctx := With(context.Background(), observer)
	Emit(ctx, Event{Kind: KindNode, Name: "n"})
	if len(*events) != 1 {
		t.Fatal("event not delivered")
	}
}

func BenchmarkSanitizeEventAttrsNoHit(b *testing.B) {
	ctx := context.Background()
	attrs := map[string]any{
		"input_tokens":    10,
		"max_tokens":      100,
		"tool.read_only":  true,
		"tenant_id":       "t1",
		"conversation_id": "c1",
		"step":            2,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if sanitizeEventAttrs(ctx, attrs) == nil {
			b.Fatal("nil")
		}
	}
}
