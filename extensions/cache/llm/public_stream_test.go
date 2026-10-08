package llm_test

import (
	"context"
	"errors"
	cachellm "github.com/goharness/goharness/extensions/cache/llm"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/stream"
	"io"
	"testing"
)

func TestPublicCacheHitPublishesCompleteMessage(t *testing.T) {
	upstream := &testModel{response: response("cached")}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "public"})
	if _, err := cached.Invoke(context.Background(), request("q")); err != nil {
		t.Fatal(err)
	}
	r, _ := stream.Run(context.Background(), func(ctx context.Context) (llm.Response, error) { return cached.Invoke(ctx, request("q")) })
	defer r.Close()
	messages := 0
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Message != nil {
			messages++
			if e.Message.Snapshot.JoinedText("") != "cached" {
				t.Fatal(e)
			}
		}
	}
	if messages != 1 || upstream.Calls() != 1 {
		t.Fatalf("messages=%d calls=%d", messages, upstream.Calls())
	}
}

func TestMessageIdentityDoesNotChangeCacheKey(t *testing.T) {
	upstream := &testModel{response: response("cached")}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "identity"})
	for _, id := range []string{"first-run", "second-run"} {
		req := request("same")
		req.Messages[0].ID = id
		if _, err := cached.Invoke(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if upstream.Calls() != 1 {
		t.Fatalf("identity changed cache key: %d calls", upstream.Calls())
	}
}
