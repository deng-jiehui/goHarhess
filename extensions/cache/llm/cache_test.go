package llm_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	cachellm "goharness/extensions/cache/llm"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func TestMemoryCacheLRUAndTTL(t *testing.T) {
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	store, err := cachellm.NewMemoryCache(cachellm.MemoryCacheConfig{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	mustSet(t, store, "a", "A", time.Minute)
	mustSet(t, store, "b", "B", time.Minute)
	if _, found, err := store.Get(ctx, "a"); err != nil || !found {
		t.Fatalf("Get(a) found=%v err=%v", found, err)
	}
	mustSet(t, store, "c", "C", time.Minute)
	if _, found, err := store.Get(ctx, "b"); err != nil || found {
		t.Fatalf("evicted Get(b) found=%v err=%v", found, err)
	}
	now = now.Add(2 * time.Minute)
	if _, found, err := store.Get(ctx, "a"); err != nil || found {
		t.Fatalf("expired Get(a) found=%v err=%v", found, err)
	}
	if store.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", store.Len())
	}
}

func TestCachedModelReplaysStandardEvents(t *testing.T) {
	upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("hel"), delta("hello")}}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model"})

	first := collect(t, mustStream(t, cached, context.Background(), "same", llm.ChatOptions{}))
	second := collect(t, mustStream(t, cached, context.Background(), "same", llm.ChatOptions{}))
	if upstream.Calls() != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.Calls())
	}
	if fmt.Sprint(first) != fmt.Sprint(second) || len(second) != 2 {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
	stats := cached.Stats()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Writes != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestCachedModelInvokeUsesSameCacheProtocol(t *testing.T) {
	upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("answer")}}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model"})
	request := llm.Request{Messages: []schema.Message{schema.UserMessage("same")}}
	for range 2 {
		response, err := cached.Invoke(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Messages) != 1 || response.Messages[0].Text() != "answer" {
			t.Fatalf("response = %#v", response)
		}
	}
	if upstream.Calls() != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.Calls())
	}
}

func TestCachedModelCombinesConcurrentMisses(t *testing.T) {
	release := make(chan struct{})
	upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("shared")}, release: release}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model"})

	const callers = 8
	start := make(chan struct{})
	results := make(chan string, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			stream, err := cached.Stream(context.Background(), llm.Request{Messages: []schema.Message{schema.UserMessage("same")}})
			if err != nil {
				results <- "error: " + err.Error()
				return
			}
			events, err := collectStream(stream)
			if err != nil {
				results <- "error: " + err.Error()
				return
			}
			results <- events[len(events)-1].Messages[0].Text()
		}()
	}
	ready.Wait()
	close(start)
	waitForCalls(t, upstream, 1)
	close(release)
	for range callers {
		if got := <-results; got != "shared" {
			t.Fatalf("result = %q, want shared", got)
		}
	}
	if cached.Stats().Shared != callers-1 {
		t.Fatalf("shared requests = %d, want %d", cached.Stats().Shared, callers-1)
	}
}

func TestCachedModelIsolatesTenants(t *testing.T) {
	upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("ok")}}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model", Namespace: "prod"})
	for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-a"} {
		ctx := cachellm.WithCacheTenant(context.Background(), tenant)
		collect(t, mustStream(t, cached, ctx, "same", llm.ChatOptions{}))
	}
	if upstream.Calls() != 2 {
		t.Fatalf("upstream calls = %d, want 2", upstream.Calls())
	}
}

func TestCachedModelBypassesSensitiveRequests(t *testing.T) {
	upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("secret")}}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{
		ModelID: "test-model",
		SensitiveDetector: func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) bool {
			return messages[len(messages)-1].Text() == "password"
		},
	})
	for range 2 {
		collect(t, mustStream(t, cached, context.Background(), "password", llm.ChatOptions{}))
	}
	collect(t, mustStream(t, cached, cachellm.WithSensitiveRequest(context.Background()), "marked", llm.ChatOptions{}))
	collect(t, mustStream(t, cached, cachellm.WithCacheBypass(context.Background()), "manual", llm.ChatOptions{}))
	if upstream.Calls() != 4 || cached.Stats().Bypasses != 4 {
		t.Fatalf("calls=%d stats=%#v", upstream.Calls(), cached.Stats())
	}
}

func TestCachedModelFunctionCallPolicy(t *testing.T) {
	options := llm.ChatOptions{Functions: []schema.FunctionDefinition{{Name: "search"}}}
	t.Run("disabled by default", func(t *testing.T) {
		upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("tool")}}
		cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model"})
		for range 2 {
			collect(t, mustStream(t, cached, context.Background(), "search", options))
		}
		if upstream.Calls() != 2 {
			t.Fatalf("upstream calls = %d, want 2", upstream.Calls())
		}
	})
	t.Run("explicitly enabled", func(t *testing.T) {
		upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("tool")}}
		cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model", CacheFunctionCalls: true})
		for range 2 {
			collect(t, mustStream(t, cached, context.Background(), "search", options))
		}
		if upstream.Calls() != 1 {
			t.Fatalf("upstream calls = %d, want 1", upstream.Calls())
		}
	})
}

func TestCachedModelDoesNotCacheErrors(t *testing.T) {
	upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("partial"), {Err: errors.New("provider failed")}}}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model"})
	for range 2 {
		stream := mustStream(t, cached, context.Background(), "same", llm.ChatOptions{})
		if _, err := collectStream(stream); err == nil {
			t.Fatal("collectStream returned nil error")
		}
	}
	if upstream.Calls() != 2 || cached.Stats().Writes != 0 {
		t.Fatalf("calls=%d stats=%#v", upstream.Calls(), cached.Stats())
	}
}

func TestCachedModelDoesNotCacheSilentLeaderCancellation(t *testing.T) {
	upstream := &silentCancelModel{}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "cancel-model"})
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cached.Stream(ctx, llm.Request{Messages: []schema.Message{schema.UserMessage("same")}})
	if err != nil {
		t.Fatal(err)
	}
	if item := <-stream; item.Err != nil || item.Value.Kind != llm.EventDelta {
		t.Fatalf("first event = %#v", item)
	}
	cancel()
	for range stream {
	}
	second, err := cached.Stream(context.Background(), llm.Request{Messages: []schema.Message{schema.UserMessage("same")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collectStream(second); err != nil {
		t.Fatal(err)
	}
	if upstream.Calls() != 2 || cached.Stats().Writes != 1 {
		t.Fatalf("calls=%d stats=%#v", upstream.Calls(), cached.Stats())
	}
}

func TestCachedModelFollowerSurvivesLeaderCancellation(t *testing.T) {
	release := make(chan struct{})
	upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("shared")}, release: release}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "leader-cancel"})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := mustStream(t, cached, leaderCtx, "same", llm.ChatOptions{})
	follower := mustStream(t, cached, context.Background(), "same", llm.ChatOptions{})
	deadline := time.Now().Add(time.Second)
	for cached.Stats().Shared != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if cached.Stats().Shared != 1 {
		t.Fatalf("stats=%#v", cached.Stats())
	}
	cancelLeader()
	for range leader {
	}
	close(release)
	events, err := collectStream(follower)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Messages[0].Text() != "shared" {
		t.Fatalf("events=%#v", events)
	}
	if upstream.Calls() != 1 || cached.Stats().Writes != 1 {
		t.Fatalf("calls=%d stats=%#v", upstream.Calls(), cached.Stats())
	}
}

type stagedStreamModel struct {
	release chan struct{}
	calls   atomic.Int64
}

func (m *stagedStreamModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *stagedStreamModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
	m.calls.Add(1)
	out := make(chan runnable.Item[llm.Event])
	go func() {
		defer close(out)
		select {
		case <-ctx.Done():
			return
		case out <- delta("first"):
		}
		select {
		case <-ctx.Done():
			return
		case <-m.release:
		}
		select {
		case <-ctx.Done():
			return
		case out <- delta("second"):
		}
		select {
		case <-ctx.Done():
			return
		case out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}:
		}
	}()
	return out, nil
}

func (*stagedStreamModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*stagedStreamModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "staged"} }

func TestCachedModelFollowerReceivesIncrementalHistory(t *testing.T) {
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	upstream := &stagedStreamModel{release: release}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "incremental"})
	leader := mustStream(t, cached, context.Background(), "same", llm.ChatOptions{})
	first := <-leader
	if first.Err != nil || first.Value.Messages[0].Text() != "first" {
		t.Fatalf("leader first=%#v", first)
	}
	follower := mustStream(t, cached, context.Background(), "same", llm.ChatOptions{})
	select {
	case item := <-follower:
		if item.Err != nil || item.Value.Messages[0].Text() != "first" {
			t.Fatalf("follower first=%#v", item)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("follower did not receive incremental history")
	}
	close(release)
	released = true
	for range leader {
	}
	for range follower {
	}
}

func TestRedisCacheRoundTripAndTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := cachellm.NewRedisCache(cachellm.RedisCacheConfig{Client: client, Prefix: "test"})
	if err != nil {
		t.Fatal(err)
	}
	entry := cachellm.CacheEntry{Events: []llm.Event{{Kind: llm.EventDelta, Messages: []schema.Message{schema.AssistantMessage("redis")}}}, CreatedAt: time.Now().UTC()}
	if err := store.Set(context.Background(), "key", entry, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Get(context.Background(), "key")
	if err != nil || !found || got.Events[0].Messages[0].Text() != "redis" {
		t.Fatalf("Get() entry=%#v found=%v err=%v", got, found, err)
	}
	server.FastForward(2 * time.Minute)
	if _, found, err := store.Get(context.Background(), "key"); err != nil || found {
		t.Fatalf("expired Get() found=%v err=%v", found, err)
	}
}

type streamModel struct {
	items   []runnable.Item[llm.Event]
	release <-chan struct{}
	calls   atomic.Int64
}

type silentCancelModel struct{ calls atomic.Int64 }

func (m *silentCancelModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *silentCancelModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
	call := m.calls.Add(1)
	out := make(chan runnable.Item[llm.Event], 2)
	out <- delta("partial")
	if call > 1 {
		out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
		close(out)
		return out, nil
	}
	go func() {
		defer close(out)
		<-ctx.Done()
	}()
	return out, nil
}

func (*silentCancelModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*silentCancelModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "silent-cancel"} }
func (m *silentCancelModel) Calls() int                   { return int(m.calls.Load()) }

func (m *streamModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *streamModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
	m.calls.Add(1)
	out := make(chan runnable.Item[llm.Event])
	go func() {
		defer close(out)
		if m.release != nil {
			select {
			case <-ctx.Done():
				out <- runnable.Item[llm.Event]{Err: ctx.Err()}
				return
			case <-m.release:
			}
		}
		for _, item := range m.items {
			select {
			case <-ctx.Done():
				out <- runnable.Item[llm.Event]{Err: ctx.Err()}
				return
			case out <- item:
			}
			if item.Err != nil {
				return
			}
		}
		out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
	}()
	return out, nil
}

func (m *streamModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true, Reasoning: true}
}

func (m *streamModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "stream", Provider: "test", ModelType: "scripted"}
}

func (m *streamModel) Calls() int { return int(m.calls.Load()) }

func newMemoryCachedModel(t *testing.T, upstream llm.Model, cfg cachellm.CacheConfig) *cachellm.CachedModel {
	t.Helper()
	store, err := cachellm.NewMemoryCache(cachellm.MemoryCacheConfig{Capacity: 100})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Store = store
	cached, err := cachellm.NewCachedModel(upstream, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cached
}

func mustStream(t *testing.T, model llm.Model, ctx context.Context, text string, opts llm.ChatOptions) runnable.Stream[llm.Event] {
	t.Helper()
	stream, err := model.Stream(ctx, llm.Request{Messages: []schema.Message{schema.UserMessage(text)}, Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func collect(t *testing.T, stream runnable.Stream[llm.Event]) []llm.Event {
	t.Helper()
	events, err := collectStream(stream)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func collectStream(stream runnable.Stream[llm.Event]) ([]llm.Event, error) {
	var events []llm.Event
	for item := range stream {
		if item.Err != nil {
			return events, item.Err
		}
		if item.Value.Kind != llm.EventDone {
			events = append(events, item.Value)
		}
	}
	return events, nil
}

func delta(text string) runnable.Item[llm.Event] {
	return runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDelta, Messages: []schema.Message{schema.AssistantMessage(text)}}}
}

func mustSet(t *testing.T, store cachellm.CacheStore, key, text string, ttl time.Duration) {
	t.Helper()
	err := store.Set(context.Background(), key, cachellm.CacheEntry{
		Events:    []llm.Event{{Kind: llm.EventDelta, Messages: []schema.Message{schema.AssistantMessage(text)}}},
		CreatedAt: time.Now().UTC(),
	}, ttl)
	if err != nil {
		t.Fatal(err)
	}
}

func waitForCalls(t *testing.T, model *streamModel, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for model.Calls() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if model.Calls() != want {
		t.Fatalf("upstream calls = %d, want %d", model.Calls(), want)
	}
}
