package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	cachellm "github.com/goharness/goharness/extensions/cache/llm"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
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

func TestCachedModelCachesCompleteResponse(t *testing.T) {
	upstream := &testModel{response: response("answer")}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model"})
	for range 2 {
		got, err := cached.Invoke(context.Background(), request("same"))
		if err != nil || got.Messages[0].JoinedText("") != "answer" {
			t.Fatalf("response=%#v err=%v", got, err)
		}
	}
	if upstream.Calls() != 1 {
		t.Fatalf("upstream calls=%d, want 1", upstream.Calls())
	}
	stats := cached.Stats()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Writes != 1 {
		t.Fatalf("stats=%#v", stats)
	}
}

func TestCachedModelCombinesConcurrentMisses(t *testing.T) {
	release := make(chan struct{})
	upstream := &testModel{response: response("shared"), release: release}
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
			got, err := cached.Invoke(context.Background(), request("same"))
			if err != nil {
				results <- "error: " + err.Error()
				return
			}
			results <- got.Messages[0].JoinedText("")
		}()
	}
	ready.Wait()
	close(start)
	waitForCalls(t, upstream, 1)
	close(release)
	for range callers {
		if got := <-results; got != "shared" {
			t.Fatalf("result=%q", got)
		}
	}
	if cached.Stats().Shared != callers-1 {
		t.Fatalf("shared=%d, want %d", cached.Stats().Shared, callers-1)
	}
}

func TestCachedModelIsolatesTenants(t *testing.T) {
	upstream := &testModel{response: response("ok")}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model", Namespace: "prod"})
	for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-a"} {
		if _, err := cached.Invoke(cachellm.WithCacheTenant(context.Background(), tenant), request("same")); err != nil {
			t.Fatal(err)
		}
	}
	if upstream.Calls() != 2 {
		t.Fatalf("calls=%d, want 2", upstream.Calls())
	}
}

func TestCachedModelBypassesSensitiveRequests(t *testing.T) {
	upstream := &testModel{response: response("secret")}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{
		ModelID: "test-model",
		SensitiveDetector: func(_ context.Context, messages []schema.Message, _ llm.Options) bool {
			return messages[len(messages)-1].JoinedText("") == "password"
		},
	})
	contexts := []context.Context{
		context.Background(), context.Background(),
		cachellm.WithSensitiveRequest(context.Background()),
		cachellm.WithCacheBypass(context.Background()),
	}
	texts := []string{"password", "password", "marked", "manual"}
	for index := range contexts {
		if _, err := cached.Invoke(contexts[index], request(texts[index])); err != nil {
			t.Fatal(err)
		}
	}
	if upstream.Calls() != 4 || cached.Stats().Bypasses != 4 {
		t.Fatalf("calls=%d stats=%#v", upstream.Calls(), cached.Stats())
	}
}

func TestCachedModelFunctionCallPolicy(t *testing.T) {
	requestWithTools := request("search")
	requestWithTools.Functions = []schema.FunctionDefinition{{Name: "search"}}
	for _, test := range []struct {
		name    string
		enabled bool
		calls   int
	}{{"disabled", false, 2}, {"enabled", true, 1}} {
		t.Run(test.name, func(t *testing.T) {
			upstream := &testModel{response: response("tool")}
			cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "test-model", CacheFunctionCalls: test.enabled})
			for range 2 {
				if _, err := cached.Invoke(context.Background(), requestWithTools); err != nil {
					t.Fatal(err)
				}
			}
			if upstream.Calls() != test.calls {
				t.Fatalf("calls=%d, want %d", upstream.Calls(), test.calls)
			}
		})
	}
}

func TestCachedModelBlockOrderAffectsKeyAndCloneIsolatesMetadata(t *testing.T) {
	upstream := &testModel{response: response("ok")}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "order"})
	a := request("same")
	a.Messages[0].ContentBlocks = []schema.ContentBlock{schema.NewTextBlock("a"), schema.NewTextBlock("b")}
	b := request("same")
	b.Messages[0].ContentBlocks = []schema.ContentBlock{schema.NewTextBlock("b"), schema.NewTextBlock("a")}
	if _, err := cached.Invoke(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := cached.Invoke(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if upstream.Calls() != 2 {
		t.Fatalf("calls=%d, want 2", upstream.Calls())
	}
	entry := cachellm.CacheEntry{Response: llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("x"))}}}
	clone := entry.Clone()
	clone.Response.Messages[0].ContentBlocks[0].Text.Text = "changed"
	if entry.Response.Messages[0].ContentBlocks[0].Text.Text != "x" {
		t.Fatal("response clone shares nested blocks")
	}
}

func TestCachedModelDoesNotCacheErrorsOrEmptyReaders(t *testing.T) {
	for _, test := range []struct {
		name  string
		model *testModel
		want  error
	}{
		{"error", &testModel{err: errors.New("provider failed")}, nil},
		{"empty", &testModel{empty: true}, llm.ErrStreamTruncated},
	} {
		t.Run(test.name, func(t *testing.T) {
			cached := newMemoryCachedModel(t, test.model, cachellm.CacheConfig{ModelID: test.name})
			for range 2 {
				_, err := cached.Invoke(context.Background(), request("same"))
				if err == nil || test.want != nil && !errors.Is(err, test.want) {
					t.Fatalf("error=%v, want %v", err, test.want)
				}
			}
			if test.model.Calls() != 2 || cached.Stats().Writes != 0 {
				t.Fatalf("calls=%d stats=%#v", test.model.Calls(), cached.Stats())
			}
		})
	}
}

func TestCachedModelFollowerSurvivesLeaderCancellation(t *testing.T) {
	release := make(chan struct{})
	upstream := &testModel{response: response("shared"), release: release}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "leader-cancel"})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader, err := cached.Stream(leaderCtx, request("same"))
	if err != nil {
		t.Fatal(err)
	}
	follower, err := cached.Stream(context.Background(), request("same"))
	if err != nil {
		t.Fatal(err)
	}
	waitForShared(t, cached, 1)
	cancelLeader()
	if _, err := leader.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error=%v", err)
	}
	close(release)
	got, err := llm.CollectResponse(context.Background(), follower)
	if err != nil || got.Messages[0].JoinedText("") != "shared" {
		t.Fatalf("follower=%#v err=%v", got, err)
	}
	if upstream.Calls() != 1 || cached.Stats().Writes != 1 {
		t.Fatalf("calls=%d stats=%#v", upstream.Calls(), cached.Stats())
	}
}

func TestCachedModelCancelsUpstreamWhenAllSubscribersLeave(t *testing.T) {
	canceled := make(chan struct{})
	upstream := &testModel{waitForCancel: canceled}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "cancel-all"})
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := cached.Stream(ctx, request("same"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = reader.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("upstream was not canceled")
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
	entry := cachellm.CacheEntry{Response: response("redis"), CreatedAt: time.Now().UTC()}
	if err := store.Set(context.Background(), "key", entry, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Get(context.Background(), "key")
	if err != nil || !found || got.Response.Messages[0].JoinedText("") != "redis" {
		t.Fatalf("entry=%#v found=%v err=%v", got, found, err)
	}
	server.FastForward(2 * time.Minute)
	if _, found, err := store.Get(context.Background(), "key"); err != nil || found {
		t.Fatalf("expired found=%v err=%v", found, err)
	}
}

// extConfigModel 声明扩展配置类型并按 tag 回应：验证 Provider 扩展参数
// 计入缓存身份。
type extConfig struct {
	Tier string
}

type opaqueConfig struct {
	Tier  string
	Hooks func() // 无法 JSON 编码的字段
}

type extConfigModel struct {
	calls atomic.Int64
}

func (m *extConfigModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	reader, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.CollectResponse(ctx, reader)
}

// hiddenConfig 的未导出字段被 JSON 静默丢弃：两个仅内部字段不同的配置
// 若按 JSON 编码会共用缓存键。
type hiddenConfig struct {
	Tier   string
	secret int
}

// anyConfig 的接口字段承载任意具体类型：命名类型 Celsius(5) 与
// float64(5) 的 Kind 相同，若指纹只按 Kind 编码会误命中。
type anyConfig struct {
	Extra any
}

// goStringConfig 实现 GoStringer：%#v 指纹会被劫持为恒定输出，使不同
// 配置值产生相同缓存键；结构指纹按字段内容取值，不受其影响。
type goStringConfig struct {
	Tier string
}

// celsius 是与 float64 同底层类型的命名类型，用于验证接口字段的
// 具体类型区分。
type celsius float64

func (goStringConfig) GoString() string { return "poison" }

func (m *extConfigModel) Stream(ctx context.Context, _ llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	m.calls.Add(1)
	resolved, remaining, err := runnable.ResolveComponentOptions(extConfig{}, options...)
	var hidden hiddenConfig
	if err == nil {
		var rest []runnable.Option
		hidden, rest, err = runnable.ResolveComponentOptions(hiddenConfig{}, remaining...)
		remaining = rest
	}
	var goStr goStringConfig
	if err == nil {
		var rest []runnable.Option
		goStr, rest, err = runnable.ResolveComponentOptions(goStringConfig{}, remaining...)
		remaining = rest
	}
	var anyCfg anyConfig
	if err == nil {
		var rest []runnable.Option
		anyCfg, rest, err = runnable.ResolveComponentOptions(anyConfig{}, remaining...)
		remaining = rest
	}
	if err == nil {
		var rest []runnable.Option
		_, rest, err = runnable.ResolveComponentOptions(opaqueConfig{}, remaining...)
		remaining = rest
	}
	if err == nil {
		err = runnable.CheckNoRemainingOptions(remaining...)
	}
	tag := resolved.Tier
	if tag == "" {
		tag = "none"
	}
	if goStr.Tier != "" {
		tag = "gs-" + goStr.Tier
	}
	if anyCfg.Extra != nil {
		tag = fmt.Sprintf("%s-any%T", tag, anyCfg.Extra)
	}
	if hidden.secret != 0 {
		tag = fmt.Sprintf("%s-secret%d", tag, hidden.secret)
	}
	reader, writer := runnable.Pipe[llm.Response](context.WithoutCancel(ctx))
	go func() {
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := writer.Send(response("tier:" + tag)); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}
func (m *extConfigModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}
func (m *extConfigModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}
func (*extConfigModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*extConfigModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "ext-model"} }
func (m *extConfigModel) Calls() int                   { return int(m.calls.Load()) }

// 扩展配置值进入缓存键：tier=fast 的缓存命中不能让 tier=deep 复用。
func TestCachedModelIncludesExtensionConfigInKey(t *testing.T) {
	upstream := &extConfigModel{}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "ext"})
	withTier := func(tier string) runnable.Option {
		return runnable.WrapOption(func(c *extConfig) { c.Tier = tier })
	}
	for _, item := range []struct {
		tier string
		want string
	}{{"fast", "tier:fast"}, {"deep", "tier:deep"}, {"fast", "tier:fast"}} {
		got, err := cached.Invoke(context.Background(), request("same"), withTier(item.tier))
		if err != nil || got.Messages[0].JoinedText("") != item.want {
			t.Fatalf("tier=%s response=%#v err=%v", item.tier, got, err)
		}
	}
	if upstream.Calls() != 2 {
		t.Fatalf("calls=%d, want 2", upstream.Calls())
	}
}

// 仅在未导出字段上不同的配置必须产生不同缓存键——JSON 会静默丢弃这些
// 字段，指纹化保留它们。
func TestCachedModelSeparatesHiddenFields(t *testing.T) {
	upstream := &extConfigModel{}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "hidden"})
	withHidden := func(secret int) runnable.Option {
		return runnable.WrapOption(func(c *hiddenConfig) { c.secret = secret })
	}
	for _, item := range []struct {
		secret int
		want   string
	}{{1, "tier:none-secret1"}, {2, "tier:none-secret2"}, {1, "tier:none-secret1"}} {
		got, err := cached.Invoke(context.Background(), request("same"), withHidden(item.secret))
		if err != nil || got.Messages[0].JoinedText("") != item.want {
			t.Fatalf("secret=%d response=%#v err=%v", item.secret, got, err)
		}
	}
	if upstream.Calls() != 2 {
		t.Fatalf("calls=%d, want 2", upstream.Calls())
	}
}

// 实现了 GoStringer 的配置类型不能让 %#v 指纹塌缩为恒定值：两个不同
// 的 Tier 必须产生不同缓存键，不能误命中。
func TestCachedModelSeparatesGoStringerConfigs(t *testing.T) {
	upstream := &extConfigModel{}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "gostring"})
	withTier := func(tier string) runnable.Option {
		return runnable.WrapOption(func(c *goStringConfig) { c.Tier = tier })
	}
	for _, item := range []struct {
		tier string
		want string
	}{{"a", "tier:gs-a"}, {"b", "tier:gs-b"}, {"a", "tier:gs-a"}} {
		got, err := cached.Invoke(context.Background(), request("same"), withTier(item.tier))
		if err != nil || got.Messages[0].JoinedText("") != item.want {
			t.Fatalf("tier=%s response=%#v err=%v", item.tier, got, err)
		}
	}
	if upstream.Calls() != 2 {
		t.Fatalf("calls=%d, want 2 (GoStringer must not collapse the key)", upstream.Calls())
	}
}

// 接口字段中底层类型相同的命名类型不能塌缩为同一指纹：any 持有的
// celsius(5) 与 float64(5) 必须产生不同缓存键。
func TestCachedModelSeparatesInterfaceConcreteTypes(t *testing.T) {
	upstream := &extConfigModel{}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "any"})
	withExtra := func(v any) runnable.Option {
		return runnable.WrapOption(func(c *anyConfig) { c.Extra = v })
	}
	for _, item := range []struct {
		value any
		want  string
	}{{celsius(5), "tier:none-anyllm_test.celsius"}, {float64(5), "tier:none-anyfloat64"}, {celsius(5), "tier:none-anyllm_test.celsius"}} {
		got, err := cached.Invoke(context.Background(), request("same"), withExtra(item.value))
		if err != nil || got.Messages[0].JoinedText("") != item.want {
			t.Fatalf("value=%v(%T) response=%#v err=%v", item.value, item.value, got, err)
		}
	}
	if upstream.Calls() != 2 {
		t.Fatalf("calls=%d, want 2 (named types sharing a Kind must not collide)", upstream.Calls())
	}
}

// 无法稳定编码的扩展配置不参与缓存身份：绕过缓存而不是命中错误结果。
func TestCachedModelBypassesUnencodableExtensionConfig(t *testing.T) {
	upstream := &extConfigModel{}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "opaque"})
	opaqueOpt := runnable.WrapOption(func(c *opaqueConfig) { c.Hooks = func() {} })
	for range 2 {
		if _, err := cached.Invoke(context.Background(), request("same"), opaqueOpt); err != nil {
			t.Fatal(err)
		}
	}
	if upstream.Calls() != 2 {
		t.Fatalf("calls=%d, want 2 (unencodable config must bypass)", upstream.Calls())
	}
	if cached.Stats().Bypasses != 2 {
		t.Fatalf("stats=%#v", cached.Stats())
	}
}

type testModel struct {
	response      llm.Response
	err           error
	empty         bool
	release       <-chan struct{}
	waitForCancel chan<- struct{}
	calls         atomic.Int64
}

func (m *testModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	reader, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.CollectResponse(ctx, reader)
}

func (m *testModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	m.calls.Add(1)
	reader, writer := runnable.Pipe[llm.Response](context.WithoutCancel(ctx))
	go func() {
		if m.waitForCancel != nil {
			<-ctx.Done()
			close(m.waitForCancel)
			_ = writer.CloseWithError(ctx.Err())
			return
		}
		if m.release != nil {
			select {
			case <-ctx.Done():
				_ = writer.CloseWithError(ctx.Err())
				return
			case <-m.release:
			}
		}
		if m.err != nil {
			_ = writer.CloseWithError(m.err)
			return
		}
		if m.empty {
			_ = writer.Close()
			return
		}
		if err := writer.Send(m.response.Clone()); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}

func (m *testModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}
func (m *testModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}
func (*testModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true}
}
func (*testModel) ModelInfo() llm.ModelInfo { return llm.ModelInfo{Name: "test"} }
func (m *testModel) Calls() int             { return int(m.calls.Load()) }

func response(text string) llm.Response {
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage(text)}}
}

func request(text string) llm.Request {
	return llm.Request{Messages: []schema.Message{schema.UserMessage(text)}}
}

func newMemoryCachedModel(t *testing.T, upstream llm.ChatModel, cfg cachellm.CacheConfig) *cachellm.CachedModel {
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

func mustSet(t *testing.T, store cachellm.CacheStore, key, text string, ttl time.Duration) {
	t.Helper()
	if err := store.Set(context.Background(), key, cachellm.CacheEntry{Response: response(text), CreatedAt: time.Now().UTC()}, ttl); err != nil {
		t.Fatal(err)
	}
}

func waitForCalls(t *testing.T, model *testModel, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for model.Calls() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if model.Calls() != want {
		t.Fatalf("calls=%d, want %d", model.Calls(), want)
	}
}

func waitForShared(t *testing.T, model *cachellm.CachedModel, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for model.Stats().Shared != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if model.Stats().Shared != want {
		t.Fatalf("shared=%d, want %d", model.Stats().Shared, want)
	}
}

// ResponseFormat 进入缓存身份：同一份 Schema/名称/Strict 命中，任一变化
// 产生不同键；调用方修改格式副本不污染已冻结的键。
func TestCachedModelResponseFormatIdentity(t *testing.T) {
	upstream := &testModel{response: response("answer")}
	cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "fmt"})
	format := func(name, schemaJSON string, strict bool) *llm.ResponseFormat {
		return &llm.ResponseFormat{Name: name, Schema: json.RawMessage(schemaJSON), Strict: strict}
	}
	schemaA := `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"],"additionalProperties":false}`
	schemaB := `{"type":"object","properties":{"b":{"type":"string"}},"required":["b"],"additionalProperties":false}`
	for index, item := range []struct {
		format    *llm.ResponseFormat
		wantCalls int
	}{
		{format("d", schemaA, true), 1},  // 首次未命中
		{format("d", schemaA, true), 1},  // 同格式命中
		{format("d", schemaB, true), 2},  // Schema 不同不命中
		{format("e", schemaA, true), 3},  // 名称不同不命中
		{format("d", schemaA, false), 4}, // Strict 不同不命中
		{format("d", schemaA, true), 4},  // 回到首组仍命中（不新增调用）
	} {
		if _, err := cached.Invoke(context.Background(), request("same"),
			llm.WithResponseFormat(item.format)); err != nil {
			t.Fatalf("case %d err=%v", index, err)
		}
		if upstream.Calls() != item.wantCalls {
			t.Fatalf("case %d calls=%d, want %d", index, upstream.Calls(), item.wantCalls)
		}
	}
	// 选项应用后修改调用方的格式副本：已冻结身份不变，仍命中第一组。
	mutable := format("d", schemaA, true)
	if _, err := cached.Invoke(context.Background(), request("same"),
		llm.WithResponseFormat(mutable)); err != nil {
		t.Fatal(err)
	}
	mutable.Name = "mutated"
	mutable.Schema[0] = ' '
	if _, err := cached.Invoke(context.Background(), request("same"),
		llm.WithResponseFormat(format("d", schemaA, true))); err != nil {
		t.Fatal(err)
	}
	if upstream.Calls() != 4 {
		t.Fatalf("calls=%d, want 4 (cached identity unaffected by caller mutation)", upstream.Calls())
	}
	// 清除类型化格式的调用使用另一条身份，不命中带格式组。
	if _, err := cached.Invoke(context.Background(), request("same")); err != nil {
		t.Fatal(err)
	}
	if upstream.Calls() != 5 {
		t.Fatalf("calls=%d, want 5 (plain request is a distinct identity)", upstream.Calls())
	}
}
