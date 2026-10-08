package redisstore_test

import (
	"context"
	"encoding/base64"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func newTestStore(t *testing.T, config redisstore.RedisStoreConfig) (*redisstore.RedisStore, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	config.Client = client
	store, err := redisstore.NewRedisStore(config)
	if err != nil {
		t.Fatal(err)
	}
	return store, client
}

// storeLLen 读取指定会话某代次消息日志的 LIST 长度。
func storeLLen(t *testing.T, ctx context.Context, client *redis.Client, conversationID string, epoch int) int64 {
	t.Helper()
	n, err := client.LLen(ctx, "goharness:checkpoint:{"+
		base64.RawURLEncoding.EncodeToString([]byte(conversationID))+"}:msgs:"+strconv.Itoa(epoch)).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// MaxHistory 淘汰运行前记录后，放弃运行仍须回退到运行开始前的会话
// （P1：回退依据独立于可裁剪历史保留）。
func TestRedisAbandonAfterHistoryTrim(t *testing.T) {
	store, _ := newTestStore(t, redisstore.RedisStoreConfig{MaxHistory: 2})
	ctx := context.Background()
	prev, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "abandon", RunID: "run-a", Status: state.StatusCompleted,
		Messages: []schema.Message{schema.UserMessage("old")},
		Values:   map[string]any{"k": "v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	last := prev
	for i := 0; i < 3; i++ {
		last, err = store.Save(ctx, state.Checkpoint{
			ConversationID: "abandon", RunID: "run-b", Status: state.StatusInterrupted,
			Version: last.Version, ID: last.ID,
			Messages: append(append([]schema.Message{}, prev.Messages...), schema.UserMessage("new")),
			Values:   map[string]any{"k": "v2"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	abandoned, err := state.AbandonRun(ctx, store, "abandon")
	if err != nil {
		t.Fatalf("AbandonRun err=%v", err)
	}
	if abandoned.Status != state.StatusAbandoned || len(abandoned.Messages) != 1 ||
		abandoned.Messages[0].JoinedText("") != "old" || abandoned.Values["k"] != "v1" {
		t.Fatalf("abandoned=%#v", abandoned)
	}
}

// 零共享前缀的分叉不得复制旧日志：LRANGE 0,-1 会误取整份旧日志，
// 新代次应只含新消息（P2）。
func TestRedisForkZeroSharedPrefix(t *testing.T) {
	store, client := newTestStore(t, redisstore.RedisStoreConfig{})
	ctx := context.Background()
	v1, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "zfork",
		Messages:       []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 两条消息全部改写：与旧日志共享前缀为零。
	v2, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "zfork", Version: v1.Version, ID: v1.ID,
		Messages: []schema.Message{schema.UserMessage("x1"), schema.AssistantMessage("y1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := storeLLen(t, ctx, client, "zfork", 2); n != 2 {
		t.Fatalf("new epoch log len=%d, want 2 (old log must not be copied)", n)
	}
	got, found, _ := store.Get(ctx, "zfork", v2.ID)
	if !found || len(got.Messages) != 2 || got.Messages[0].JoinedText("") != "x1" {
		t.Fatalf("v2 materialized %#v", got.Messages)
	}
	prev, found, _ := store.Get(ctx, "zfork", v1.ID)
	if !found || len(prev.Messages) != 2 || prev.Messages[0].JoinedText("") != "q1" {
		t.Fatalf("v1 materialized %#v", prev.Messages)
	}
}

// 截断历史到前缀（不新增）不产生分叉，随后改写应开新代次：
// 分叉判断须基于 LLEN 的真实日志长度，而非截断后的读取窗口。
func TestRedisTruncateThenRewriteForks(t *testing.T) {
	store, client := newTestStore(t, redisstore.RedisStoreConfig{})
	ctx := context.Background()
	v1, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "trunc",
		Messages: []schema.Message{
			schema.UserMessage("m1"), schema.UserMessage("m2"), schema.UserMessage("m3"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 截断到前两条：共享前缀与日志前缀一致，不产生分叉。
	v2, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "trunc", Version: v1.Version, ID: v1.ID,
		Messages: []schema.Message{schema.UserMessage("m1"), schema.UserMessage("m2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 从截断处改写并追加：旧日志第 3 条仍在，必须分叉。
	v3, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "trunc", Version: v2.Version, ID: v2.ID,
		Messages: []schema.Message{schema.UserMessage("m1"), schema.UserMessage("m2"), schema.UserMessage("edited")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := storeLLen(t, ctx, client, "trunc", 2); n != 3 {
		t.Fatalf("epoch2 log len=%d, want 3", n)
	}
	got, found, _ := store.Get(ctx, "trunc", v3.ID)
	if !found || len(got.Messages) != 3 || got.Messages[2].JoinedText("") != "edited" {
		t.Fatalf("v3 materialized %#v", got.Messages)
	}
}

// 保存与删除并发：缓存访问与网络调用分离，跨会话操作不共享状态（P1）。
func TestRedisConcurrentSaveAndDelete(t *testing.T) {
	store, _ := newTestStore(t, redisstore.RedisStoreConfig{})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func(id string) {
			defer wg.Done()
			var last state.Checkpoint
			for j := 0; j < 10; j++ {
				saved, err := store.Save(ctx, state.Checkpoint{
					ConversationID: id, Version: last.Version, ID: last.ID,
					Messages: []schema.Message{schema.UserMessage("m")},
				})
				if err != nil {
					return
				}
				last = saved
			}
		}("c" + string(rune('a'+i)))
		go func(id string) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = store.Delete(ctx, id)
			}
		}("d" + string(rune('a'+i)))
	}
	wg.Wait()
}

// 缓存失配且真实前缀为零时，保存不得返回假成功（P1：此前把 {2,0}
// 重试响应当作成功，返回新版本而实际未写入）。
func TestRedisStalePrefixCacheDoesNotFakeSuccess(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	a, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	b, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	// 实例 A 写入消息并缓存前缀；实例 B 改写会话内容。
	if _, err := a.Save(ctx, state.Checkpoint{
		ConversationID: "stale", Messages: []schema.Message{schema.UserMessage("from-a")},
	}); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err := b.Load(ctx, "stale")
	if err != nil || !found {
		t.Fatalf("b load found=%v err=%v", found, err)
	}
	v2, err := b.Save(ctx, state.Checkpoint{
		ConversationID: "stale", Version: loaded.Version, ID: loaded.ID,
		Messages: []schema.Message{schema.UserMessage("from-b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A 基于最新版本提交与旧缓存匹配的消息：claimed=1，真实前缀为 0。
	v3, err := a.Save(ctx, state.Checkpoint{
		ConversationID: "stale", Version: v2.Version, ID: v2.ID,
		Messages: []schema.Message{schema.UserMessage("from-a"), schema.UserMessage("again-a")},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := a.Load(ctx, "stale")
	if err != nil || !found {
		t.Fatalf("reload found=%v err=%v", found, err)
	}
	if got.ID != v3.ID || got.Version != v3.Version || len(got.Messages) != 2 ||
		got.Messages[0].JoinedText("") != "from-a" {
		t.Fatalf("reloaded %#v, want committed v3 with 2 messages", got)
	}
	// B 的改写已开代次 2，A 的零前缀分叉再开代次 3，只含本次消息。
	if n := storeLLen(t, ctx, client, "stale", 3); n != 2 {
		t.Fatalf("epoch3 log len=%d, want 2", n)
	}
}

// 会话 TTL 到期后用相同消息前缀重建：缓存失配前缀为零时保存必须
// 真正提交，随后能读到完整会话。
func TestRedisRebuildAfterTTLExpiry(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "ttl", Messages: []schema.Message{schema.UserMessage("q")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "ttl", Version: v1.Version, ID: v1.ID,
		Messages: []schema.Message{schema.UserMessage("q"), schema.UserMessage("more")},
	}); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Minute)
	// 本地缓存仍记着 2 条前缀，服务端数据已过期：真实前缀为零。
	rebuilt, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "ttl", Messages: []schema.Message{schema.UserMessage("q"), schema.UserMessage("r")},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Load(ctx, "ttl")
	if err != nil || !found {
		t.Fatalf("reload found=%v err=%v", found, err)
	}
	if got.ID != rebuilt.ID || len(got.Messages) != 2 || got.Messages[1].JoinedText("") != "r" {
		t.Fatalf("rebuilt record %#v", got)
	}
}

// 另一实例删除会话后，原实例用相同消息前缀重建：同样不得假成功。
func TestRedisRebuildAfterForeignDelete(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	a, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	b, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := a.Save(ctx, state.Checkpoint{
		ConversationID: "gone", Messages: []schema.Message{schema.UserMessage("q")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Save(ctx, state.Checkpoint{
		ConversationID: "gone", Version: v1.Version, ID: v1.ID,
		Messages: []schema.Message{schema.UserMessage("q"), schema.UserMessage("more")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := a.Save(ctx, state.Checkpoint{
		ConversationID: "gone", Messages: []schema.Message{schema.UserMessage("q"), schema.UserMessage("r")},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := a.Load(ctx, "gone")
	if err != nil || !found {
		t.Fatalf("reload found=%v err=%v", found, err)
	}
	if got.ID != rebuilt.ID || len(got.Messages) != 2 {
		t.Fatalf("rebuilt record %#v", got)
	}
}

// 非零失配重试：缓存前缀长于真实前缀（非零），收缩后重试须提交
// 完整会话且记录指向新代次。
func TestRedisNonzeroPrefixMismatchRetries(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	a, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	b, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Save(ctx, state.Checkpoint{
		ConversationID: "partial",
		Messages:       []schema.Message{schema.UserMessage("same"), schema.UserMessage("a-only")},
	}); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err := b.Load(ctx, "partial")
	if err != nil || !found {
		t.Fatalf("b load found=%v err=%v", found, err)
	}
	// B 把第 2 条改写成别的内容：真实共享前缀缩到 1。
	v2, err := b.Save(ctx, state.Checkpoint{
		ConversationID: "partial", Version: loaded.Version, ID: loaded.ID,
		Messages: []schema.Message{schema.UserMessage("same"), schema.UserMessage("b-only")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A 的缓存记着 2 条前缀；提交 3 条，真实前缀只有 1。
	v3, err := a.Save(ctx, state.Checkpoint{
		ConversationID: "partial", Version: v2.Version, ID: v2.ID,
		Messages: []schema.Message{
			schema.UserMessage("same"), schema.UserMessage("a-only"), schema.UserMessage("a-tail"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := a.Load(ctx, "partial")
	if err != nil || !found {
		t.Fatalf("reload found=%v err=%v", found, err)
	}
	if got.ID != v3.ID || len(got.Messages) != 3 || got.Messages[1].JoinedText("") != "a-only" ||
		got.Messages[2].JoinedText("") != "a-tail" {
		t.Fatalf("reloaded %#v", got.Messages)
	}
}
