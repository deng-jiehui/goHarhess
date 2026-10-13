package redisstore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func textMessage(text string) schema.Message {
	return schema.NewMessage(schema.RoleUser, schema.NewTextBlock(text))
}

// LogCacheSize 上限：保存超过上限的会话后条目数不超过上限且记录淘汰数。
func TestRedisLogCacheLRUEvictsBeyondCap(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: "t", LogCacheSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := store.Save(ctx, state.Checkpoint{ConversationID: fmt.Sprintf("c%d", i), Messages: []schema.Message{textMessage("m")}}); err != nil {
			t.Fatal(err)
		}
	}
	if got := store.LogCacheEntries(); got != 2 {
		t.Fatalf("entries = %d, want 2", got)
	}
	if got := store.LogCacheEvictions(); got != 3 {
		t.Fatalf("evictions = %d, want 3", got)
	}
	// 被淘汰的会话仍能正确读写：缓存只是乐观猜测，淘汰只影响流量。
	prev, found, err := store.Load(ctx, "c0")
	if err != nil || !found {
		t.Fatalf("load c0: found=%v err=%v", found, err)
	}
	messages := append(append([]schema.Message(nil), prev.Messages...), textMessage("m2"))
	if _, err := store.Save(ctx, state.Checkpoint{ConversationID: "c0", ID: prev.ID, Version: prev.Version, Messages: messages}); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(ctx, "c0")
	if err != nil || !found || len(loaded.Messages) != 2 {
		t.Fatalf("loaded = %+v found=%v err=%v", loaded, found, err)
	}
}

// 视图命中（头部未变）时脚本信任 claimed：先验证连续追加的正确性，
// 再验证信任路径下消息分叉仍按代次复制前缀。
func TestRedisTrustedViewStillForksOnDivergence(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: "t"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base, err := store.Save(ctx, state.Checkpoint{ConversationID: "c", Messages: []schema.Message{textMessage("a"), textMessage("b"), textMessage("c")}})
	if err != nil {
		t.Fatal(err)
	}
	// 第二次保存：视图命中且头部未变，走信任 claimed 路径。
	appended, err := store.Save(ctx, state.Checkpoint{ConversationID: "c", ID: base.ID, Version: base.Version, Messages: []schema.Message{textMessage("a"), textMessage("b"), textMessage("c"), textMessage("d")}})
	if err != nil {
		t.Fatal(err)
	}
	// 分叉：同一父记录下消息前缀改写 —— 信任路径给出的 claimed=1 < nhashes，
	// 脚本必须按既有规则复制共享前缀进新代次。
	forked, err := store.Save(ctx, state.Checkpoint{ConversationID: "c", ID: appended.ID, Version: appended.Version, Messages: []schema.Message{textMessage("a"), textMessage("x")}})
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(ctx, "c")
	if err != nil || !found {
		t.Fatal(err)
	}
	if len(loaded.Messages) != 2 || loaded.Messages[1].JoinedText("") != "x" {
		t.Fatalf("forked messages = %+v", loaded.Messages)
	}
	// 历史记录仍可取回（旧代次日志保留）。
	history, err := store.List(ctx, "c", state.ListOptions{})
	if err != nil || len(history) != 3 {
		t.Fatalf("history = %d err=%v", len(history), err)
	}
	if len(history[1].Messages) != 4 || history[1].Messages[3].JoinedText("") != "d" {
		t.Fatalf("appended record messages = %+v", history[1].Messages)
	}
	_ = forked
}

// 删除会话会同时清掉视图：删除后重建不会误信任旧视图。
func TestRedisDeleteDropsView(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: "t"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Save(ctx, state.Checkpoint{ConversationID: "c", Messages: []schema.Message{textMessage("a")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	if got := store.LogCacheEntries(); got != 0 {
		t.Fatalf("entries = %d after delete", got)
	}
	if _, err := store.Save(ctx, state.Checkpoint{ConversationID: "c", Messages: []schema.Message{textMessage("new")}}); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(ctx, "c")
	if err != nil || !found || len(loaded.Messages) != 1 || loaded.Messages[0].JoinedText("") != "new" {
		t.Fatalf("loaded = %+v found=%v err=%v", loaded, found, err)
	}
}

// 并发保存与删除在小容量缓存下不触发数据竞争（-race 下运行）。
func TestRedisLogCacheConcurrentUse(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: "t", LogCacheSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("c%d", i%6)
			for j := 0; j < 4; j++ {
				loaded, _, _ := store.Load(ctx, id)
				var messages []schema.Message
				var parent string
				var version uint64
				if loaded.ID != "" {
					messages = loaded.Messages
					parent = loaded.ID
					version = loaded.Version
				}
				messages = append(messages, textMessage(fmt.Sprintf("m%d-%d", i, j)))
				_, _ = store.Save(ctx, state.Checkpoint{ConversationID: id, ID: parent, Version: version, Messages: messages})
				if j == 2 {
					_ = store.Delete(ctx, id)
				}
			}
		}(i)
	}
	wg.Wait()
	if got := store.LogCacheEntries(); got > 4 {
		t.Fatalf("entries = %d exceeds cap", got)
	}
}
