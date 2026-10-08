package redisstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func TestRedisStoreRoundTripCASAndTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: "test", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Save(context.Background(), state.Checkpoint{
		ConversationID: "tenant/a conversation",
		Messages: []schema.Message{func() schema.Message {
			m := schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("call-1", "lookup", false, schema.NewTextBlock("value"), schema.NewOpaqueBlock("provider.citation", json.RawMessage(`{"page":3}`))))
			m.ResponseMeta = &schema.ResponseMeta{ID: "resp-1", Usage: &schema.TokenUsage{CacheReadTokens: 32}}
			return m
		}()},
		Values: map[string]any{"tenant": "a"},
	})
	if err != nil || created.Version != 1 {
		t.Fatalf("created = %#v, err=%v", created, err)
	}
	loaded, found, err := store.Load(context.Background(), created.ConversationID)
	results := loaded.Messages[0].FunctionToolResults()
	if err != nil || !found || loaded.Version != 1 || len(results) != 1 || results[0].CallID != "call-1" || string(results[0].ContentBlocks[1].Opaque.Raw) != `{"page":3}` || loaded.Messages[0].ResponseMeta.Usage.CacheReadTokens != 32 {
		t.Fatalf("loaded = %#v, found=%t, err=%v", loaded, found, err)
	}
	updated := loaded.Clone()
	updated.Messages = append(updated.Messages, schema.AssistantMessage("answer"))
	if _, err := store.Save(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(context.Background(), loaded); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("stale Save error = %v", err)
	}
	server.FastForward(time.Minute)
	if _, found, err := store.Load(context.Background(), created.ConversationID); err != nil || found {
		t.Fatalf("expired Load found=%t err=%v", found, err)
	}
}

func TestRedisHistorySurvivesNewStoreAndRejectsPartialWrites(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Save(ctx, state.Checkpoint{ConversationID: "history", Execution: json.RawMessage(`{"n":9007199254740993}`)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Save(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(ctx, first); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("conflict=%v", err)
	}
	other, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	records, err := other.List(ctx, "history", state.ListOptions{Limit: 1})
	if err != nil || len(records) != 1 || records[0].ID != second.ID {
		t.Fatalf("history=%+v %v", records, err)
	}
	records, err = other.List(ctx, "history", state.ListOptions{Before: second.ID})
	if err != nil || len(records) != 1 || records[0].ID != first.ID {
		t.Fatalf("older=%+v %v", records, err)
	}
	old, found, err := other.Get(ctx, "history", first.ID)
	if err != nil || !found || string(old.Execution) != `{"n":9007199254740993}` {
		t.Fatalf("historical JSON=%s %v", old.Execution, err)
	}
	if err := other.Delete(ctx, "history"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, "history", first.ID); err != nil || found {
		t.Fatalf("delete=%t %v", found, err)
	}
}

func TestStoreConcurrentWritersShareOneHistory(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	remote, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	for name, store := range map[string]state.Store{"memory": state.NewMemoryStore(), "redis": remote} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base, err := store.Save(ctx, state.Checkpoint{ConversationID: "race"})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			results := make(chan error, 8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, err := store.Save(ctx, base); results <- err }()
			}
			wg.Wait()
			close(results)
			success := 0
			conflicts := 0
			for err := range results {
				if err == nil {
					success++
				} else if errors.Is(err, state.ErrConflict) {
					conflicts++
				} else {
					t.Fatal(err)
				}
			}
			history, err := store.List(ctx, "race", state.ListOptions{})
			if err != nil || success != 1 || conflicts != 7 || len(history) != 2 {
				t.Fatalf("success=%d conflicts=%d history=%d err=%v", success, conflicts, len(history), err)
			}
			if err := store.Delete(ctx, "race"); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Save(ctx, state.Checkpoint{ConversationID: "race"}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Save(ctx, base); !errors.Is(err, state.ErrConflict) {
				t.Fatalf("stale generation accepted: %v", err)
			}
		})
	}
}

// 消息日志去重：连续保存只追加增量，记录体内不重复完整会话。
func TestRedisMessageLogDeduplicates(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	base := []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")}
	v1, err := store.Save(ctx, state.Checkpoint{ConversationID: "dedup", Messages: base})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "dedup", Version: v1.Version, ID: v1.ID,
		Messages: append(append([]schema.Message{}, base...), schema.UserMessage("q2")),
	})
	if err != nil {
		t.Fatal(err)
	}
	key := "goharness:checkpoint:{ZGVkdXA}"
	raw, err := client.HGet(ctx, key, "record:"+v2.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	if stored["messages_from"] != float64(2) || len(stored["messages"].([]any)) != 1 {
		t.Fatalf("stored delta from=%v len=%d", stored["messages_from"], len(stored["messages"].([]any)))
	}
	if n := client.LLen(ctx, key+":msgs:1").Val(); n != 3 {
		t.Fatalf("message log len=%d, want 3", n)
	}
	got, found, err := store.Get(ctx, "dedup", v2.ID)
	if err != nil || !found || len(got.Messages) != 3 || got.MessagesFrom != 0 {
		t.Fatalf("materialized %d messages from=%d", len(got.Messages), got.MessagesFrom)
	}
}

// 消息日志分叉：改写历史后沿同一版本链保存开启新代次，新旧记录
// 分别物化出各自会话。
func TestRedisMessageLogFork(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "fork",
		Messages:       []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	diverged := []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("edited"), schema.UserMessage("q2")}
	v2, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "fork", Version: v1.Version, ID: v1.ID, Messages: diverged,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := "goharness:checkpoint:{Zm9yaw}"
	if epoch := client.HGet(ctx, key, "epoch").Val(); epoch != "2" {
		t.Fatalf("epoch=%s, want 2", epoch)
	}
	got, found, _ := store.Get(ctx, "fork", v2.ID)
	if !found || len(got.Messages) != 3 || got.Messages[1].JoinedText("") != "edited" {
		t.Fatalf("v2 materialized %#v", got.Messages)
	}
	prev, found, _ := store.Get(ctx, "fork", v1.ID)
	if !found || len(prev.Messages) != 2 || prev.Messages[1].JoinedText("") != "a1" {
		t.Fatalf("v1 materialized %#v", prev.Messages)
	}
	// 新代次上继续追加不分叉。
	v3, err := store.Save(ctx, state.Checkpoint{
		ConversationID: "fork", Version: v2.Version, ID: v2.ID,
		Messages: append(append([]schema.Message{}, diverged...), schema.AssistantMessage("a2")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if epoch := client.HGet(ctx, key, "epoch").Val(); epoch != "2" {
		t.Fatalf("epoch=%s after append, want 2", epoch)
	}
	latest, _, _ := store.Load(ctx, "fork")
	if len(latest.Messages) != 4 || latest.ID != v3.ID {
		t.Fatalf("latest %d messages", len(latest.Messages))
	}
}

// MaxHistory：超出上限的历史记录按保存顺序淘汰，最新记录与物化不受影响。
func TestRedisMaxHistory(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, MaxHistory: 2})
	if err != nil {
		t.Fatal(err)
	}
	messages := []schema.Message{schema.UserMessage("m0")}
	var last state.Checkpoint
	var ids []string
	for i := 1; i <= 4; i++ {
		messages = append(append([]schema.Message{}, messages...), schema.UserMessage("m"))
		saved, err := store.Save(ctx, state.Checkpoint{
			ConversationID: "retain", Version: last.Version, ID: last.ID, Messages: messages,
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, saved.ID)
		last = saved
	}
	if _, found, err := store.Get(ctx, "retain", ids[0]); err != nil || found {
		t.Fatalf("evicted record found=%v err=%v", found, err)
	}
	got, found, err := store.Get(ctx, "retain", ids[2])
	if err != nil || !found || len(got.Messages) != 4 {
		t.Fatalf("kept record found=%v messages=%d", found, len(got.Messages))
	}
	list, err := store.List(ctx, "retain", state.ListOptions{})
	if err != nil || len(list) != 2 {
		t.Fatalf("List len=%d err=%v, want 2", len(list), err)
	}
}

func TestRedisStoreCarriesSubAgentsAcrossRunsAndRollback(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: "sub"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.Save(ctx, state.Checkpoint{ConversationID: "c", RunID: "r1", Status: state.StatusCompleted, SubAgents: json.RawMessage(`{"format":1,"agents":[]}`)})
	if err != nil {
		t.Fatal(err)
	}
	next := first.Clone()
	next.RunID, next.Status = "r2", state.StatusRunning
	next.SubAgents = json.RawMessage(`{"format":1,"agents":[{"id":"x"}]}`)
	if _, err := store.Save(ctx, next); err != nil {
		t.Fatal(err)
	}
	// 另起存储实例读取：集合随记录持久化。
	other, _ := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: "sub"})
	loaded, found, err := other.Load(ctx, "c")
	if err != nil || !found || string(loaded.SubAgents) != `{"format":1,"agents":[{"id":"x"}]}` {
		t.Fatalf("loaded sub agents = %s found=%t err=%v", loaded.SubAgents, found, err)
	}
	// 放弃运行回退到运行开始前的集合。
	rolled, err := state.AbandonRun(ctx, other, "c")
	if err != nil || string(rolled.SubAgents) != `{"format":1,"agents":[]}` {
		t.Fatalf("rolled back sub agents = %s err=%v", rolled.SubAgents, err)
	}
}
