package redisstore_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// Redis 存储沿父链回退会话内容，放弃记录与原运行一起留在历史中。
func TestRedisAbandonRunRevertsConversation(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := store.Save(ctx, state.Checkpoint{ConversationID: "thread", RunID: "run-a", Status: state.StatusCompleted,
		Messages: []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")}})
	if err != nil {
		t.Fatal(err)
	}
	running := completed.Clone()
	running.RunID, running.Status = "run-b", state.StatusRunning
	running.Messages = append(running.Messages, schema.UserMessage("q2"))
	if _, err := store.Save(ctx, running); err != nil {
		t.Fatal(err)
	}
	abandoned, err := state.AbandonRun(ctx, store, "thread")
	if err != nil || abandoned.Status != state.StatusAbandoned || len(abandoned.Messages) != 2 {
		t.Fatalf("abandoned=%+v err=%v", abandoned, err)
	}
	history, err := store.List(ctx, "thread", state.ListOptions{})
	if err != nil || len(history) != 3 {
		t.Fatalf("history=%d err=%v", len(history), err)
	}
}
