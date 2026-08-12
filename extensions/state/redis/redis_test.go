package redisstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "goharness/extensions/state/redis"
	"goharness/pkg/schema"
	"goharness/pkg/state"
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
		Messages:       []schema.Message{schema.UserMessage("hello")},
		Values:         map[string]any{"tenant": "a"},
	})
	if err != nil || created.Version != 1 {
		t.Fatalf("created = %#v, err=%v", created, err)
	}
	loaded, found, err := store.Load(context.Background(), created.ConversationID)
	if err != nil || !found || loaded.Version != 1 || loaded.Messages[0].Text() != "hello" {
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
