package state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"goharness/pkg/schema"
)

func TestMemoryStoreVersionsClonesAndRejectsStaleWrites(t *testing.T) {
	store := NewMemoryStore(MemoryStoreConfig{})
	message := schema.UserMessage("hello")
	message = schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("call-1", "lookup", false, schema.NewTextBlock("value"), schema.NewOpaqueBlock("provider.citation", json.RawMessage(`{"page":3}`))))
	message.ResponseMeta = &schema.ResponseMeta{ID: "resp-1", Usage: &schema.TokenUsage{CacheReadTokens: 32}}
	message.Extra = map[string]any{"nested": map[string]any{"value": "message-original"}}
	created, err := store.Save(context.Background(), Checkpoint{
		ConversationID: "conversation-1",
		Messages:       []schema.Message{message},
		Values:         map[string]any{"nested": map[string]any{"value": "original"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Version != 1 || created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatalf("created checkpoint = %#v", created)
	}
	created.Messages[0].ContentBlocks[0] = schema.NewTextBlock("changed")
	created.Messages[0].Extra["nested"].(map[string]any)["value"] = "message-changed"
	created.Values["nested"].(map[string]any)["value"] = "changed"
	loaded, found, err := store.Load(context.Background(), "conversation-1")
	if err != nil || !found {
		t.Fatalf("Load found=%t err=%v", found, err)
	}
	results := loaded.Messages[0].FunctionToolResults()
	if len(results) != 1 || results[0].CallID != "call-1" || results[0].ContentBlocks[1].Opaque == nil || string(results[0].ContentBlocks[1].Opaque.Raw) != `{"page":3}` || loaded.Messages[0].ResponseMeta.Usage.CacheReadTokens != 32 || loaded.Messages[0].Extra["nested"].(map[string]any)["value"] != "message-original" || loaded.Values["nested"].(map[string]any)["value"] != "original" {
		t.Fatalf("stored checkpoint was mutated: %#v", loaded)
	}

	stale := loaded.Clone()
	updated := loaded.Clone()
	updated.Messages = append(updated.Messages, schema.AssistantMessage("answer"))
	updated, err = store.Save(context.Background(), updated)
	if err != nil || updated.Version != 2 {
		t.Fatalf("updated checkpoint = %#v, err=%v", updated, err)
	}
	_, err = store.Save(context.Background(), stale)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Save error = %v", err)
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Expected != 1 || conflict.Actual != 2 {
		t.Fatalf("conflict = %#v", conflict)
	}
}

func TestMemoryStoreExpiresCheckpoint(t *testing.T) {
	now := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore(MemoryStoreConfig{TTL: time.Minute})
	store.now = func() time.Time { return now }
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "expiring"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if _, found, err := store.Load(context.Background(), "expiring"); err != nil || found {
		t.Fatalf("expired Load found=%t err=%v", found, err)
	}
}

func TestCheckpointContextReturnsClonedValues(t *testing.T) {
	ctx := WithCheckpoint(context.Background(), Checkpoint{
		ConversationID: "context-conversation",
		Version:        3,
		Values:         map[string]any{"nested": map[string]any{"value": "original"}},
	})
	checkpoint, ok := FromContext(ctx)
	if !ok || checkpoint.Version != 3 {
		t.Fatalf("checkpoint = %#v, ok=%t", checkpoint, ok)
	}
	checkpoint.Values["nested"].(map[string]any)["value"] = "changed"
	if ValuesFromContext(ctx)["nested"].(map[string]any)["value"] != "original" {
		t.Fatal("checkpoint context values were mutated")
	}
}
