package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"goharness/pkg/agent"
	"goharness/pkg/contextengine"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

func TestConversationCheckpointPreservesCallerSystemMessage(t *testing.T) {
	store := state.NewMemoryStore()
	var calls [][]schema.Message
	model := llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
		calls = append(calls, schema.CloneMessages(messages))
		return []schema.Message{schema.AssistantMessage("answer")}, nil
	})
	value, err := agent.NewBasicAgent(agent.BaseConfig{SystemMessage: "agent policy", Model: model, CheckpointStore: store})
	if err != nil {
		t.Fatal(err)
	}
	first := []schema.Message{schema.SystemMessage("caller policy"), schema.UserMessage("first")}
	if _, err := value.Invoke(context.Background(), first, agent.WithConversationID("caller-system")); err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("second")}, agent.WithConversationID("caller-system")); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1][0].Role != schema.RoleSystem || calls[1][0].Text() != "agent policy\n\ncaller policy" {
		t.Fatalf("second model context=%#v", calls)
	}
	checkpoint, found, err := store.Load(context.Background(), "caller-system")
	if err != nil || !found {
		t.Fatalf("found=%t err=%v", found, err)
	}
	if len(checkpoint.Messages) == 0 || checkpoint.Messages[0].Role != schema.RoleSystem || checkpoint.Messages[0].Text() != "caller policy" {
		t.Fatalf("checkpoint messages=%#v", checkpoint.Messages)
	}
}

type blockingCheckpointStore struct{}

func (blockingCheckpointStore) Load(context.Context, string) (state.Checkpoint, bool, error) {
	return state.Checkpoint{}, false, nil
}

func (blockingCheckpointStore) Save(ctx context.Context, _ state.Checkpoint) (state.Checkpoint, error) {
	<-ctx.Done()
	return state.Checkpoint{}, ctx.Err()
}

func (blockingCheckpointStore) Delete(context.Context, string) error { return nil }

func TestAgentLoadsAssemblesAndSavesConversationCheckpoint(t *testing.T) {
	store := state.NewMemoryStore(state.MemoryStoreConfig{})
	assembler, err := contextengine.New(contextengine.Config{
		MaxContextTokens:     1000,
		ReservedOutputTokens: 100,
		PromptCache: contextengine.PromptCacheConfig{
			Enabled:         true,
			MinPrefixTokens: 0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var calls [][]schema.Message
	model := llm.NewHandlerModel(func(ctx context.Context, messages []schema.Message, opts llm.ChatOptions) ([]schema.Message, error) {
		mu.Lock()
		calls = append(calls, schema.CloneMessages(messages))
		call := len(calls)
		mu.Unlock()
		values := state.ValuesFromContext(ctx)
		if values["tenant"] != "tenant-a" {
			t.Fatalf("state values in model context = %#v", values)
		}
		if call == 2 && values["turn"] != 2 && values["turn"] != 2.0 {
			t.Fatalf("second-run state values = %#v", values)
		}
		return []schema.Message{schema.AssistantMessage(fmt.Sprintf("answer-%d", call))}, nil
	})
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:            "persistent",
		SystemMessage:   "stable policy",
		Model:           model,
		ContextEngine:   assembler,
		CheckpointStore: store,
		MaxTokens:       100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(
		context.Background(), []schema.Message{schema.UserMessage("first")},
		agent.WithConversationID("conversation-1"),
		agent.WithStateValue("tenant", "tenant-a"),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(
		context.Background(), []schema.Message{schema.UserMessage("second")},
		agent.WithConversationID("conversation-1"),
		agent.WithStateValue("turn", 2),
	); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	gotCalls := make([][]schema.Message, len(calls))
	for index := range calls {
		gotCalls[index] = schema.CloneMessages(calls[index])
	}
	mu.Unlock()
	if len(gotCalls) != 2 {
		t.Fatalf("model calls = %d", len(gotCalls))
	}
	second := gotCalls[1]
	if len(second) != 4 || second[0].Role != schema.RoleSystem || second[1].Text() != "first" || second[2].Text() != "answer-1" || second[3].Text() != "second" {
		t.Fatalf("second model context = %#v", second)
	}
	if _, ok := second[0].Extra[contextengine.PromptCacheMetadataKey]; !ok {
		t.Fatalf("system prompt was not cache-aligned: %#v", second[0].Extra)
	}
	checkpoint, found, err := store.Load(context.Background(), "conversation-1")
	if err != nil || !found {
		t.Fatalf("Load found=%t err=%v", found, err)
	}
	if checkpoint.Version != 2 || len(checkpoint.Messages) != 4 {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}
	if checkpoint.Messages[0].Text() != "first" || checkpoint.Messages[3].Text() != "answer-2" {
		t.Fatalf("checkpoint messages = %#v", checkpoint.Messages)
	}
	if checkpoint.Values["tenant"] != "tenant-a" || checkpoint.Values["turn"] != 2.0 && checkpoint.Values["turn"] != 2 {
		t.Fatalf("checkpoint values = %#v", checkpoint.Values)
	}
}

func TestConversationIDRequiresCheckpointStore(t *testing.T) {
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: llm.NewScriptedModel()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}, agent.WithConversationID("missing-store"))
	if err == nil {
		t.Fatal("Invoke accepted a conversation ID without a checkpoint store")
	}
}

func TestCheckpointWriteIsBoundedAfterSuccessfulRun(t *testing.T) {
	value, err := agent.NewBasicAgent(
		agent.BaseConfig{
			Model:           llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}),
			CheckpointStore: blockingCheckpointStore{},
		},
		agent.WithCheckpointTimeout(20*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = value.Invoke(
		context.Background(),
		[]schema.Message{schema.UserMessage("hello")},
		agent.WithConversationID("blocking-store"),
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Invoke error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("checkpoint write returned after %s", elapsed)
	}
}
