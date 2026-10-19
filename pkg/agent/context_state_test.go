package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func TestConversationCheckpointPreservesCallerSystemMessage(t *testing.T) {
	store := state.NewMemoryStore()
	var calls [][]schema.Message
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		calls = append(calls, schema.CloneMessages(request.Messages))
		return []schema.Message{schema.AssistantMessage("answer")}, nil
	})
	value, err := agent.New(model, agent.WithSystemMessage("agent policy"), agent.WithCheckpointStore(store))
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
	if len(calls) != 2 || calls[1][0].Role != schema.RoleSystem || calls[1][0].JoinedText("") != "agent policy\n\ncaller policy" {
		t.Fatalf("second model context=%#v", calls)
	}
	checkpoint, found, err := store.Load(context.Background(), "caller-system")
	if err != nil || !found {
		t.Fatalf("found=%t err=%v", found, err)
	}
	if len(checkpoint.Messages) == 0 || checkpoint.Messages[0].Role != schema.RoleSystem || checkpoint.Messages[0].JoinedText("") != "caller policy" {
		t.Fatalf("checkpoint messages=%#v", checkpoint.Messages)
	}
}

func TestAgentAddsEphemeralSystemReminderToModelRequest(t *testing.T) {
	store := state.NewMemoryStore()
	var requests []llm.Request
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		requests = append(requests, request.Clone())
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	value, err := agent.New(model, agent.WithCheckpointStore(store), agent.WithName("reminder-agent"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}, agent.WithConversationID("reminder-session")); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || len(requests[0].Messages) == 0 {
		t.Fatalf("requests=%#v", requests)
	}
	reminder := requests[0].Messages[len(requests[0].Messages)-1]
	if !contextengine.IsSystemReminder(reminder) {
		t.Fatalf("request reminder=%#v", reminder)
	}
	checkpoint, found, err := store.Load(context.Background(), "reminder-session")
	if err != nil || !found {
		t.Fatalf("checkpoint found=%t err=%v", found, err)
	}
	for _, message := range checkpoint.Messages {
		if contextengine.IsSystemReminder(message) {
			t.Fatal("ephemeral reminder was persisted in checkpoint")
		}
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
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
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
	model := llm.NewHandlerModel(func(ctx context.Context, request llm.Request, opts llm.Options) ([]schema.Message, error) {
		mu.Lock()
		calls = append(calls, schema.CloneMessages(request.Messages))
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
	value, err := agent.New(model, agent.WithName("persistent"), agent.WithSystemMessage("stable policy"), agent.WithContextBuilder(assembler), agent.WithCheckpointStore(store), agent.WithMaxTokens(100))
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
	if len(second) != 5 || second[0].Role != schema.RoleSystem || second[1].JoinedText("") != "first" || second[2].JoinedText("") != "answer-1" || second[3].JoinedText("") != "second" || !contextengine.IsSystemReminder(second[4]) {
		t.Fatalf("second model context = %#v", second)
	}
	if _, ok := second[0].Extra[contextengine.PromptCacheMetadataKey]; !ok {
		t.Fatalf("system prompt was not cache-aligned: %#v", second[0].Extra)
	}
	checkpoint, found, err := store.Load(context.Background(), "conversation-1")
	if err != nil || !found {
		t.Fatalf("Load found=%t err=%v", found, err)
	}
	if checkpoint.Version != 6 || len(checkpoint.Messages) != 4 {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}
	if checkpoint.Messages[0].JoinedText("") != "first" || checkpoint.Messages[3].JoinedText("") != "answer-2" {
		t.Fatalf("checkpoint messages = %#v", checkpoint.Messages)
	}
	if checkpoint.Values["tenant"] != "tenant-a" || checkpoint.Values["turn"] != 2.0 && checkpoint.Values["turn"] != 2 {
		t.Fatalf("checkpoint values = %#v", checkpoint.Values)
	}
}

// 会话加载后 conversation_id 与 checkpoint_version 经派生 ctx 元数据下发到
// 组件；调用方在 ctx 上的追踪标签同样继承。这些追踪信息不进入持久化
// State values——跨轮保存的数据必须显式走 WithStateValue。
func TestConversationMetadataFlowsThroughContextNotState(t *testing.T) {
	store := state.NewMemoryStore()
	var mu sync.Mutex
	var observed []map[string]any
	var observedValues []map[string]any
	model := llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		mu.Lock()
		observed = append(observed, observe.MetadataFromContext(ctx))
		observedValues = append(observedValues, state.ValuesFromContext(ctx))
		mu.Unlock()
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	value, err := agent.New(model, agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	runCtx := observe.WithMetadata(context.Background(), map[string]any{"source": "web"})
	invoke := func() {
		_, err := value.Invoke(runCtx, []schema.Message{schema.UserMessage("hi")},
			agent.WithConversationID("ctx-meta"),
			agent.WithStateValue("tenant", "tenant-a"),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	invoke()
	invoke()

	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 2 {
		t.Fatalf("model calls = %d", len(observed))
	}
	for index, metadata := range observed {
		if metadata["conversation_id"] != "ctx-meta" {
			t.Fatalf("call %d metadata = %#v", index, metadata)
		}
		if metadata["source"] != "web" {
			t.Fatalf("call %d lost caller ctx metadata: %#v", index, metadata)
		}
		if _, exists := metadata["checkpoint_version"]; !exists {
			t.Fatalf("call %d missing checkpoint_version: %#v", index, metadata)
		}
	}
	// 第二轮加载到已保存的检查点，version 应反映持久化进度。
	if observed[1]["checkpoint_version"] == observed[0]["checkpoint_version"] {
		t.Fatalf("checkpoint_version did not advance: %#v / %#v", observed[0], observed[1])
	}
	for index, values := range observedValues {
		if _, exists := values["conversation_id"]; exists {
			t.Fatalf("call %d leaked metadata into state values: %#v", index, values)
		}
		if _, exists := values["checkpoint_version"]; exists {
			t.Fatalf("call %d leaked checkpoint_version into state values: %#v", index, values)
		}
		if values["tenant"] != "tenant-a" {
			t.Fatalf("call %d lost durable state value: %#v", index, values)
		}
	}
	checkpoint, found, err := store.Load(context.Background(), "ctx-meta")
	if err != nil || !found {
		t.Fatalf("checkpoint found=%t err=%v", found, err)
	}
	if _, exists := checkpoint.Values["conversation_id"]; exists {
		t.Fatalf("checkpoint values contain metadata: %#v", checkpoint.Values)
	}
	if checkpoint.Values["tenant"] != "tenant-a" {
		t.Fatalf("checkpoint values = %#v", checkpoint.Values)
	}
}

func TestConversationIDRequiresCheckpointStore(t *testing.T) {
	value, err := agent.New(llm.NewScriptedModel())
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}, agent.WithConversationID("missing-store"))
	if err == nil {
		t.Fatal("Invoke accepted a conversation ID without a checkpoint store")
	}
}

func TestCheckpointWriteIsBoundedAfterSuccessfulRun(t *testing.T) {
	value, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}), agent.WithCheckpointStore(blockingCheckpointStore{}), agent.WithCheckpointTimeout(20*time.Millisecond))
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

func (blockingCheckpointStore) Get(context.Context, string, string) (state.Checkpoint, bool, error) {
	return state.Checkpoint{}, false, nil
}
func (blockingCheckpointStore) List(context.Context, string, state.ListOptions) ([]state.Checkpoint, error) {
	return nil, nil
}
