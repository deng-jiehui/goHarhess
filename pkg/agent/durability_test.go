package agent_test

import (
	"context"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// gatedAgentStore 在放行前阻塞进度写入（running），用于观察模型调用是否等待存储。
type gatedAgentStore struct {
	*state.MemoryStore
	gate chan struct{}
}

func (s *gatedAgentStore) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	if c.Status == state.StatusRunning {
		<-s.gate
	}
	return s.MemoryStore.Save(ctx, c)
}

// modelStartsBeforeWrite 报告模型是否在初始进度写入被阻塞时就已开始调用。
func modelStartsBeforeWrite(t *testing.T, cfg agent.BaseConfig, options ...runnable.Option) bool {
	t.Helper()
	store := &gatedAgentStore{MemoryStore: state.NewMemoryStore(), gate: make(chan struct{})}
	started := make(chan struct{}, 1)
	cfg.CheckpointStore = store
	cfg.Model = llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		started <- struct{}{}
		return []schema.Message{schema.AssistantMessage("answer")}, nil
	})
	a, err := agent.NewBasicAgent(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	runOptions := []runnable.Option{agent.WithConversationID("durability")}
	for _, option := range options {
		runOptions = append(runOptions, option)
	}
	go func() {
		_, err := a.Invoke(context.Background(), []schema.Message{schema.UserMessage("q")}, runOptions...)
		done <- err
	}()
	overlapped := false
	select {
	case <-started:
		overlapped = true
	case <-time.After(150 * time.Millisecond):
	}
	close(store.gate)
	if !overlapped {
		<-started
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	latest, _, err := store.Load(context.Background(), "durability")
	if err != nil || latest.Status != state.StatusCompleted {
		t.Fatalf("status=%q err=%v", latest.Status, err)
	}
	return overlapped
}

func TestAgentCheckpointDurabilityDefaultsToAsync(t *testing.T) {
	if !modelStartsBeforeWrite(t, agent.BaseConfig{}) {
		t.Fatal("default durability waited for the checkpoint write")
	}
}

func TestAgentCheckpointDurabilityConfigAndRunOverride(t *testing.T) {
	syncConfig := agent.BaseConfig{CheckpointDurability: graph.DurabilitySync}
	if modelStartsBeforeWrite(t, syncConfig) {
		t.Fatal("sync durability did not wait for the checkpoint write")
	}
	if !modelStartsBeforeWrite(t, syncConfig, agent.WithDurability(graph.DurabilityAsync)) {
		t.Fatal("run option did not override the configured durability")
	}
}

func TestAgentRejectsUnknownCheckpointDurability(t *testing.T) {
	_, err := agent.NewBasicAgent(agent.BaseConfig{
		Model:                llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("x")}),
		CheckpointDurability: "eventually",
	})
	if err == nil {
		t.Fatal("unknown durability accepted")
	}
}
