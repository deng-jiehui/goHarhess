package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func saveRecord(t *testing.T, store state.Store, previous state.Checkpoint, next state.Checkpoint) state.Checkpoint {
	t.Helper()
	next.ConversationID = "thread"
	next.ID, next.Version = previous.ID, previous.Version
	saved, err := store.Save(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	return saved
}

func TestAbandonRunRevertsConversationToRunStart(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	completed := saveRecord(t, store, state.Checkpoint{}, state.Checkpoint{
		RunID: "run-a", Status: state.StatusCompleted,
		Messages: []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")},
		Values:   map[string]any{"step": "a"},
	})
	started := saveRecord(t, store, completed, state.Checkpoint{
		RunID: "run-b", Status: state.StatusRunning, Execution: []byte(`{"node":"llm"}`),
		Messages: []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1"), schema.UserMessage("q2")},
		Values:   map[string]any{"step": "b"},
	})
	latest := saveRecord(t, store, started, state.Checkpoint{
		RunID: "run-b", Status: state.StatusInterrupted, Execution: []byte(`{"node":"tools"}`),
		Messages: []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1"), schema.UserMessage("q2"), schema.AssistantMessage("partial")},
		Values:   map[string]any{"step": "b"},
	})

	abandoned, err := state.AbandonRun(ctx, store, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if abandoned.Status != state.StatusAbandoned || abandoned.ParentID != latest.ID || abandoned.RunID != "run-b" {
		t.Fatalf("abandoned=%+v", abandoned)
	}
	if len(abandoned.Execution) != 0 || len(abandoned.Result) != 0 {
		t.Fatal("abandoned record keeps execution data")
	}
	if len(abandoned.Messages) != 2 || abandoned.Messages[1].JoinedText("") != "a1" || abandoned.Values["step"] != "a" {
		t.Fatalf("conversation was not reverted: messages=%d values=%v", len(abandoned.Messages), abandoned.Values)
	}
	history, err := store.List(ctx, "thread", state.ListOptions{})
	if err != nil || len(history) != 4 || history[1].ID != latest.ID {
		t.Fatalf("abandoned run is not kept in history: %d %v", len(history), err)
	}
	if _, err := state.AbandonRun(ctx, store, "thread"); !errors.Is(err, state.ErrNoUnfinishedRun) {
		t.Fatalf("second abandon err=%v", err)
	}
}

func TestAbandonFirstRunLeavesEmptyConversation(t *testing.T) {
	store := state.NewMemoryStore()
	saveRecord(t, store, state.Checkpoint{}, state.Checkpoint{
		RunID: "run-a", Status: state.StatusRunning, Messages: []schema.Message{schema.UserMessage("q1")},
	})
	abandoned, err := state.AbandonRun(context.Background(), store, "thread")
	if err != nil || len(abandoned.Messages) != 0 || abandoned.Status != state.StatusAbandoned {
		t.Fatalf("abandoned=%+v err=%v", abandoned, err)
	}
}

func TestAbandonRunRequiresUnfinishedRun(t *testing.T) {
	store := state.NewMemoryStore()
	if _, err := state.AbandonRun(context.Background(), store, "missing"); !errors.Is(err, state.ErrNoUnfinishedRun) {
		t.Fatalf("missing err=%v", err)
	}
	saveRecord(t, store, state.Checkpoint{}, state.Checkpoint{RunID: "run-a", Status: state.StatusCompleted})
	if _, err := state.AbandonRun(context.Background(), store, "thread"); !errors.Is(err, state.ErrNoUnfinishedRun) {
		t.Fatalf("completed err=%v", err)
	}
}
