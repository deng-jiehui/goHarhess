package agent_test

import (
	"context"
	"errors"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"testing"
)

func TestUnifiedCheckpointCompletesConversationAndReusesResult(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	calls := 0
	a, err := agent.New(llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		calls++
		return []schema.Message{schema.AssistantMessage("answer")}, nil
	}), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithConversationID("unified")
	if _, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("first")}, option); err != nil {
		t.Fatal(err)
	}
	c, found, err := store.Load(ctx, "unified")
	if err != nil || !found || c.Status != state.StatusCompleted || len(c.Execution) == 0 || len(c.Result) == 0 || len(c.Messages) != 2 {
		t.Fatalf("checkpoint=%+v %v", c, err)
	}
	if _, err := agent.Resume(ctx, a, option); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("completed run executed again: %d", calls)
	}
	if _, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("second")}, option); err != nil {
		t.Fatal(err)
	}
	next, _, err := store.Load(ctx, "unified")
	if err != nil || len(next.Messages) != 4 || next.RunID == c.RunID || next.Messages[0].JoinedText("") != "first" || next.Messages[2].JoinedText("") != "second" {
		t.Fatalf("next checkpoint=%+v %v", next, err)
	}
}

type completionFailureStore struct {
	*state.MemoryStore
	fail bool
}

func (s *completionFailureStore) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	if s.fail && c.Status == state.StatusCompleted {
		return state.Checkpoint{}, errors.New("completion unavailable")
	}
	return s.MemoryStore.Save(ctx, c)
}

func TestAgentFailedCompletionResumesWithoutRepeatingModelOrMessages(t *testing.T) {
	ctx := context.Background()
	store := &completionFailureStore{MemoryStore: state.NewMemoryStore(), fail: true}
	calls := 0
	a, err := agent.New(llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		calls++
		return []schema.Message{schema.AssistantMessage("answer")}, nil
	}), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithConversationID("failed-completion")
	if _, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("question")}, option); err == nil {
		t.Fatal("completion failure hidden")
	}
	previous, _, err := store.Load(ctx, "failed-completion")
	if err != nil || previous.Status != state.StatusRunning || len(previous.Result) != 0 {
		t.Fatalf("failed completion status=%s err=%v", previous.Status, err)
	}
	store.fail = false
	if _, err := agent.Resume(ctx, a, option); err != nil {
		t.Fatal(err)
	}
	completed, _, err := store.Load(ctx, "failed-completion")
	if err != nil || completed.Status != state.StatusCompleted || len(completed.Messages) != 2 || calls != 1 {
		t.Fatalf("status=%s messages=%d calls=%d err=%v", completed.Status, len(completed.Messages), calls, err)
	}
}

func TestCheckpointConfigurationRejectsEmptyID(t *testing.T) {
	a, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("unexpected")}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Invoke(context.Background(), nil, agent.WithCheckpoint(state.NewMemoryStore(), "")); err == nil {
		t.Fatal("empty checkpoint ID silently disabled persistence")
	}
}

func TestModelContextTransformationPreservesCompleteHistory(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	builder := runnable.Func[contextengine.Request, contextengine.Result](func(context.Context, contextengine.Request, ...runnable.Option) (contextengine.Result, error) {
		return contextengine.Result{Messages: []schema.Message{schema.UserMessage("summary only")}}, nil
	})
	a, err := agent.New(llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if len(request.Messages) != 1 || request.Messages[0].JoinedText("") != "summary only" {
			t.Fatalf("model context=%v", request.Messages)
		}
		return []schema.Message{schema.AssistantMessage("answer")}, nil
	}), agent.WithCheckpointStore(store), agent.WithContextBuilder(builder))
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"original first", "original second"} {
		if _, err := a.Invoke(ctx, []schema.Message{schema.UserMessage(input)}, agent.WithConversationID("history")); err != nil {
			t.Fatal(err)
		}
	}
	c, _, err := store.Load(ctx, "history")
	if err != nil || len(c.Messages) != 4 || c.Messages[0].JoinedText("") != "original first" || c.Messages[2].JoinedText("") != "original second" {
		t.Fatalf("complete history lost: %v %v", c.Messages, err)
	}
}

func TestAgentPausedBeforeFirstNodeSavesCheckpoint(t *testing.T) {
	calls := 0
	a, err := agent.New(llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		calls++
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(control.RequestInterrupt("pause before first node", nil))
	store := state.NewMemoryStore()
	_, err = a.Invoke(ctx, []schema.Message{schema.UserMessage("input")}, agent.WithCheckpoint(store, "paused"))
	if !errors.Is(err, graph.ErrInterrupted) || calls != 0 {
		t.Fatalf("pause=%v calls=%d", err, calls)
	}
	c, found, err := store.Load(context.Background(), "paused")
	if err != nil || !found || c.Status != state.StatusInterrupted || len(c.Messages) != 1 {
		t.Fatalf("checkpoint status=%s found=%t err=%v", c.Status, found, err)
	}
}
