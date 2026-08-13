package agent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

type recordingAgent struct {
	name string

	mu    sync.Mutex
	calls [][]schema.Message
}

type incompleteGroupAgent struct{}

func (incompleteGroupAgent) Name() string        { return "incomplete" }
func (incompleteGroupAgent) Description() string { return "malformed group member" }
func (incompleteGroupAgent) Invoke(context.Context, []schema.Message, ...runnable.Option) ([]schema.Message, error) {
	return []schema.Message{schema.AssistantMessage("partial")}, nil
}
func (incompleteGroupAgent) Stream(context.Context, []schema.Message, ...runnable.Option) (runnable.Stream[schema.MessageEvent], error) {
	stream := make(chan runnable.Item[schema.MessageEvent], 1)
	stream <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{
		Kind:     schema.MessageEventSnapshot,
		Messages: []schema.Message{schema.AssistantMessage("partial")},
	}}
	close(stream)
	return stream, nil
}

func (a *recordingAgent) Name() string {
	return a.name
}

func (a *recordingAgent) Description() string {
	return "test agent"
}

func (a *recordingAgent) Invoke(ctx context.Context, messages []schema.Message, opts ...runnable.Option) ([]schema.Message, error) {
	a.mu.Lock()
	a.calls = append(a.calls, schema.CloneMessages(messages))
	callNumber := len(a.calls)
	a.mu.Unlock()

	return []schema.Message{schema.AssistantMessage(fmt.Sprintf("%s-%d", a.name, callNumber))}, nil
}

func (a *recordingAgent) Stream(ctx context.Context, messages []schema.Message, opts ...runnable.Option) (runnable.Stream[schema.MessageEvent], error) {
	output, err := a.Invoke(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}
	out := make(chan runnable.Item[schema.MessageEvent], 2)
	out <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: output}}
	out <- runnable.Item[schema.MessageEvent]{Value: schema.MessageEvent{Kind: schema.MessageEventDone}}
	close(out)
	return out, nil
}

func (a *recordingAgent) Calls() [][]schema.Message {
	a.mu.Lock()
	defer a.mu.Unlock()

	calls := make([][]schema.Message, len(a.calls))
	for i := range a.calls {
		calls[i] = schema.CloneMessages(a.calls[i])
	}
	return calls
}

func TestGroupChatCollaboratesAcrossRounds(t *testing.T) {
	alpha := &recordingAgent{name: "alpha"}
	beta := &recordingAgent{name: "beta"}
	group, err := agent.NewGroupChat(agent.GroupChatConfig{
		BaseConfig: agent.BaseConfig{Name: "team"},
		Agents:     []agent.Agent{alpha, beta},
	})
	if err != nil {
		t.Fatal(err)
	}

	last, err := group.Invoke(context.Background(), []schema.Message{schema.UserMessage("start")})
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != agent.DefaultGroupChatMaxRounds {
		t.Fatalf("len(last) = %d, want %d: %#v", len(last), agent.DefaultGroupChatMaxRounds, last)
	}

	wantNames := []string{"alpha", "beta", "alpha"}
	wantTexts := []string{"alpha-1", "beta-1", "alpha-2"}
	for i := range last {
		if last[i].Name != wantNames[i] || last[i].Text() != wantTexts[i] {
			t.Fatalf("message %d = %#v, want name=%q text=%q", i, last[i], wantNames[i], wantTexts[i])
		}
	}

	betaCalls := beta.Calls()
	if len(betaCalls) != 1 || len(betaCalls[0]) != 2 {
		t.Fatalf("beta calls = %#v", betaCalls)
	}
	if betaCalls[0][1].Name != "alpha" || betaCalls[0][1].Text() != "alpha-1" {
		t.Fatalf("beta did not receive alpha output: %#v", betaCalls[0])
	}

	alphaCalls := alpha.Calls()
	if len(alphaCalls) != 2 || len(alphaCalls[1]) != 3 {
		t.Fatalf("alpha calls = %#v", alphaCalls)
	}
	if alphaCalls[1][2].Name != "beta" || alphaCalls[1][2].Text() != "beta-1" {
		t.Fatalf("alpha did not receive beta output: %#v", alphaCalls[1])
	}
}

func TestNewHubRejectsTypedNilAgent(t *testing.T) {
	var typedNil *agent.BasicAgent
	if _, err := agent.NewHub([]agent.Agent{typedNil}); err == nil {
		t.Fatal("NewHub accepted a typed nil agent")
	}
}

func TestGroupChatUsesCustomSelector(t *testing.T) {
	alpha := &recordingAgent{name: "alpha"}
	beta := &recordingAgent{name: "beta"}
	selector := agent.AgentSelectorFunc(func(ctx context.Context, agents []agent.Agent, state agent.SelectionState) (agent.Agent, error) {
		return agents[1], nil
	})
	group, err := agent.NewGroupChat(agent.GroupChatConfig{
		BaseConfig: agent.BaseConfig{Name: "team"},
		Agents:     []agent.Agent{alpha, beta},
		Selector:   selector,
		MaxRounds:  2,
	})
	if err != nil {
		t.Fatal(err)
	}

	last, err := group.Invoke(context.Background(), []schema.Message{schema.UserMessage("start")})
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 2 || last[0].Name != "beta" || last[1].Name != "beta" {
		t.Fatalf("messages = %#v", last)
	}
	if len(alpha.Calls()) != 0 || len(beta.Calls()) != 2 {
		t.Fatalf("alpha calls = %d, beta calls = %d", len(alpha.Calls()), len(beta.Calls()))
	}
}

func TestGroupChatRejectsMemberStreamWithoutDone(t *testing.T) {
	group, err := agent.NewGroupChat(agent.GroupChatConfig{
		BaseConfig: agent.BaseConfig{Name: "team"},
		Agents:     []agent.Agent{incompleteGroupAgent{}},
		MaxRounds:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = group.Invoke(context.Background(), []schema.Message{schema.UserMessage("start")})
	if err == nil || !strings.Contains(err.Error(), "without done event") {
		t.Fatalf("group error=%v", err)
	}
}

func TestGroupChatPersistsTypedEngineCheckpointState(t *testing.T) {
	store := state.NewMemoryStore()
	group, err := agent.NewGroupChat(agent.GroupChatConfig{
		BaseConfig: agent.BaseConfig{
			Name:            "team",
			CheckpointStore: store,
			Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(_ context.Context, point agent.Breakpoint) (agent.InterruptDecision, error) {
				return agent.InterruptDecision{Interrupt: point.LLMCalls == 1, Reason: "pause round"}, nil
			})},
		},
		Agents:    []agent.Agent{&recordingAgent{name: "alpha"}, &recordingAgent{name: "beta"}},
		MaxRounds: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = group.Invoke(context.Background(), []schema.Message{schema.UserMessage("start")}, agent.WithConversationID("group-engine"))
	if !errors.Is(err, agent.ErrInterrupted) {
		t.Fatalf("Invoke error = %v", err)
	}
	checkpoint, found, err := store.Load(context.Background(), "group-engine")
	if err != nil || !found || checkpoint.Continuation == nil || checkpoint.Continuation.Engine == nil {
		t.Fatalf("checkpoint = %#v, found=%t, err=%v", checkpoint, found, err)
	}
	engine := checkpoint.Continuation.Engine
	if engine.Round != 1 || engine.LastAgent != "alpha" {
		t.Fatalf("engine checkpoint = %#v", engine)
	}
	if _, exists := checkpoint.Continuation.Cursor["last_agent"]; exists {
		t.Fatalf("last agent leaked into generic cursor: %#v", checkpoint.Continuation.Cursor)
	}
	if _, err := checkpoint.JSON(); err != nil {
		t.Fatalf("engine checkpoint is not portable JSON: %v", err)
	}
}
