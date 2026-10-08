package agent_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
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
func (incompleteGroupAgent) Stream(ctx context.Context, _ []schema.Message, _ ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	reader, writer := runnable.Pipe[[]schema.Message](ctx)
	go func() { _ = writer.CloseWithError(fmt.Errorf("incomplete group stream")) }()
	return reader, nil
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

func (a *recordingAgent) Stream(ctx context.Context, messages []schema.Message, opts ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	output, err := a.Invoke(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}
	reader, writer := runnable.Pipe[[]schema.Message](ctx)
	go func() { _ = writer.Send(output); _ = writer.Close() }()
	return reader, nil
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

func (a *recordingAgent) Collect(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, in, a.Invoke, opts...)
}
func (a *recordingAgent) Transform(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, in, a.Invoke, opts...)
}
func (a incompleteGroupAgent) Collect(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, in, a.Invoke, opts...)
}
func (a incompleteGroupAgent) Transform(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, in, a.Invoke, opts...)
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
		if last[i].Extra["agent_name"] != wantNames[i] || last[i].JoinedText("") != wantTexts[i] {
			t.Fatalf("message %d = %#v, want name=%q text=%q", i, last[i], wantNames[i], wantTexts[i])
		}
	}

	betaCalls := beta.Calls()
	if len(betaCalls) != 1 || len(betaCalls[0]) != 2 {
		t.Fatalf("beta calls = %#v", betaCalls)
	}
	if betaCalls[0][1].Extra["agent_name"] != "alpha" || betaCalls[0][1].JoinedText("") != "alpha-1" {
		t.Fatalf("beta did not receive alpha output: %#v", betaCalls[0])
	}

	alphaCalls := alpha.Calls()
	if len(alphaCalls) != 2 || len(alphaCalls[1]) != 3 {
		t.Fatalf("alpha calls = %#v", alphaCalls)
	}
	if alphaCalls[1][2].Extra["agent_name"] != "beta" || alphaCalls[1][2].JoinedText("") != "beta-1" {
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
	if len(last) != 2 || last[0].Extra["agent_name"] != "beta" || last[1].Extra["agent_name"] != "beta" {
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
	if err == nil || !strings.Contains(err.Error(), "incomplete group stream") {
		t.Fatalf("group error=%v", err)
	}
}
