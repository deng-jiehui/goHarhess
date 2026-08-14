package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goharness/pkg/agent"
	"goharness/pkg/control"
	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/state"
	"goharness/pkg/tool"
)

type agentExecutionStore struct {
	mu      sync.Mutex
	data    map[string][]byte
	saves   int
	saveErr error
}

func (s *agentExecutionStore) Save(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.saves++
	if s.saveErr != nil {
		return s.saveErr
	}
	s.data[key] = append([]byte(nil), data...)
	return nil
}

func (s *agentExecutionStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func (s *agentExecutionStore) Load(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.data[key]
	return append([]byte(nil), data...), ok, nil
}

func TestAgentGraphResumeUsesExecutionStoreIndependentlyFromConversation(t *testing.T) {
	executionStore := &agentExecutionStore{}
	conversationStore := state.NewMemoryStore()
	started := make(chan struct{})
	first, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:          "recoverable-basic",
		SystemMessage: "old policy",
		Model: llm.NewHandlerModel(func(ctx context.Context, _ []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}),
		CheckpointStore: conversationStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := first.Invoke(ctx, []schema.Message{schema.SystemMessage("caller policy"), schema.UserMessage("continue")},
			agent.WithConversationID("conversation-1"),
			agent.WithCheckpoint(executionStore, "run-1"),
		)
		done <- runErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("model did not start")
	}
	cancel(control.RequestInterrupt("move execution", map[string]any{"device": "other"}))
	select {
	case runErr := <-done:
		if !errors.Is(runErr, graph.ErrInterrupted) {
			t.Fatalf("Invoke error=%v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupted Agent did not release its stack")
	}
	if checkpoint, found, loadErr := conversationStore.Load(context.Background(), "conversation-1"); loadErr != nil || found {
		t.Fatalf("conversation checkpoint=%#v found=%t err=%v", checkpoint, found, loadErr)
	}

	resumed, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:          "recoverable-basic",
		SystemMessage: "replacement policy",
		Model: llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			if len(messages) != 2 || messages[0].JoinedText("") != "replacement policy\n\ncaller policy" || messages[1].JoinedText("") != "continue" {
				t.Fatalf("restored messages=%#v", messages)
			}
			return []schema.Message{schema.AssistantMessage("resumed")}, nil
		}),
		CheckpointStore: conversationStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := agent.Resume(context.Background(), resumed,
		agent.WithConversationID("conversation-1"),
		agent.WithCheckpoint(executionStore, "run-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 || output[0].JoinedText("") != "resumed" {
		t.Fatalf("resume output=%#v", output)
	}
	checkpoint, found, err := conversationStore.Load(context.Background(), "conversation-1")
	if err != nil || !found || len(checkpoint.Messages) != 3 || checkpoint.Messages[0].JoinedText("") != "caller policy" || checkpoint.Messages[1].JoinedText("") != "continue" || checkpoint.Messages[2].JoinedText("") != "resumed" {
		t.Fatalf("conversation checkpoint=%#v found=%t err=%v", checkpoint, found, err)
	}
}

func TestAgentPreToolResumeRunsApprovalAgainBeforeTool(t *testing.T) {
	executionStore := &agentExecutionStore{}
	var approved atomic.Bool
	var toolCalls atomic.Int32
	hook := tool.BeforeBatchFunc(func(context.Context, tool.Batch) error {
		if !approved.Load() {
			return control.RequestInterrupt("approval required", nil)
		}
		return nil
	})
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		toolCalls.Add(1)
		return tool.TextResult("tool-result"), nil
	})
	value, err := agent.New(agent.BaseConfig{
		Name: "recoverable-react",
		Model: llm.NewScriptedModel(
			[]schema.Message{functionCallMessage("echo", "call-1")},
			[]schema.Message{schema.AssistantMessage("final")},
		),
		Tools: []tool.Tool{echo},
	}, agent.WithBeforeToolBatch(hook))
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, agent.WithCheckpoint(executionStore, "tool-run"))
	if !errors.Is(err, graph.ErrInterrupted) || toolCalls.Load() != 0 {
		t.Fatalf("Invoke error=%v tool calls=%d", err, toolCalls.Load())
	}
	approved.Store(true)
	output, err := agent.Resume(context.Background(), value, agent.WithCheckpoint(executionStore, "tool-run"))
	if err != nil {
		t.Fatal(err)
	}
	if toolCalls.Load() != 1 || len(output) != 3 || output[len(output)-1].JoinedText("") != "final" {
		t.Fatalf("resume output=%#v tool calls=%d", output, toolCalls.Load())
	}
}

func TestAgentPreToolResumeUsesCurrentReactPrompt(t *testing.T) {
	executionStore := &agentExecutionStore{}
	approved := atomic.Bool{}
	hook := tool.BeforeBatchFunc(func(context.Context, tool.Batch) error {
		if !approved.Load() {
			return control.RequestInterrupt("approval required", nil)
		}
		return nil
	})
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	first, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{Name: "prompt-stable", Model: llm.NewScriptedModel([]schema.Message{functionCallMessage("echo", "call-1")}), Tools: []tool.Tool{echo}},
		Prompt:     "old react prompt",
	}, agent.WithBeforeToolBatch(hook))
	if err != nil {
		t.Fatal(err)
	}
	_, err = first.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, agent.WithCheckpoint(executionStore, "prompt-run"))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	approved.Store(true)
	second, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "prompt-stable",
			Tools: []tool.Tool{echo},
			Model: llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
				if len(messages) == 0 || !strings.Contains(messages[0].JoinedText(""), "new react prompt") || strings.Contains(messages[0].JoinedText(""), "old react prompt") {
					t.Fatalf("resume model messages=%#v", messages)
				}
				return []schema.Message{schema.AssistantMessage("final")}, nil
			}),
		},
		Prompt: "new react prompt",
	}, agent.WithBeforeToolBatch(hook))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Resume(context.Background(), second, agent.WithCheckpoint(executionStore, "prompt-run")); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRoutingResumeDoesNotRepeatCompletedChild(t *testing.T) {
	executionStore := &agentExecutionStore{}
	var childCalls atomic.Int32
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker",
		Model: llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
			childCalls.Add(1)
			return []schema.Message{schema.AssistantMessage("worker result")}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	var supervisorCalls atomic.Int32
	modelStarted := make(chan struct{})
	supervisor, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name: "recoverable-routing",
			Model: llm.NewHandlerModel(func(ctx context.Context, _ []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
				switch supervisorCalls.Add(1) {
				case 1:
					return []schema.Message{routingCall("worker")}, nil
				case 2:
					close(modelStarted)
					<-ctx.Done()
					return nil, ctx.Err()
				default:
					return []schema.Message{schema.AssistantMessage("final")}, nil
				}
			}),
		},
		SubAgents:     []agent.Agent{worker},
		Mode:          agent.RoutingModeSupervisor,
		ReturnControl: true,
		MaxSteps:      3,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := supervisor.Invoke(ctx, []schema.Message{schema.UserMessage("route")}, agent.WithCheckpoint(executionStore, "routing-run"))
		done <- runErr
	}()
	select {
	case <-modelStarted:
	case <-time.After(time.Second):
		t.Fatal("second supervisor call did not start")
	}
	cancel(control.RequestInterrupt("replace supervisor", nil))
	if runErr := <-done; !errors.Is(runErr, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", runErr)
	}
	output, err := agent.Resume(context.Background(), supervisor, agent.WithCheckpoint(executionStore, "routing-run"))
	if err != nil {
		t.Fatal(err)
	}
	if childCalls.Load() != 1 || len(output) == 0 || output[len(output)-1].JoinedText("") != "final" {
		t.Fatalf("resume output=%#v child calls=%d", output, childCalls.Load())
	}
}

func TestAgentGroupResumeKeepsCompletedRound(t *testing.T) {
	executionStore := &agentExecutionStore{}
	alpha := &recordingAgent{name: "alpha"}
	beta := &recordingAgent{name: "beta"}
	var approved atomic.Bool
	selector := agent.AgentSelectorFunc(func(_ context.Context, agents []agent.Agent, state agent.SelectionState) (agent.Agent, error) {
		if state.Round == 1 && !approved.Load() {
			return nil, control.RequestInterrupt("approve next round", map[string]any{"round": state.Round})
		}
		return agents[state.Round%len(agents)], nil
	})
	group, err := agent.NewGroupChat(agent.GroupChatConfig{
		BaseConfig: agent.BaseConfig{Name: "recoverable-group"},
		Agents:     []agent.Agent{alpha, beta},
		Selector:   selector,
		MaxRounds:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = group.Invoke(context.Background(), []schema.Message{schema.UserMessage("start")}, agent.WithCheckpoint(executionStore, "group-run"))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	if len(alpha.Calls()) != 1 || len(beta.Calls()) != 0 {
		t.Fatalf("before resume alpha=%d beta=%d", len(alpha.Calls()), len(beta.Calls()))
	}
	approved.Store(true)
	output, err := agent.Resume(context.Background(), group, agent.WithCheckpoint(executionStore, "group-run"))
	if err != nil {
		t.Fatal(err)
	}
	if len(alpha.Calls()) != 1 || len(beta.Calls()) != 1 || len(output) != 2 || output[0].Extra["agent_name"] != "alpha" || output[1].Extra["agent_name"] != "beta" {
		t.Fatalf("resume output=%#v alpha=%d beta=%d", output, len(alpha.Calls()), len(beta.Calls()))
	}
}

func TestAgentRoutingChildCannotOverwriteRootCheckpoint(t *testing.T) {
	executionStore := &agentExecutionStore{}
	childStarted := make(chan struct{})
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "blocking-worker",
		Model: llm.NewHandlerModel(func(ctx context.Context, _ []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			close(childStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{Name: "root-router", Model: llm.NewScriptedModel([]schema.Message{routingCall("blocking-worker")})},
		SubAgents:  []agent.Agent{worker},
		Mode:       agent.RoutingModeRouter,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := router.Invoke(ctx, []schema.Message{schema.UserMessage("route")}, agent.WithCheckpoint(executionStore, "nested-run"))
		done <- runErr
	}()
	select {
	case <-childStarted:
	case <-time.After(time.Second):
		t.Fatal("child model did not start")
	}
	cancel(control.RequestInterrupt("pause root", nil))
	if runErr := <-done; !errors.Is(runErr, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", runErr)
	}
	if executionStore.saveCount() != 1 {
		t.Fatalf("checkpoint saves=%d, want root save only", executionStore.saveCount())
	}
	data, found, err := executionStore.Load(context.Background(), "nested-run")
	if err != nil || !found {
		t.Fatalf("checkpoint found=%t err=%v", found, err)
	}
	continuation, err := graph.DecodeContinuation(data)
	if err != nil {
		t.Fatal(err)
	}
	if continuation.GraphName != "root-router" || continuation.ResumeNode != "agent:blocking-worker" {
		t.Fatalf("continuation=%#v", continuation)
	}
}

func TestAgentInterruptCheckpointFailureIsReturned(t *testing.T) {
	saveErr := errors.New("agent checkpoint unavailable")
	executionStore := &agentExecutionStore{saveErr: saveErr}
	started := make(chan struct{})
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "checkpoint-failure",
		Model: llm.NewHandlerModel(func(ctx context.Context, _ []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := value.Invoke(ctx, []schema.Message{schema.UserMessage("run")}, agent.WithCheckpoint(executionStore, "failed-run"))
		done <- runErr
	}()
	<-started
	cancel(control.RequestInterrupt("pause", nil))
	select {
	case runErr := <-done:
		if !errors.Is(runErr, saveErr) || errors.Is(runErr, graph.ErrInterrupted) {
			t.Fatalf("Invoke error=%v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Agent did not return checkpoint failure")
	}
}

func TestAgentResumeRejectsTypedNil(t *testing.T) {
	var value *agent.BasicAgent
	if _, err := agent.Resume(context.Background(), value); err == nil {
		t.Fatal("Resume accepted typed nil Agent")
	}
}
