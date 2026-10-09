package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

type agentExecutionStore struct {
	mu        sync.Mutex
	memory    *state.MemoryStore
	data      map[string][]byte
	saves     int
	saveErr   error
	failAfter int
}

func (s *agentExecutionStore) store() *state.MemoryStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.memory == nil {
		s.memory = state.NewMemoryStore()
	}
	return s.memory
}
func (s *agentExecutionStore) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	s.mu.Lock()
	s.saves++
	err := s.saveErr
	if s.saves <= s.failAfter {
		err = nil
	}
	s.mu.Unlock()
	if err != nil {
		return state.Checkpoint{}, err
	}
	return s.store().Save(ctx, c)
}
func (s *agentExecutionStore) Load(ctx context.Context, key string) (state.Checkpoint, bool, error) {
	return s.store().Load(ctx, key)
}
func (s *agentExecutionStore) Get(ctx context.Context, key, id string) (state.Checkpoint, bool, error) {
	return s.store().Get(ctx, key, id)
}
func (s *agentExecutionStore) List(ctx context.Context, key string, o state.ListOptions) ([]state.Checkpoint, error) {
	return s.store().List(ctx, key, o)
}
func (s *agentExecutionStore) Delete(ctx context.Context, key string) error {
	return s.store().Delete(ctx, key)
}
func (s *agentExecutionStore) saveCount() int { s.mu.Lock(); defer s.mu.Unlock(); return s.saves }

func TestAgentGraphResumeUsesOneStoreForConversationAndExecution(t *testing.T) {
	executionStore := &agentExecutionStore{}
	conversationStore := executionStore
	started := make(chan struct{})
	first, err := agent.New(llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}), agent.WithName("recoverable-basic"), agent.WithSystemMessage("old policy"), agent.WithCheckpointStore(conversationStore))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := first.Invoke(ctx, []schema.Message{schema.SystemMessage("caller policy"), schema.UserMessage("continue")},
			agent.WithConversationID("conversation-1"),
			agent.WithCheckpoint(executionStore, "conversation-1"),
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
	if checkpoint, found, loadErr := conversationStore.Load(context.Background(), "conversation-1"); loadErr != nil || !found || checkpoint.Status != state.StatusInterrupted || len(checkpoint.Messages) != 2 {
		t.Fatalf("conversation checkpoint=%#v found=%t err=%v", checkpoint, found, loadErr)
	}

	resumed, err := agent.New(llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if len(request.Messages) != 2 || request.Messages[0].JoinedText("") != "replacement policy\n\ncaller policy" || request.Messages[1].JoinedText("") != "continue" {
			t.Fatalf("restored messages=%#v", request.Messages)
		}
		return []schema.Message{schema.AssistantMessage("resumed")}, nil
	}), agent.WithName("recoverable-basic"), agent.WithSystemMessage("replacement policy"), agent.WithCheckpointStore(conversationStore))
	if err != nil {
		t.Fatal(err)
	}
	output, err := agent.Resume(context.Background(), resumed,
		agent.WithConversationID("conversation-1"),
		agent.WithCheckpoint(executionStore, "conversation-1"),
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
	value, err := agent.New(llm.NewScriptedModel(
		[]schema.Message{functionCallMessage("echo", "call-1")},
		[]schema.Message{schema.AssistantMessage("final")},
	), agent.WithName("recoverable-react"), agent.WithTools(echo), agent.WithBeforeToolBatch(hook))
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
	first, err := agent.New(llm.NewScriptedModel([]schema.Message{functionCallMessage("echo", "call-1")}), agent.WithName("prompt-stable"), agent.WithTools(echo), agent.WithPrompt("old react prompt"), agent.WithBeforeToolBatch(hook))
	if err != nil {
		t.Fatal(err)
	}
	_, err = first.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, agent.WithCheckpoint(executionStore, "prompt-run"))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	approved.Store(true)
	second, err := agent.New(llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if len(request.Messages) == 0 || !strings.Contains(request.Messages[0].JoinedText(""), "new react prompt") || strings.Contains(request.Messages[0].JoinedText(""), "old react prompt") {
			t.Fatalf("resume model messages=%#v", request.Messages)
		}
		return []schema.Message{schema.AssistantMessage("final")}, nil
	}), agent.WithName("prompt-stable"), agent.WithTools(echo), agent.WithPrompt("new react prompt"), agent.WithBeforeToolBatch(hook))
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
	worker, err := agent.New(llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		childCalls.Add(1)
		return []schema.Message{schema.AssistantMessage("worker result")}, nil
	}), agent.WithName("worker"))
	if err != nil {
		t.Fatal(err)
	}
	var supervisorCalls atomic.Int32
	modelStarted := make(chan struct{})
	supervisor := newSupervisorApp(t, "recoverable-routing", llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
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
	}), false, 3, worker)
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := supervisor.Invoke(ctx, supervisorState{Messages: []schema.Message{schema.UserMessage("route")}}, graph.WithCheckpoint(executionStore, "routing-run"))
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
	output, err := supervisor.Resume(context.Background(), graph.WithCheckpoint(executionStore, "routing-run"))
	if err != nil {
		t.Fatal(err)
	}
	if childCalls.Load() != 1 || len(output.Messages) == 0 || output.Messages[len(output.Messages)-1].JoinedText("") != "final" {
		t.Fatalf("resume output=%#v child calls=%d", output.Messages, childCalls.Load())
	}
}

func TestAgentRoutingChildCannotOverwriteRootCheckpoint(t *testing.T) {
	executionStore := &agentExecutionStore{}
	childStarted := make(chan struct{})
	worker, err := agent.New(llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		close(childStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}), agent.WithName("blocking-worker"))
	if err != nil {
		t.Fatal(err)
	}
	router := newSupervisorApp(t, "root-router", llm.NewScriptedModel([]schema.Message{routingCall("blocking-worker")}), true, 0, worker)
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := router.Invoke(ctx, supervisorState{Messages: []schema.Message{schema.UserMessage("route")}}, graph.WithCheckpoint(executionStore, "nested-run"))
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
	// 执行树模型下子调用登记与子 frame 提交会额外写检查点；关键是根记录的
	// Execution 始终是根 frame——子 Agent 不得用自己的进度覆盖根调度现场。
	if executionStore.saveCount() < 3 {
		t.Fatalf("checkpoint saves=%d, want at least initial, route, and interrupt saves for root", executionStore.saveCount())
	}
	record, found, err := executionStore.Load(context.Background(), "nested-run")
	data := record.Execution
	if err != nil || !found {
		t.Fatalf("checkpoint found=%t err=%v", found, err)
	}
	var progress struct {
		GraphName  string `json:"graph_name"`
		ResumeNode string `json:"resume_node"`
		Children   []struct {
			Frame struct {
				GraphName string `json:"graph_name"`
			} `json:"frame"`
		} `json:"children"`
	}
	err = json.Unmarshal(data, &progress)
	if err != nil {
		t.Fatal(err)
	}
	if progress.GraphName != "root-router" || progress.ResumeNode != "agent:blocking-worker" {
		t.Fatalf("execution progress=%#v", progress)
	}
	// 子调用在执行树中登记；子 frame 以自己的 GraphName 标识。
	if len(progress.Children) == 0 {
		t.Fatalf("execution progress missing child record: %s", data)
	}
}

func TestAgentInterruptCheckpointFailureIsReturned(t *testing.T) {
	saveErr := errors.New("agent checkpoint unavailable")
	executionStore := &agentExecutionStore{saveErr: saveErr, failAfter: 1}
	started := make(chan struct{})
	value, err := agent.New(llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}), agent.WithName("checkpoint-failure"))
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

// 会话外部化：持久化的 Execution 里会话以 history_len 引用保存，
// 不再内联完整历史；恢复时从检查点会话回填。
func TestAgentCheckpointExternalizesConversationHistory(t *testing.T) {
	executionStore := &agentExecutionStore{}
	started := make(chan struct{})
	first, err := agent.New(llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}), agent.WithName("externalized"), agent.WithSystemMessage("policy"), agent.WithCheckpointStore(executionStore))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := first.Invoke(ctx, []schema.Message{schema.UserMessage("very long user prompt for externalization check")},
			agent.WithConversationID("ext-conv"),
			agent.WithCheckpoint(executionStore, "ext-conv"),
		)
		done <- runErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("model did not start")
	}
	cancel(control.RequestInterrupt("pause", nil))
	<-done

	record, found, err := executionStore.Load(context.Background(), "ext-conv")
	if err != nil || !found {
		t.Fatalf("checkpoint found=%v err=%v", found, err)
	}
	// state.data 是 base64 内嵌的 State JSON：外部化后 history 字段
	// 消失，由 conv_ext/history_len 引用代替。
	var probe struct {
		State struct {
			Data json.RawMessage `json:"data"`
		} `json:"state"`
	}
	if err := json.Unmarshal(record.Execution, &probe); err != nil {
		t.Fatal(err)
	}
	var data []byte
	if err := json.Unmarshal(probe.State.Data, &data); err != nil {
		t.Fatal(err)
	}
	var wire struct {
		History    []json.RawMessage `json:"history"`
		ConvExt    bool              `json:"conv_ext"`
		HistoryLen int               `json:"history_len"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if !wire.ConvExt || wire.HistoryLen != len(record.Messages) || wire.History != nil {
		t.Fatalf("conv_ext=%v history_len=%d inline history=%d checkpoint messages=%d",
			wire.ConvExt, wire.HistoryLen, len(wire.History), len(record.Messages))
	}
}
