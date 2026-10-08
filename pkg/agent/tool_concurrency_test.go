package agent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

type agentRunResult struct {
	messages []schema.Message
	err      error
}

func TestReactAgentStartsToolOnlyAfterLLMStreamEnds(t *testing.T) {
	allowArgumentsComplete := make(chan struct{})
	allowStreamEnd := make(chan struct{})
	toolStarted := make(chan string, 1)
	allowToolEnd := make(chan struct{})
	partialArgumentsSent := make(chan struct{})
	firstStreamEnded := make(chan struct{})
	secondCallStarted := make(chan struct{})
	model := &earlyStartModel{
		allowArgumentsComplete: allowArgumentsComplete,
		allowFirstStreamEnd:    allowStreamEnd,
		partialArgumentsSent:   partialArgumentsSent,
		firstStreamEnded:       firstStreamEnded,
		secondCallStarted:      secondCallStarted,
	}
	value, err := agent.New(agent.BaseConfig{
		Name:  "early",
		Model: model,
		Tools: []tool.Tool{blockingTool(
			"early-tool",
			"network",
			1,
			toolStarted,
			allowToolEnd,
		)},
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan agentRunResult, 1)
	go func() {
		messages, err := value.Invoke(
			context.Background(),
			[]schema.Message{schema.UserMessage("run")},
			agent.WithDeltaStream(true),
		)
		done <- agentRunResult{messages: messages, err: err}
	}()

	select {
	case <-partialArgumentsSent:
	case <-time.After(time.Second):
		t.Fatal("partial tool arguments were not streamed")
	}
	assertNoToolStart(t, toolStarted)
	close(allowArgumentsComplete)
	assertNoToolStart(t, toolStarted)
	close(allowStreamEnd)
	select {
	case <-firstStreamEnded:
	case <-time.After(time.Second):
		t.Fatal("LLM stream did not end")
	}
	if name := waitToolName(t, toolStarted); name != "early-tool" {
		t.Fatalf("started tool = %q", name)
	}
	close(allowToolEnd)

	result := waitRunResult(t, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.messages) != 3 || len(result.messages[1].FunctionToolResults()) != 1 || result.messages[1].FunctionToolResults()[0].Name != "early-tool" || result.messages[2].JoinedText("") != "done" {
		t.Fatalf("messages = %#v", result.messages)
	}
}

func TestToolBatchInterruptExecutesNoTools(t *testing.T) {
	var calls atomic.Int32
	makeTool := func(name string) tool.Tool {
		return tool.NewFunctionTool(tool.Spec{Name: name}, func(context.Context, tool.CallInput) (tool.Result, error) {
			calls.Add(1)
			return tool.TextResult(name), nil
		})
	}
	model := llm.NewScriptedModel([]schema.Message{
		functionCallMessage("read", "read-1"),
		functionCallMessage("write", "write-1"),
	})
	hookCalls := 0
	hookCalled := make(chan struct{}, 1)
	value, err := agent.New(agent.BaseConfig{
		Name:  "approval",
		Model: model,
		Tools: []tool.Tool{makeTool("read"), makeTool("write")},
	}, agent.WithBeforeToolBatch(tool.BeforeBatchFunc(func(_ context.Context, batch tool.Batch) error {
		hookCalls++
		hookCalled <- struct{}{}
		if len(batch.Calls) != 2 || batch.Calls[0].ID != "read-1" || batch.Calls[1].ID != "write-1" {
			t.Fatalf("batch=%#v", batch)
		}
		return control.RequestInterrupt("approval required", map[string]any{"tool": "write"})
	})))
	if err != nil {
		t.Fatal(err)
	}

	store := &agentExecutionStore{}
	stream, err := value.Stream(context.Background(), []schema.Message{schema.UserMessage("run")}, agent.WithCheckpoint(store, "batch-interrupt"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-hookCalled:
	case <-time.After(time.Second):
		t.Fatal("before-batch hook did not run")
	}
	// 读到流终止错误时后台写入已 flush，落盘次数是确定的。
	for {
		if _, err := stream.Recv(); err != nil {
			if !errors.Is(err, graph.ErrInterrupted) {
				t.Fatalf("stream error=%v", err)
			}
			break
		}
	}
	_ = stream.Close()
	if hookCalls != 1 || calls.Load() != 0 {
		t.Fatalf("hook calls=%d tool calls=%d", hookCalls, calls.Load())
	}
	if store.saveCount() != 3 {
		t.Fatalf("checkpoint saves=%d", store.saveCount())
	}
}

func TestReactAgentToolTimeoutJoinsAsObservation(t *testing.T) {
	toolStarted := make(chan string, 1)
	neverRelease := make(chan struct{})
	model := llm.NewScriptedModel(
		[]schema.Message{functionCallMessage("slow", "slow-1")},
		[]schema.Message{schema.AssistantMessage("continued")},
	)
	value, err := agent.New(agent.BaseConfig{
		Name:  "timeout",
		Model: model,
		Tools: []tool.Tool{blockingTool(
			"slow",
			"network",
			1,
			toolStarted,
			neverRelease,
		)},
	})
	if err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	last, err := value.Invoke(
		context.Background(),
		[]schema.Message{schema.UserMessage("run")},
		agent.WithToolCallTimeout(30*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(startedAt) > 500*time.Millisecond {
		t.Fatalf("tool timeout took too long: %s", time.Since(startedAt))
	}
	if len(last) != 3 {
		t.Fatalf("messages = %#v", last)
	}
	if !strings.Contains(toolResultText(last[1]), `tool "slow" timed out`) || last[1].FunctionToolResults()[0].CallID != "slow-1" {
		t.Fatalf("timeout observation = %#v", last[1])
	}
	if last[2].JoinedText("") != "continued" {
		t.Fatalf("final response = %#v", last[2])
	}
}

func TestReactAgentRunsToolBatchConcurrentlyAndPreservesOrder(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	first := blockingTool("first", "network", 2, started, release)
	second := blockingTool("second", "network", 2, started, release)
	model := llm.NewScriptedModel(
		[]schema.Message{
			functionCallMessage("first", "call-1"),
			functionCallMessage("second", "call-2"),
		},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.New(agent.BaseConfig{
		Name: "parallel", Model: model, Tools: []tool.Tool{first, second},
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan agentRunResult, 1)
	go func() {
		messages, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")})
		done <- agentRunResult{messages: messages, err: err}
	}()

	waitToolName(t, started)
	waitToolName(t, started)
	close(release)
	result := waitRunResult(t, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.messages) != 5 {
		t.Fatalf("messages = %#v", result.messages)
	}
	firstResults := result.messages[2].FunctionToolResults()
	if len(firstResults) != 1 || firstResults[0].Name != "first" || firstResults[0].CallID != "call-1" {
		t.Fatalf("first result = %#v", result.messages[2])
	}
	secondResults := result.messages[3].FunctionToolResults()
	if len(secondResults) != 1 || secondResults[0].Name != "second" || secondResults[0].CallID != "call-2" {
		t.Fatalf("second result = %#v", result.messages[3])
	}
}

func TestReactAgentEmitsToolObservationsAsOneBatch(t *testing.T) {
	first := tool.NewFunctionTool(tool.Spec{Name: "first"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult("first-result"), nil
	})
	second := tool.NewFunctionTool(tool.Spec{Name: "second"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult("second-result"), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{functionCallMessage("first", "1"), functionCallMessage("second", "2")},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.New(agent.BaseConfig{
		Name: "batch", Model: model, Tools: []tool.Tool{first, second},
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := value.Stream(context.Background(), []schema.Message{schema.UserMessage("run")})
	if err != nil {
		t.Fatal(err)
	}
	output, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	chunks := [][]schema.Message{output}
	observationBatches := 0
	for _, messages := range chunks {
		functions := 0
		for _, message := range messages {
			if message.Role == schema.RoleTool && len(message.FunctionToolResults()) == 1 {
				functions++
			}
		}
		if functions > 0 {
			observationBatches++
			if functions != 2 {
				t.Fatalf("function messages in batch = %d, want 2: %#v", functions, messages)
			}
		}
	}
	if observationBatches != 1 {
		t.Fatalf("observation-bearing chunks = %d, want one complete batch: %#v", observationBatches, chunks)
	}
}

func TestReactAgentHonorsPerRunParallelLimit(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	first := blockingTool("first", "network", 2, started, release)
	second := blockingTool("second", "network", 2, started, release)
	model := llm.NewScriptedModel(
		[]schema.Message{functionCallMessage("first", "1"), functionCallMessage("second", "2")},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.New(agent.BaseConfig{Name: "limited", Model: model, Tools: []tool.Tool{first, second}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := value.Invoke(
			context.Background(),
			[]schema.Message{schema.UserMessage("run")},
			agent.WithMaxParallelTools(1),
		)
		done <- err
	}()

	waitToolName(t, started)
	assertNoToolStart(t, started)
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("agent run did not finish")
	}
}

func TestReactAgentRunsStructuredToolBatchConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	first := blockingTool("first", "network", 2, started, release)
	second := blockingTool("second", "network", 2, started, release)
	model := llm.NewScriptedModel(
		[]schema.Message{
			functionCallMessage("first", "call-1"),
			functionCallMessage("second", "call-2"),
		},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{
		Name: "react-parallel", Model: model, Tools: []tool.Tool{first, second},
	}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan agentRunResult, 1)
	go func() {
		messages, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")})
		done <- agentRunResult{messages: messages, err: err}
	}()

	waitToolName(t, started)
	waitToolName(t, started)
	close(release)
	result := waitRunResult(t, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.messages) != 5 {
		t.Fatalf("messages = %#v", result.messages)
	}
	firstResults := result.messages[2].FunctionToolResults()
	if len(firstResults) != 1 || firstResults[0].Name != "first" || firstResults[0].CallID != "call-1" {
		t.Fatalf("first result = %#v", result.messages[2])
	}
	secondResults := result.messages[3].FunctionToolResults()
	if len(secondResults) != 1 || secondResults[0].Name != "second" || secondResults[0].CallID != "call-2" {
		t.Fatalf("second result = %#v", result.messages[3])
	}
}

func TestAgentRejectsConflictingToolGroupLimits(t *testing.T) {
	first := tool.NewFunctionTool(tool.Spec{
		Name: "first", Concurrency: tool.ConcurrencyPolicy{Group: "browser", MaxParallel: 1},
	}, nil)
	second := tool.NewFunctionTool(tool.Spec{
		Name: "second", Concurrency: tool.ConcurrencyPolicy{Group: "browser", MaxParallel: 2},
	}, nil)
	_, err := agent.New(agent.BaseConfig{
		Name: "invalid", Model: llm.NewScriptedModel(), Tools: []tool.Tool{first, second},
	})
	if err == nil || !strings.Contains(err.Error(), "concurrency group \"browser\"") {
		t.Fatalf("agent.New error = %v", err)
	}
}

func TestReactAgentRunsMiddlewareAndCallbackWithSubject(t *testing.T) {
	var middlewareChecked atomic.Bool
	rbac := tool.MiddlewareFuncs{
		MiddlewareName: "rbac",
		HandleFunc: func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
			subject, err := tool.RequireSubject(ctx)
			if err != nil {
				return tool.Result{}, err
			}
			if subject.TenantID != "tenant-a" {
				return tool.Result{}, errors.New("agent tenant mismatch")
			}
			middlewareChecked.Store(true)
			if !spec.IsReadOnly && subject.HasRole("viewer") {
				return tool.Result{}, tool.Deny(tool.ModelVisible(errors.New("viewer cannot call mutating tools")))
			}
			return next(ctx, input)
		},
	}
	var startCalled atomic.Bool
	var errorCalled atomic.Bool
	callbackDone := make(chan struct{}, 1)
	audit := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind != observe.KindTool {
			return
		}
		switch event.Phase {
		case observe.PhaseStart:
			startCalled.Store(true)
		case observe.PhaseError:
			errorCalled.Store(true)
			if errors.Is(event.Err, tool.ErrPermissionDenied) {
				callbackDone <- struct{}{}
			}
		}
	})
	var handlerCalls atomic.Int32
	deleteTool := tool.NewFunctionTool(tool.Spec{Name: "delete"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		handlerCalls.Add(1)
		return tool.TextResult("deleted"), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{functionCallMessage("delete", "delete-1")},
		[]schema.Message{schema.AssistantMessage("denied safely")},
	)
	value, err := agent.New(agent.BaseConfig{
		Name:        "guarded",
		Model:       model,
		Tools:       []tool.Tool{deleteTool},
		Middlewares: []tool.Middleware{rbac},
	})
	if err != nil {
		t.Fatal(err)
	}

	runCtx := observe.With(tool.WithSubject(context.Background(), tool.Subject{
		ID:       "viewer-1",
		TenantID: "tenant-a",
		Roles:    []tool.Role{"viewer"},
	}), audit)
	messages, err := value.Invoke(
		runCtx,
		[]schema.Message{schema.UserMessage("delete it")},
	)
	if err != nil {
		t.Fatal(err)
	}
	if handlerCalls.Load() != 0 {
		t.Fatalf("delete handler calls = %d, want 0", handlerCalls.Load())
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tool callback")
	}
	if !middlewareChecked.Load() || !startCalled.Load() || !errorCalled.Load() {
		t.Fatalf(
			"extension calls: middleware=%t start=%t error=%t",
			middlewareChecked.Load(),
			startCalled.Load(),
			errorCalled.Load(),
		)
	}
	if len(messages) != 3 {
		t.Fatalf("messages = %#v", messages)
	}
	// 中间件名是内部诊断不进模型文本；Deny 理由经 ModelVisible 显式可见。
	if !strings.Contains(toolResultText(messages[1]), `tool "delete" permission denied: viewer cannot call mutating tools`) {
		t.Fatalf("permission observation = %q", toolResultText(messages[1]))
	}
	if messages[1].FunctionToolResults()[0].CallID != "delete-1" || messages[2].JoinedText("") != "denied safely" {
		t.Fatalf("messages = %#v", messages)
	}
}

func TestReactAgentIsolatesNestedToolMetadata(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var failed atomic.Bool
	metadata := map[string]any{"nested": map[string]any{"owner": "original"}}

	makeTool := func(name string) tool.Tool {
		return tool.NewFunctionTool(tool.Spec{
			Name:        name,
			Concurrency: tool.ConcurrencyPolicy{Group: name, MaxParallel: 1},
		}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
			nested := input.Metadata["nested"].(map[string]any)
			nested["owner"] = name
			started <- struct{}{}
			<-release
			if nested["owner"] != name {
				failed.Store(true)
			}
			return tool.TextResult(name), nil
		})
	}
	value, err := agent.New(agent.BaseConfig{
		Name: "metadata-isolation",
		Model: llm.NewScriptedModel(
			[]schema.Message{functionCallMessage("first", "first"), functionCallMessage("second", "second")},
			[]schema.Message{schema.AssistantMessage("done")},
		),
		Tools: []tool.Tool{makeTool("first"), makeTool("second")},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		runCtx := observe.WithMetadata(context.Background(), map[string]any{"nested": metadata["nested"]})
		_, runErr := value.Invoke(runCtx, []schema.Message{schema.UserMessage("run")}, agent.WithMaxParallelTools(2))
		done <- runErr
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("tool did not start")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if failed.Load() {
		t.Fatal("concurrent tools shared nested metadata")
	}
	if metadata["nested"].(map[string]any)["owner"] != "original" {
		t.Fatalf("source metadata mutated: %#v", metadata)
	}
}

func blockingTool(
	name string,
	group string,
	maxParallel int,
	started chan<- string,
	release <-chan struct{},
) tool.Tool {
	return tool.NewFunctionTool(tool.Spec{
		Name: name,
		Concurrency: tool.ConcurrencyPolicy{
			Group:       group,
			MaxParallel: maxParallel,
		},
	}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		started <- name
		select {
		case <-ctx.Done():
			return tool.Result{}, ctx.Err()
		case <-release:
			return tool.TextResult(name + "-done"), nil
		}
	})
}

func functionCallMessage(name, id string) schema.Message {
	return schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(id, name, `{}`))
}

func waitToolName(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case name := <-started:
		return name
	case <-time.After(time.Second):
		t.Fatal("tool did not start")
		return ""
	}
}

func assertNoToolStart(t *testing.T, started <-chan string) {
	t.Helper()
	select {
	case name := <-started:
		t.Fatalf("tool %q exceeded the per-run concurrency limit", name)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitRunResult(t *testing.T, done <-chan agentRunResult) agentRunResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("agent run did not finish")
		return agentRunResult{}
	}
}

type earlyStartModel struct {
	mu                     sync.Mutex
	calls                  int
	allowArgumentsComplete <-chan struct{}
	allowFirstStreamEnd    <-chan struct{}
	partialArgumentsSent   chan struct{}
	firstStreamEnded       chan struct{}
	secondCallStarted      chan struct{}
}

func (m *earlyStartModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return stream.Collect(ctx)
}

func (m *earlyStartModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	resolved, _, err := runnable.ResolveComponentOptions(llm.Options{}, options...)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.calls++
	call := m.calls
	if m.firstStreamEnded == nil {
		m.firstStreamEnded = make(chan struct{})
	}
	if m.partialArgumentsSent == nil {
		m.partialArgumentsSent = make(chan struct{})
	}
	if m.secondCallStarted == nil {
		m.secondCallStarted = make(chan struct{})
	}
	firstEnded := m.firstStreamEnded
	partialSent := m.partialArgumentsSent
	secondStarted := m.secondCallStarted
	m.mu.Unlock()

	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() {
		accumulator := llm.NewDeltaAccumulator(resolved.DeltaStream)
		send := func(response llm.Response) bool {
			messages, err := accumulator.Apply(response)
			if err != nil {
				_ = writer.CloseWithError(err)
				return false
			}
			return writer.Send(llm.Response{Messages: messages}) == nil
		}
		switch call {
		case 1:
			if !send(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.ContentBlock{Type: schema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &schema.FunctionToolCall{ID: "early-", Name: "early-tool", Arguments: `{"value":`}})}}) {
				return
			}
			close(partialSent)
			select {
			case <-ctx.Done():
				_ = writer.CloseWithError(ctx.Err())
				return
			case <-m.allowArgumentsComplete:
			}
			if !send(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.ContentBlock{Type: schema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &schema.FunctionToolCall{ID: "1", Arguments: `1}`}})}}) {
				return
			}
			select {
			case <-ctx.Done():
				_ = writer.CloseWithError(ctx.Err())
				return
			case <-m.allowFirstStreamEnd:
				close(firstEnded)
				_ = writer.Close()
			}
		case 2:
			close(secondStarted)
			if send(llm.Response{Messages: []schema.Message{schema.AssistantMessage("done")}}) {
				_ = writer.Close()
			}
		default:
			_ = writer.CloseWithError(fmt.Errorf("unexpected LLM call %d", call))
		}
	}()
	return reader, nil
}

func (m *earlyStartModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true}
}

func (m *earlyStartModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "early-start", Provider: "test", ModelType: "scripted"}
}
func (m *earlyStartModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m *earlyStartModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}
