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

	"goharness/pkg/agent"
	"goharness/pkg/callback"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

type agentRunResult struct {
	messages []schema.Message
	err      error
}

func TestReactAgentStartsToolBeforeLLMStreamEnds(t *testing.T) {
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
	if name := waitToolName(t, toolStarted); name != "early-tool" {
		t.Fatalf("started tool = %q", name)
	}
	select {
	case <-firstStreamEnded:
		t.Fatal("tool started only after LLM stream ended")
	default:
	}
	close(allowToolEnd)
	select {
	case <-secondCallStarted:
		t.Fatal("agent called LLM again before the first stream ended")
	case <-time.After(50 * time.Millisecond):
	}
	close(allowStreamEnd)

	result := waitRunResult(t, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.messages) != 3 || result.messages[1].Name != "early-tool" || result.messages[2].Text() != "done" {
		t.Fatalf("messages = %#v", result.messages)
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
	if !strings.Contains(last[1].Text(), "context deadline exceeded") || last[1].FunctionID("") != "slow-1" {
		t.Fatalf("timeout observation = %#v", last[1])
	}
	if last[2].Text() != "continued" {
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
	if result.messages[2].Name != "first" || result.messages[2].FunctionID("") != "call-1" {
		t.Fatalf("first result = %#v", result.messages[2])
	}
	if result.messages[3].Name != "second" || result.messages[3].FunctionID("") != "call-2" {
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
	var chunks [][]schema.Message
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Value.Kind != schema.MessageEventDone {
			chunks = append(chunks, item.Value.Messages)
		}
	}
	observationBatches := 0
	for _, messages := range chunks {
		functions := 0
		for _, message := range messages {
			if message.Role == schema.RoleFunction {
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
	if observationBatches != 2 {
		// The cumulative final chunk includes the same complete batch; no partial one-tool batch is allowed.
		t.Fatalf("observation-bearing chunks = %d, want 2 cumulative chunks: %#v", observationBatches, chunks)
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
	if result.messages[2].Name != "first" || result.messages[2].FunctionID("") != "call-1" {
		t.Fatalf("first result = %#v", result.messages[2])
	}
	if result.messages[3].Name != "second" || result.messages[3].FunctionID("") != "call-2" {
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
				return tool.Result{}, tool.Deny(errors.New("viewer cannot call mutating tools"))
			}
			return next(ctx, input)
		},
	}
	var startCalled atomic.Bool
	var errorCalled atomic.Bool
	callbackDone := make(chan struct{}, 1)
	audit := callback.Funcs{
		CallbackName: "audit",
		OnToolStart: func(context.Context, callback.Event) error {
			startCalled.Store(true)
			return nil
		},
		OnToolError: func(_ context.Context, event callback.Event) error {
			errorCalled.Store(true)
			if !errors.Is(event.Error, tool.ErrPermissionDenied) {
				return fmt.Errorf("audit expected permission denial, got %v", event.Error)
			}
			callbackDone <- struct{}{}
			return nil
		},
	}
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
		Callbacks:   []callback.Callback{audit},
	})
	if err != nil {
		t.Fatal(err)
	}

	runCtx := tool.WithSubject(context.Background(), tool.Subject{
		ID:       "viewer-1",
		TenantID: "tenant-a",
		Roles:    []tool.Role{"viewer"},
	})
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
	if !strings.Contains(messages[1].Text(), `tool "delete" permission denied by middleware "rbac": viewer cannot call mutating tools`) {
		t.Fatalf("permission observation = %q", messages[1].Text())
	}
	if messages[1].FunctionID("") != "delete-1" || messages[2].Text() != "denied safely" {
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
		_, runErr := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, agent.WithMaxParallelTools(2), agent.WithMetadata("nested", metadata["nested"]))
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
	return schema.Message{
		Role:         schema.RoleAssistant,
		FunctionCall: &schema.FunctionCall{Name: name, Arguments: `{}`},
		Extra:        map[string]any{"function_id": id},
	}
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
	return llm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *earlyStartModel) Stream(ctx context.Context, request llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
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

	out := make(chan runnable.Item[llm.Event])
	go func() {
		defer close(out)
		switch call {
		case 1:
			select {
			case <-ctx.Done():
				out <- runnable.Item[llm.Event]{Err: ctx.Err()}
				return
			case out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDelta, Messages: []schema.Message{{
				Role: schema.RoleAssistant,
				FunctionCall: &schema.FunctionCall{
					Name:      "early-tool",
					Arguments: `{"value":`,
				},
				Extra: map[string]any{"function_id": "early-1"},
			}}}}:
			}
			close(partialSent)
			select {
			case <-ctx.Done():
				out <- runnable.Item[llm.Event]{Err: ctx.Err()}
				return
			case <-m.allowArgumentsComplete:
			}
			select {
			case <-ctx.Done():
				out <- runnable.Item[llm.Event]{Err: ctx.Err()}
				return
			case out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDelta, Messages: []schema.Message{{
				Role:         schema.RoleAssistant,
				FunctionCall: &schema.FunctionCall{Arguments: `1}`},
			}}}}:
			}
			select {
			case <-ctx.Done():
				out <- runnable.Item[llm.Event]{Err: ctx.Err()}
			case <-m.allowFirstStreamEnd:
				close(firstEnded)
				out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
			}
		case 2:
			close(secondStarted)
			out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDelta, Messages: []schema.Message{schema.AssistantMessage("done")}}}
			out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
		default:
			out <- runnable.Item[llm.Event]{Err: fmt.Errorf("unexpected LLM call %d", call)}
		}
	}()
	return out, nil
}

func (m *earlyStartModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true, ToolCalls: true}
}

func (m *earlyStartModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "early-start", Provider: "test", ModelType: "scripted"}
}
