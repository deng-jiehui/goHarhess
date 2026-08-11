package agent_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"goharness/pkg/agent"
	"goharness/pkg/callback"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

type lifecycleContextKey struct{}

type unsupportedAgentOption struct{}

func (unsupportedAgentOption) Apply(options *runnable.OptionSet) error {
	options.Values["unsupported"] = true
	return nil
}

type usageEventModel struct{}

func (usageEventModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := (usageEventModel{}).Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return llm.ReduceEvents(ctx, stream, false)
}

func (usageEventModel) Stream(context.Context, llm.Request, ...runnable.Option) (runnable.Stream[llm.Event], error) {
	out := make(chan runnable.Item[llm.Event], 3)
	out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDelta, Messages: []schema.Message{schema.AssistantMessage("ok")}}}
	out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventUsage, Usage: &llm.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5, ReasoningTokens: 1}}}
	out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}
	close(out)
	return out, nil
}

func (usageEventModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (usageEventModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "usage-events"} }

func TestAgentTranslatesCommonRunnableMetadataAndAcceptsUnknownGlobalOptions(t *testing.T) {
	events := make(chan callback.Event, 4)
	observer := callback.Funcs{OnEvent: func(_ context.Context, event callback.Event) error {
		if event.Type == callback.EventAgentStart {
			events <- event
		}
		return nil
	}}
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Model:     llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}),
		Callbacks: []callback.Handler{observer},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), nil, runnable.WithMetadata("tenant", "tenant-a")); err != nil {
		t.Fatal(err)
	}
	event := waitCallbackEvent(t, events)
	if event.Metadata["tenant"] != "tenant-a" {
		t.Fatalf("agent metadata = %#v", event.Metadata)
	}
	if _, err := value.Invoke(context.Background(), nil, unsupportedAgentOption{}); err != nil {
		t.Fatalf("valid global option was rejected: %v", err)
	}
}

func TestCallbackLifecyclePropagatesTraceContextAndMetadata(t *testing.T) {
	var mu sync.Mutex
	var events []callback.Event
	done := make(chan struct{})
	observer := callback.Funcs{
		CallbackName: "monitor",
		OnEvent: func(ctx context.Context, event callback.Event) error {
			if ctx.Value(lifecycleContextKey{}) != "request-value" {
				t.Errorf("event %s lost caller context value", event.Type)
			}
			trace, ok := callback.TraceFromContext(ctx)
			if !ok || trace != event.Trace {
				t.Errorf("event %s context trace = %#v, event trace = %#v", event.Type, trace, event.Trace)
			}
			mu.Lock()
			events = append(events, event.Clone())
			mu.Unlock()
			if event.Type == callback.EventAgentEnd {
				close(done)
			}
			return nil
		},
	}
	search := tool.NewFunctionTool(tool.Spec{Name: "search", IsReadOnly: true}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		trace, ok := callback.TraceFromContext(ctx)
		if !ok || trace.TraceID != "trace-test" {
			t.Fatalf("tool trace = %#v", trace)
		}
		return tool.TextResult("found"), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{functionCallMessage("search", "call-1")},
		[]schema.Message{schema.AssistantMessage("answer")},
	)
	value, err := agent.New(agent.BaseConfig{
		Name:      "assistant",
		Model:     model,
		Tools:     []tool.Tool{search},
		Callbacks: []callback.Handler{observer},
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx := context.WithValue(context.Background(), lifecycleContextKey{}, "request-value")
	runCtx = callback.WithTraceID(runCtx, "trace-test")
	if _, err := value.Invoke(runCtx, []schema.Message{schema.UserMessage("find it")}, agent.WithMetadata("tenant", "tenant-a")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback lifecycle")
	}

	mu.Lock()
	got := append([]callback.Event(nil), events...)
	mu.Unlock()
	counts := map[callback.EventType]int{}
	var agentSpan string
	var graphSpan string
	var runID string
	for _, event := range got {
		counts[event.Type]++
		if event.Trace.TraceID != "trace-test" {
			t.Fatalf("event %s trace ID = %q", event.Type, event.Trace.TraceID)
		}
		if event.Metadata["tenant"] != "tenant-a" {
			t.Fatalf("event %s metadata = %#v", event.Type, event.Metadata)
		}
		if runID == "" {
			runID = event.Trace.RunID
		} else if event.Trace.RunID != runID {
			t.Fatalf("event %s run ID = %q, want %q", event.Type, event.Trace.RunID, runID)
		}
		if event.Type == callback.EventAgentStart {
			agentSpan = event.Trace.SpanID
			if event.Trace.ParentSpanID != "" {
				t.Fatalf("agent parent span = %q", event.Trace.ParentSpanID)
			}
		}
		if event.Type == callback.EventGraphStart {
			graphSpan = event.Trace.SpanID
		}
	}
	for _, event := range got {
		switch event.Component {
		case callback.ComponentGraph:
			if event.Trace.ParentSpanID != agentSpan {
				t.Fatalf("event %s parent span = %q, want agent span %q", event.Type, event.Trace.ParentSpanID, agentSpan)
			}
		case callback.ComponentLLM, callback.ComponentTool, callback.ComponentRetriever:
			if event.Trace.ParentSpanID != graphSpan {
				t.Fatalf("event %s parent span = %q, want graph span %q", event.Type, event.Trace.ParentSpanID, graphSpan)
			}
		}
	}
	if counts[callback.EventAgentStart] != 1 || counts[callback.EventAgentEnd] != 1 || counts[callback.EventAgentError] != 0 {
		t.Fatalf("agent event counts = %#v", counts)
	}
	if counts[callback.EventLLMStart] != 2 || counts[callback.EventLLMEnd] != 2 || counts[callback.EventLLMError] != 0 {
		t.Fatalf("LLM event counts = %#v", counts)
	}
	if counts[callback.EventToolStart] != 1 || counts[callback.EventToolEnd] != 1 || counts[callback.EventToolError] != 0 {
		t.Fatalf("tool event counts = %#v", counts)
	}
}

func TestCallbackLifecycleReportsImmediateLLMAndAgentErrors(t *testing.T) {
	reason := errors.New("provider unavailable")
	events := make(chan callback.Event, 4)
	observer := callback.Funcs{OnEvent: func(_ context.Context, event callback.Event) error {
		if event.Type == callback.EventLLMError || event.Type == callback.EventAgentError {
			events <- event
		}
		return nil
	}}
	model := llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
		return nil, reason
	})
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model, Callbacks: []callback.Handler{observer}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}); !errors.Is(err, reason) {
		t.Fatalf("Invoke error = %v", err)
	}
	first := waitCallbackEvent(t, events)
	second := waitCallbackEvent(t, events)
	if first.Type != callback.EventLLMError || second.Type != callback.EventAgentError {
		t.Fatalf("error lifecycle = %s, %s", first.Type, second.Type)
	}
	if !errors.Is(first.Error, reason) || !errors.Is(second.Error, reason) {
		t.Fatalf("callback errors = %v, %v", first.Error, second.Error)
	}
}

func TestLLMUsageEventUpdatesTotalsWithoutCountingAsChunk(t *testing.T) {
	events := make(chan callback.Event, 16)
	observer := callback.Funcs{OnEvent: func(_ context.Context, event callback.Event) error {
		events <- event
		return nil
	}}
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: usageEventModel{}, Callbacks: []callback.Handler{observer}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}); err != nil {
		t.Fatal(err)
	}
	chunks := 0
	var end callback.Event
	deadline := time.After(time.Second)
	for end.Type == "" {
		select {
		case event := <-events:
			if event.Type == callback.EventLLMChunk {
				chunks++
			}
			if event.Type == callback.EventLLMEnd {
				end = event
			}
		case <-deadline:
			t.Fatal("timed out waiting for LLM end callback")
		}
	}
	if chunks != 1 {
		t.Fatalf("LLM chunk callbacks = %d, want 1", chunks)
	}
	if end.Metrics.Chunks != 1 || end.Metrics.PromptTokens != 3 || end.Metrics.CompletionTokens != 2 || end.Metrics.TotalTokens != 5 || end.Metrics.ReasoningTokens != 1 {
		t.Fatalf("LLM end metrics = %#v", end.Metrics)
	}
}

func TestCallbackFilterChunksKeepsSynchronousTerminalMetrics(t *testing.T) {
	var events []callback.Event
	observer := callback.Funcs{OnEvent: func(_ context.Context, event callback.Event) error {
		events = append(events, event.Clone())
		return nil
	}}
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Model:          usageEventModel{},
		Callbacks:      []callback.Handler{observer},
		CallbackConfig: callback.Config{FilterChunks: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}); err != nil {
		t.Fatal(err)
	}
	var llmEnd, agentEnd *callback.Event
	for index := range events {
		event := events[index]
		switch event.Type {
		case callback.EventLLMChunk:
			t.Fatal("filtered callback received llm.chunk")
		case callback.EventLLMEnd:
			llmEnd = &event
		case callback.EventAgentEnd:
			agentEnd = &event
		}
	}
	if llmEnd == nil || llmEnd.Metrics.TotalTokens != 5 || llmEnd.Metrics.ReasoningTokens != 1 {
		t.Fatalf("llm terminal metrics = %#v", llmEnd)
	}
	if agentEnd == nil {
		t.Fatal("missing synchronous agent.end callback")
	}
}

func TestNestedAgentKeepsLocalCallbacksAndGraphSpansScoped(t *testing.T) {
	var mu sync.Mutex
	var events []callback.Event
	rootDone := make(chan struct{})
	childDone := make(chan struct{})
	var rootOnce sync.Once
	var childOnce sync.Once
	rootObserver := callback.Funcs{OnEvent: func(_ context.Context, event callback.Event) error {
		mu.Lock()
		events = append(events, event.Clone())
		mu.Unlock()
		if event.Type == callback.EventAgentEnd && event.Name == "team" {
			rootOnce.Do(func() { close(rootDone) })
		}
		return nil
	}}
	childObserver := callback.Funcs{OnEvent: func(_ context.Context, event callback.Event) error {
		if event.Type == callback.EventAgentEnd && event.Name == "worker" {
			childOnce.Do(func() { close(childDone) })
		}
		return nil
	}}
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker", Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("done")}),
		Callbacks: []callback.Handler{childObserver},
	})
	if err != nil {
		t.Fatal(err)
	}
	team, err := agent.NewGroupChat(agent.GroupChatConfig{
		BaseConfig: agent.BaseConfig{Name: "team", Callbacks: []callback.Handler{rootObserver}},
		Agents:     []agent.Agent{worker},
		MaxRounds:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := team.Invoke(context.Background(), []schema.Message{schema.UserMessage("delegate")}); err != nil {
		t.Fatal(err)
	}
	for _, done := range []<-chan struct{}{rootDone, childDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("nested callback was not delivered")
		}
	}
	mu.Lock()
	got := append([]callback.Event(nil), events...)
	mu.Unlock()
	childGraphEnd, teamGraphEnd, childAgentEnd := -1, -1, -1
	for index, event := range got {
		switch {
		case event.Type == callback.EventGraphEnd && event.Name == "worker.graph":
			childGraphEnd = index
		case event.Type == callback.EventGraphEnd && event.Name == "team.graph":
			teamGraphEnd = index
		case event.Type == callback.EventAgentEnd && event.Name == "worker":
			childAgentEnd = index
		}
	}
	if childGraphEnd < 0 || teamGraphEnd < 0 || childAgentEnd < 0 {
		t.Fatalf("missing nested lifecycle events: %#v", got)
	}
	if !(childGraphEnd < childAgentEnd && childAgentEnd < teamGraphEnd) {
		t.Fatalf("graph spans closed out of scope: childGraph=%d childAgent=%d teamGraph=%d", childGraphEnd, childAgentEnd, teamGraphEnd)
	}
}

func waitCallbackEvent(t *testing.T, events <-chan callback.Event) callback.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback event")
		return callback.Event{}
	}
}
