package agent_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/callback"
	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

type usageEventModel struct{}

func (usageEventModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := (usageEventModel{}).Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return stream.Collect(ctx)
}
func (usageEventModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() {
		response := llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}, Usage: &llm.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5, ReasoningTokens: 1}}
		callback.EmitStream(ctx, response.Clone())
		_ = writer.Send(response)
		_ = writer.Close()
	}()
	return reader, nil
}
func (usageEventModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (usageEventModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "usage-events"} }
func (m usageEventModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m usageEventModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}

func TestAgentDispatchesTypedGraphLLMAndToolCallbacksFromContext(t *testing.T) {
	var mu sync.Mutex
	var graphInvocations, llmInvocations, toolInvocations []callback.Invocation
	graphObserver := graph.CallbackFuncs{CallbackName: "graph", GraphStart: func(ctx context.Context, _ graph.RunInfo) error {
		invocation, _ := callback.InvocationFromContext(ctx)
		mu.Lock()
		graphInvocations = append(graphInvocations, invocation)
		mu.Unlock()
		return nil
	}, NodeStart: func(ctx context.Context, _ graph.NodeExecution) error {
		invocation, _ := callback.InvocationFromContext(ctx)
		mu.Lock()
		graphInvocations = append(graphInvocations, invocation)
		mu.Unlock()
		return nil
	}}
	llmObserver := llm.CallbackFuncs{CallbackName: "llm", Start: func(ctx context.Context, _ llm.Request) error {
		invocation, _ := callback.InvocationFromContext(ctx)
		mu.Lock()
		llmInvocations = append(llmInvocations, invocation)
		mu.Unlock()
		return nil
	}, End: func(ctx context.Context, response llm.Response) error {
		if len(response.Messages) == 0 {
			t.Error("LLM End callback received no response messages")
		}
		return nil
	}}
	toolObserver := tool.CallbackFuncs{CallbackName: "tool", Start: func(ctx context.Context, _ tool.Spec, _ tool.CallInput) error {
		invocation, _ := callback.InvocationFromContext(ctx)
		mu.Lock()
		toolInvocations = append(toolInvocations, invocation)
		mu.Unlock()
		return nil
	}}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{graphObserver, llmObserver, toolObserver})
	if err != nil {
		t.Fatal(err)
	}
	search := tool.NewFunctionTool(tool.Spec{Name: "search", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("found"), nil })
	model := llm.NewScriptedModel([]schema.Message{functionCallMessage("search", "call-1")}, []schema.Message{schema.AssistantMessage("answer")})
	value, err := agent.New(agent.BaseConfig{Name: "assistant", Model: model, Tools: []tool.Tool{search}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithManager(callback.WithTraceID(context.Background(), "trace-test"), manager)
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("find")}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(graphInvocations) == 0 || len(llmInvocations) != 2 || len(toolInvocations) != 1 {
		t.Fatalf("callbacks graph=%d llm=%d tool=%d", len(graphInvocations), len(llmInvocations), len(toolInvocations))
	}
	for _, invocation := range append(append(graphInvocations, llmInvocations...), toolInvocations...) {
		if invocation.Trace.TraceID != "trace-test" {
			t.Fatalf("trace = %#v", invocation.Trace)
		}
	}
	if toolInvocations[0].Trace.ParentSpanID == "" || llmInvocations[0].Trace.ParentSpanID == "" {
		t.Fatal("component callbacks are missing node parent spans")
	}
}

func TestAgentLLMErrorCallbackDoesNotReplaceBusinessError(t *testing.T) {
	reason := errors.New("provider unavailable")
	seen := make(chan error, 1)
	observer := llm.CallbackFuncs{CallbackName: "llm", Error: func(_ context.Context, err error) error { seen <- err; return errors.New("observer unavailable") }}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	model := llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) { return nil, reason })
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithManager(context.Background(), manager)
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("hello")}); !errors.Is(err, reason) {
		t.Fatalf("Invoke error=%v", err)
	}
	if err := <-seen; !errors.Is(err, reason) {
		t.Fatalf("callback error=%v", err)
	}
}

func TestAgentSendsLLMChunksOnlyToEventStreamAndUsageToEnd(t *testing.T) {
	chunks := 0
	var usage *llm.Usage
	observer := llm.CallbackFuncs{CallbackName: "llm", End: func(_ context.Context, response llm.Response) error { usage = response.Usage; return nil }}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	ctx := callback.WithManager(context.Background(), manager)
	ctx = callback.WithStreamCallbacks(ctx, callback.StreamCallbackOf[llm.Response]("ui", func(_ context.Context, response llm.Response) error {
		chunks++
		if len(response.Messages) == 0 {
			t.Error("empty chunk")
		}
		return nil
	}))
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: usageEventModel{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("hello")}); err != nil {
		t.Fatal(err)
	}
	if chunks != 1 {
		t.Fatalf("EventStream chunks=%d", chunks)
	}
	if usage == nil || usage.TotalTokens != 5 || usage.ReasoningTokens != 1 {
		t.Fatalf("usage=%#v", usage)
	}
}
