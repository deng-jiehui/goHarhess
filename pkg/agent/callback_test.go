package agent_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
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

func TestAgentEmitsGraphLLMAndToolEventsWithSharedTrace(t *testing.T) {
	var mu sync.Mutex
	var runEvents, nodeEvents, llmEvents, toolEvents []observe.Event
	collector := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		mu.Lock()
		defer mu.Unlock()
		switch event.Kind {
		case observe.KindRun:
			runEvents = append(runEvents, event)
		case observe.KindNode:
			nodeEvents = append(nodeEvents, event)
		case observe.KindLLM:
			llmEvents = append(llmEvents, event)
		case observe.KindTool:
			toolEvents = append(toolEvents, event)
		}
	})
	search := tool.NewFunctionTool(tool.Spec{Name: "search", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("found"), nil })
	model := llm.NewScriptedModel([]schema.Message{functionCallMessage("search", "call-1")}, []schema.Message{schema.AssistantMessage("answer")})
	value, err := agent.New(model, agent.WithName("assistant"), agent.WithTools(search))
	if err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(observe.WithTraceID(context.Background(), "trace-test"), collector)
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("find")}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(runEvents) == 0 || len(nodeEvents) == 0 || len(llmEvents) < 2 || len(toolEvents) == 0 {
		t.Fatalf("events run=%d node=%d llm=%d tool=%d", len(runEvents), len(nodeEvents), len(llmEvents), len(toolEvents))
	}
	all := append(append(append(runEvents, nodeEvents...), llmEvents...), toolEvents...)
	for _, event := range all {
		if event.Trace.TraceID != "trace-test" {
			t.Fatalf("trace = %#v", event.Trace)
		}
	}
	if toolEvents[0].Trace.ParentSpanID == "" || llmEvents[0].Trace.ParentSpanID == "" {
		t.Fatal("component events are missing node parent spans")
	}
}

func TestAgentLLMErrorEventDoesNotReplaceBusinessError(t *testing.T) {
	reason := errors.New("provider unavailable")
	seen := make(chan error, 1)
	collector := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind == observe.KindLLM && event.Phase == observe.PhaseError {
			seen <- event.Err
		}
	})
	model := llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) { return nil, reason })
	value, err := agent.New(model)
	if err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(context.Background(), collector)
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("hello")}); !errors.Is(err, reason) {
		t.Fatalf("Invoke error=%v", err)
	}
	if err := <-seen; !errors.Is(err, reason) {
		t.Fatalf("event error=%v", err)
	}
}

func TestAgentLLMUsageReachesEndEvent(t *testing.T) {
	var usage *llm.Usage
	collector := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind == observe.KindLLM && event.Phase == observe.PhaseEnd {
			usage = event.Usage
		}
	})
	ctx := observe.With(context.Background(), collector)
	value, err := agent.New(usageEventModel{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("hello")}); err != nil {
		t.Fatal(err)
	}
	if usage == nil || usage.TotalTokens != 5 || usage.ReasoningTokens != 1 {
		t.Fatalf("usage=%#v", usage)
	}
}
