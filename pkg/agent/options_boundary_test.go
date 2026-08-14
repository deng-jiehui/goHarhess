package agent_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/contextengine"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

type lifecycleKey struct{}

type boundaryModel struct {
	mu          sync.Mutex
	optionCount int
	contextSeen bool
	globalSeen  bool
	nodeSeen    bool
}

type boundaryTool struct {
	mu          sync.Mutex
	optionCount int
	globalSeen  bool
	nodeSeen    bool
	contextLeak bool
	contextNode bool
}

func (*boundaryTool) Spec() tool.Spec { return tool.Spec{Name: "boundary-tool", IsReadOnly: true} }

func (t *boundaryTool) Invoke(ctx context.Context, _ tool.CallInput, options ...runnable.Option) (tool.Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.optionCount += len(options)
	for _, option := range options {
		switch runnable.ScopeOf(option) {
		case runnable.OptionScopeGlobal:
			t.globalSeen = true
		case runnable.OptionScopeNode:
			t.nodeSeen = true
		}
	}
	if plan, ok := runnable.OptionPlanFromContext(ctx); ok {
		set := plan.Set()
		t.contextLeak = set.Values["request_lifecycle"] != nil
		t.contextNode = set.Values["provider_mode"] == "fast"
	}
	return tool.TextResult("ok"), nil
}
func (t *boundaryTool) Collect(ctx context.Context, in *runnable.StreamReader[tool.CallInput], opts ...runnable.Option) (tool.Result, error) {
	return runnable.CollectFromInvoke(ctx, in, t.Invoke, opts...)
}
func (t *boundaryTool) Transform(ctx context.Context, in *runnable.StreamReader[tool.CallInput], opts ...runnable.Option) (*runnable.StreamReader[tool.Result], error) {
	return runnable.TransformFromInvoke(ctx, in, t.Invoke, opts...)
}
func (t *boundaryTool) Stream(ctx context.Context, input tool.CallInput, opts ...runnable.Option) (*runnable.StreamReader[tool.Result], error) {
	return runnable.StreamFromInvoke(ctx, input, t.Invoke, opts...)
}

func (m *boundaryModel) Invoke(ctx context.Context, _ llm.Request, options ...runnable.Option) (llm.Response, error) {
	m.mu.Lock()
	m.optionCount += len(options)
	m.contextSeen = ctx.Value(lifecycleKey{}) == "request-lifecycle"
	for _, option := range options {
		switch runnable.ScopeOf(option) {
		case runnable.OptionScopeGlobal:
			m.globalSeen = true
		case runnable.OptionScopeNode:
			m.nodeSeen = true
		}
	}
	m.mu.Unlock()
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}}, nil
}

func (m *boundaryModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	response, err := m.Invoke(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() { _ = writer.Send(response); _ = writer.Close() }()
	return reader, nil
}

func (*boundaryModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*boundaryModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "boundary"} }
func (m *boundaryModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m *boundaryModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}

func TestLifecycleContextDoesNotBecomeModelOption(t *testing.T) {
	model := &boundaryModel{}
	contextSeen := false
	engine := runnable.Func[contextengine.Request, contextengine.Result](func(ctx context.Context, request contextengine.Request, options ...runnable.Option) (contextengine.Result, error) {
		if ctx.Value(lifecycleKey{}) != "request-lifecycle" {
			t.Error("context engine lost request lifecycle context")
		}
		if len(options) != 1 {
			return contextengine.Result{}, fmt.Errorf("context engine received %d options, want 1", len(options))
		}
		contextSeen = runnable.ScopeOf(options[0]) == runnable.OptionScopeGlobal
		return contextengine.Result{Messages: schema.CloneMessages(request.Input)}, nil
	})
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model, ContextBuilder: engine})
	if err != nil {
		t.Fatal(err)
	}
	global := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) {
		set.Values["request_lifecycle"] = "must-stay-framework-context"
	}))
	node := runnable.NodeOption(runnable.OptionFunc(func(set *runnable.OptionSet) {
		set.Values["provider_mode"] = "fast"
	}))
	ctx := context.WithValue(context.Background(), lifecycleKey{}, "request-lifecycle")
	if _, err := value.Invoke(ctx, nil, global, node); err != nil {
		t.Fatal(err)
	}
	if !contextSeen {
		t.Fatal("context engine did not receive the global option")
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	if model.optionCount != 1 || model.globalSeen || !model.nodeSeen || !model.contextSeen {
		t.Fatalf("model boundary optionCount=%d globalSeen=%t nodeSeen=%t contextSeen=%t", model.optionCount, model.globalSeen, model.nodeSeen, model.contextSeen)
	}
}

func TestAgentToolBoundaryReceivesOnlyNodeOptions(t *testing.T) {
	valueTool := &boundaryTool{}
	value, err := agent.New(agent.BaseConfig{
		Model: llm.NewScriptedModel(
			[]schema.Message{functionCallMessage("boundary-tool", "call-1")},
			[]schema.Message{schema.AssistantMessage("done")},
		),
		Tools: []tool.Tool{valueTool},
	})
	if err != nil {
		t.Fatal(err)
	}
	global := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) {
		set.Values["request_lifecycle"] = "framework-only"
	}))
	node := runnable.NodeOption(runnable.OptionFunc(func(set *runnable.OptionSet) {
		set.Values["provider_mode"] = "fast"
	}))
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, global, node); err != nil {
		t.Fatal(err)
	}
	valueTool.mu.Lock()
	defer valueTool.mu.Unlock()
	if valueTool.optionCount != 1 || valueTool.globalSeen || !valueTool.nodeSeen || valueTool.contextLeak || !valueTool.contextNode {
		t.Fatalf(
			"tool boundary optionCount=%d globalSeen=%t nodeSeen=%t contextLeak=%t contextNode=%t",
			valueTool.optionCount,
			valueTool.globalSeen,
			valueTool.nodeSeen,
			valueTool.contextLeak,
			valueTool.contextNode,
		)
	}
}
