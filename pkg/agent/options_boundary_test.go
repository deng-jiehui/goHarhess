package agent_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// probeConfig 是一个自定义组件配置类型，用来验证按类型归属的分发。
type probeConfig struct {
	Mode string
}

func withProbeMode(mode string) runnable.Option {
	return runnable.WrapOption(func(c *probeConfig) { c.Mode = mode })
}

// probeBuilder 声明接收 probeConfig，验证 Builder 声明类型的选项直达它。
type probeBuilder struct {
	seen atomic.Int32
}

func (b *probeBuilder) OptionTypes() []reflect.Type {
	return []reflect.Type{runnable.OptionType[probeConfig]()}
}

func (b *probeBuilder) Invoke(ctx context.Context, request contextengine.Request, options ...runnable.Option) (contextengine.Result, error) {
	resolved, remaining, err := runnable.ResolveComponentOptions(probeConfig{}, options...)
	if err != nil {
		return contextengine.Result{}, err
	}
	if resolved.Mode == "hit" {
		b.seen.Add(1)
	}
	for _, option := range remaining {
		specs, ferr := runnable.FlattenOptions([]runnable.Option{option})
		if ferr == nil && len(specs) == 1 && specs[0].ConfigType == runnable.OptionType[llm.Options]() {
			return contextengine.Result{}, fmt.Errorf("builder received llm option")
		}
	}
	if callback.MetadataFromContext(ctx)["tenant"] != "acme" {
		return contextengine.Result{}, fmt.Errorf("builder lost scoped metadata")
	}
	return contextengine.Result{Messages: schema.CloneMessages(request.Input)}, nil
}
func (b *probeBuilder) Stream(ctx context.Context, in contextengine.Request, opts ...runnable.Option) (*runnable.StreamReader[contextengine.Result], error) {
	return runnable.StreamFromInvoke(ctx, in, b.Invoke, opts...)
}
func (b *probeBuilder) Collect(ctx context.Context, in *runnable.StreamReader[contextengine.Request], opts ...runnable.Option) (contextengine.Result, error) {
	return runnable.CollectFromInvoke(ctx, in, b.Invoke, opts...)
}
func (b *probeBuilder) Transform(ctx context.Context, in *runnable.StreamReader[contextengine.Request], opts ...runnable.Option) (*runnable.StreamReader[contextengine.Result], error) {
	return runnable.TransformFromInvoke(ctx, in, b.Invoke, opts...)
}

// probeModel 解析 llm.Options，同时校验没有收到不属于它的配置类型。
type probeModel struct {
	mu          sync.Mutex
	deltaStream bool
	provider    string
	maxTokens   any
}

func (m *probeModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	resolved, _, err := runnable.ResolveComponentOptions(llm.Options{}, options...)
	if err != nil {
		return llm.Response{}, err
	}
	m.mu.Lock()
	m.deltaStream = resolved.DeltaStream
	m.provider, _ = resolved.ExtraGenerateConfig["provider_mode"].(string)
	m.maxTokens = resolved.ExtraGenerateConfig["max_tokens"]
	m.mu.Unlock()
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}}, nil
}
func (m *probeModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	response, err := m.Invoke(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() { _ = writer.Send(response); _ = writer.Close() }()
	return reader, nil
}
func (*probeModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*probeModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "probe"} }
func (m *probeModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m *probeModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}

// 组件选项按类型归属分发：Builder 声明的类型直达 Builder，llm.Options 经
// 配方图广播到模型节点；追踪元数据经 ctx 继承到各组件作用域。
func TestComponentOptionsDispatchByType(t *testing.T) {
	builder := &probeBuilder{}
	model := &probeModel{}
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model, ContextBuilder: builder})
	if err != nil {
		t.Fatal(err)
	}
	runCtx := callback.WithMetadata(context.Background(), map[string]any{"tenant": "acme"})
	_, err = value.Invoke(runCtx, []schema.Message{schema.UserMessage("hi")},
		withProbeMode("hit"),
		llm.WithDeltaStream(true),
		llm.WithGenerateConfig(llm.GenerateConfig{"provider_mode": "fast"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if builder.seen.Load() != 1 {
		t.Fatal("builder did not receive its declared option type")
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	if !model.deltaStream || model.provider != "fast" {
		t.Fatalf("model options delta=%t provider=%q", model.deltaStream, model.provider)
	}
}

// 无接收者的未声明类型在入口明确报错，而不是被静默吞掉。
func TestUnknownOptionTypeFailsAtEntry(t *testing.T) {
	type unknownConfig struct{ V int }
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")})})
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")},
		runnable.WrapOption(func(c *unknownConfig) { c.V = 1 }),
	)
	if err == nil || !strings.Contains(err.Error(), "no receiver") {
		t.Fatalf("Invoke error = %v", err)
	}
}

// 定向选项按节点路径到达配方图内部节点。
func TestForNodeReachesRecipeModelNode(t *testing.T) {
	model := &probeModel{}
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")},
		runnable.ForNode("llm", llm.WithDeltaStream(true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	if !model.deltaStream {
		t.Fatal("targeted llm option did not reach the model node")
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")},
		runnable.ForNode("missing", llm.WithDeltaStream(true)),
	); err == nil || !strings.Contains(err.Error(), "unknown node") {
		t.Fatalf("Invoke error = %v", err)
	}
}

// 定向到 tools 节点的选项经节点透传到每个工具调用；未定向的自定义类型
// 不会被工具节点误收（无接收者会在入口报错）。
type probeTool struct {
	mu   sync.Mutex
	seen string
}

func (*probeTool) Spec() tool.Spec { return tool.Spec{Name: "probe-tool", IsReadOnly: true} }

func (t *probeTool) Invoke(ctx context.Context, _ tool.CallInput, options ...runnable.Option) (tool.Result, error) {
	resolved, _, err := runnable.ResolveComponentOptions(probeConfig{}, options...)
	if err != nil {
		return tool.Result{}, err
	}
	t.mu.Lock()
	t.seen = resolved.Mode
	t.mu.Unlock()
	return tool.TextResult("ok"), nil
}
func (t *probeTool) Collect(ctx context.Context, in *runnable.StreamReader[tool.CallInput], opts ...runnable.Option) (tool.Result, error) {
	return runnable.CollectFromInvoke(ctx, in, t.Invoke, opts...)
}
func (t *probeTool) Transform(ctx context.Context, in *runnable.StreamReader[tool.CallInput], opts ...runnable.Option) (*runnable.StreamReader[tool.Result], error) {
	return runnable.TransformFromInvoke(ctx, in, t.Invoke, opts...)
}
func (t *probeTool) Stream(ctx context.Context, input tool.CallInput, opts ...runnable.Option) (*runnable.StreamReader[tool.Result], error) {
	return runnable.StreamFromInvoke(ctx, input, t.Invoke, opts...)
}

func TestTargetedOptionsReachToolCalls(t *testing.T) {
	valueTool := &probeTool{}
	value, err := agent.New(agent.BaseConfig{
		Model: llm.NewScriptedModel(
			[]schema.Message{functionCallMessage("probe-tool", "call-1")},
			[]schema.Message{schema.AssistantMessage("done")},
		),
		Tools: []tool.Tool{valueTool},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")},
		runnable.ForNode("tools", withProbeMode("hit")),
	); err != nil {
		t.Fatal(err)
	}
	valueTool.mu.Lock()
	defer valueTool.mu.Unlock()
	if valueTool.seen != "hit" {
		t.Fatalf("tool option = %q", valueTool.seen)
	}
}

// budgetBuilder 记录收到的预留输出预算，验证它与模型有效配置一致。
type budgetBuilder struct {
	reserved atomic.Int32
}

func (b *budgetBuilder) Invoke(_ context.Context, request contextengine.Request, _ ...runnable.Option) (contextengine.Result, error) {
	b.reserved.Store(int32(request.ReservedOutputTokens))
	return contextengine.Result{Messages: schema.CloneMessages(request.Input)}, nil
}
func (b *budgetBuilder) Stream(ctx context.Context, in contextengine.Request, opts ...runnable.Option) (*runnable.StreamReader[contextengine.Result], error) {
	return runnable.StreamFromInvoke(ctx, in, b.Invoke, opts...)
}
func (b *budgetBuilder) Collect(ctx context.Context, in *runnable.StreamReader[contextengine.Request], opts ...runnable.Option) (contextengine.Result, error) {
	return runnable.CollectFromInvoke(ctx, in, b.Invoke, opts...)
}
func (b *budgetBuilder) Transform(ctx context.Context, in *runnable.StreamReader[contextengine.Request], opts ...runnable.Option) (*runnable.StreamReader[contextengine.Result], error) {
	return runnable.TransformFromInvoke(ctx, in, b.Invoke, opts...)
}

// 上下文预算取裁剪前解析出的有效模型配置：调用方 llm.WithMaxTokens(128)
// 覆盖 Agent 的 32，Builder 与模型必须同时看到 128。
func TestContextBudgetUsesEffectiveModelConfig(t *testing.T) {
	builder := &budgetBuilder{}
	model := &probeModel{}
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Model: model, ContextBuilder: builder, MaxTokens: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")},
		llm.WithMaxTokens(128),
	); err != nil {
		t.Fatal(err)
	}
	if got := builder.reserved.Load(); got != 128 {
		t.Fatalf("reserved output tokens = %d, want 128", got)
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	if model.maxTokens != 128 {
		t.Fatalf("model max_tokens = %#v, want 128", model.maxTokens)
	}
}

// providerModel 声明自己的扩展配置类型，验证 Provider 专属配置经
// ChatModelNode 与 Agent 的声明透传到达模型。
type providerConfig struct {
	Tier string
}

type providerModel struct {
	mu   sync.Mutex
	tier string
}

func (*providerModel) OptionTypes() []reflect.Type {
	return []reflect.Type{runnable.OptionType[providerConfig]()}
}
func (m *providerModel) Invoke(_ context.Context, _ llm.Request, options ...runnable.Option) (llm.Response, error) {
	// 真实 Provider 同时接收通用模型配置与自有扩展类型。
	_, rest, err := runnable.ResolveComponentOptions(llm.Options{}, options...)
	if err != nil {
		return llm.Response{}, err
	}
	resolved, remaining, err := runnable.ResolveComponentOptions(providerConfig{}, rest...)
	if err != nil {
		return llm.Response{}, err
	}
	if err := runnable.CheckNoRemainingOptions(remaining...); err != nil {
		return llm.Response{}, err
	}
	m.mu.Lock()
	m.tier = resolved.Tier
	m.mu.Unlock()
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}}, nil
}
func (m *providerModel) Stream(ctx context.Context, in llm.Request, opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.StreamFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m *providerModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m *providerModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}
func (*providerModel) Capabilities() llm.Capabilities { return llm.Capabilities{} }
func (*providerModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "provider"} }

func TestProviderExtensionOptionsReachCustomModel(t *testing.T) {
	model := &providerModel{}
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")},
		runnable.WrapOption(func(c *providerConfig) { c.Tier = "deep" }),
	); err != nil {
		t.Fatal(err)
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	if model.tier != "deep" {
		t.Fatalf("provider tier = %q", model.tier)
	}
}

// 定向到配方模型节点的 llm.Options 在 Agent 边界冻结一次：有效配置与
// 模型节点应用同一份快照，非幂等设置函数不会在节点端重放第二次。
func TestTargetedModelOptionAppliesSnapshotOnce(t *testing.T) {
	model := &probeModel{}
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	applies := 0
	targeted := runnable.ForNode("llm", runnable.WrapOption(func(c *llm.Options) {
		applies++
		if c.ExtraGenerateConfig == nil {
			c.ExtraGenerateConfig = llm.GenerateConfig{}
		}
		c.ExtraGenerateConfig["provider_mode"] = fmt.Sprintf("call-%d", applies)
	}))
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}, targeted); err != nil {
		t.Fatal(err)
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	// 边界解析产出的 call-1 快照是节点收到的值；若设置函数在节点端重放，
	// 计数推进后模型会看到 call-2 或更晚的值。
	if model.provider != "call-1" {
		t.Fatalf("model provider_mode = %q, targeted setter re-executed at node", model.provider)
	}
	// 原始设置函数只在边界冻结时执行一次，快照应用与节点应用都不再触发。
	if applies != 1 {
		t.Fatalf("targeted setter executed %d times, want 1", applies)
	}
}

// 父 Agent 的模型配置不跨越 AgentNode 边界：父级 llm.WithMaxTokens 只影响
// 自己的模型节点，子 Agent 保留自己的派生配置。
func TestParentModelOptionsDoNotReachSubAgents(t *testing.T) {
	childModel := &probeModel{}
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker", Model: childModel, MaxTokens: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "router",
			Model: llm.NewScriptedModel([]schema.Message{routingCall("worker")}),
		},
		SubAgents:     []agent.Agent{worker},
		ReturnControl: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("delegate")},
		llm.WithMaxTokens(128),
	); err != nil {
		t.Fatal(err)
	}
	childModel.mu.Lock()
	defer childModel.mu.Unlock()
	if childModel.maxTokens != 32 {
		t.Fatalf("child max_tokens = %#v, want its own 32", childModel.maxTokens)
	}
}
