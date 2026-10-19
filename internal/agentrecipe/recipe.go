// Package agentrecipe contains strongly typed Graph recipes used by Agents.
//
// 每个配方的可变运行状态只存在于其 Graph State 中，Handler 直接读写 State；
// Context 只携带按配方实例隔离的只读 RunConfig，嵌套配方之间互不可见。
package agentrecipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

// stateCodecVersion 在配方 State 结构变化时递增；旧版本续点恢复时会明确报错。
// v5：会话外部化——History 在持久化快照中改为 history_len 引用，
// 并保存本轮输入边界，供模型上下文压缩区分历史与当前工作集。
const stateCodecVersion = 5

// ModelNodeID 是配方图中模型节点的固定 ID，供上层按路径定向模型选项。
const ModelNodeID = "llm"

type Call struct {
	Name       string
	Arguments  string
	FunctionID string
	Thought    string
	Structured bool
}

type ToolResult struct {
	Call          Call
	ContentBlocks []schema.ContentBlock
	Err           error
}

// RunConfig 是一次调用的只读配置，不进入 State，也不会被 Handler 修改。
// Input 与 MaxLLMCalls 只在新运行创建 State 时使用；恢复运行从 State 读取。
type RunConfig struct {
	// Input 是调用方本次的原始输入，用于会话提交；为 nil 时使用 Graph 输入。
	Input []schema.Message
	// MaxLLMCalls 是本次运行的模型调用预算，只有 tool 配方使用。
	MaxLLMCalls int
	// History 保存未经模型上下文裁剪的完整会话。
	History []schema.Message
}

// baseState 是所有配方 State 的公共部分。
// externalConv 是运行期标记：持久化边界调用 MarkConversationExternal 后，
// 序列化用 history_len 引用代替完整 History，消除每步重复保存会话。
// historyLen 仅在解码出的外部化快照中暂存引用长度，RestoreConversation
// 回填后清零。两个字段都不参与线格式。
type baseState struct {
	History      []schema.Message `json:"history,omitempty"`
	Input        []schema.Message `json:"input,omitempty"`
	Messages     []schema.Message `json:"messages,omitempty"`
	Response     []schema.Message `json:"response,omitempty"`
	externalConv bool
	historyLen   int
}

// baseStateWire 是 baseState 的持久化视图：ConvExternal 时 History
// 只保存 history_len 引用，恢复时由检查点会话回填；否则按原样内联。
// Input 为本轮输入，用于恢复时确定历史与当前工作集的边界；Messages 为
// 模型工作消息、Response 为本轮增量，与执行进度强相关，均保持内联。
type baseStateWire struct {
	History      []schema.Message `json:"history,omitempty"`
	Input        []schema.Message `json:"input,omitempty"`
	Messages     []schema.Message `json:"messages,omitempty"`
	Response     []schema.Message `json:"response,omitempty"`
	ConvExternal bool             `json:"conv_ext,omitempty"`
	HistoryLen   int              `json:"history_len,omitempty"`
}

func (s *baseState) wire() baseStateWire {
	w := baseStateWire{Input: s.Input, Messages: s.Messages, Response: s.Response}
	if s.externalConv {
		w.ConvExternal = true
		w.HistoryLen = len(s.History)
	} else {
		w.History = s.History
	}
	return w
}

func (w baseStateWire) toState() baseState {
	s := baseState{History: w.History, Input: w.Input, Messages: w.Messages, Response: w.Response}
	if w.ConvExternal {
		s.externalConv = true
		s.historyLen = w.HistoryLen
	}
	return s
}

// MarkConversationExternal 标记随后序列化以引用代替完整会话，满足
// state.ConversationExternalizer。
func (s *baseState) MarkConversationExternal() { s.externalConv = true }

// NeedsConversation 报告会话字段被外部化、需要回填，满足
// state.ConversationRestorer。
func (s *baseState) NeedsConversation() bool { return s.externalConv }

// RestoreConversation 用检查点完整会话回填 History；长度不足以覆盖
// history_len 引用时报错，不产生残缺 State。
func (s *baseState) RestoreConversation(messages []schema.Message) error {
	if !s.externalConv {
		return nil
	}
	if s.historyLen > len(messages) {
		return fmt.Errorf("checkpoint conversation of %d messages cannot cover history_len %d", len(messages), s.historyLen)
	}
	s.History = append([]schema.Message(nil), messages[:s.historyLen]...)
	s.externalConv = false
	s.historyLen = 0
	return nil
}

// newBaseState 只克隆图输入本身；RunConfig 中的 Input/History 已在
// 运行装配边界隔离，State 直接接管其所有权，共享元素按不可变约定只读。
func newBaseState(input []schema.Message, cfg RunConfig) baseState {
	messages := schema.CloneMessages(input)
	original := cfg.Input
	if original == nil {
		// 与 Messages 共享不可变元素，但持有独立切片头。
		original = append([]schema.Message(nil), messages...)
	}
	history := cfg.History
	if history == nil {
		history = original
	}
	currentInput := cfg.Input
	if currentInput == nil {
		currentInput = original
	}
	return baseState{
		History:  history,
		Input:    schema.CloneMessages(currentInput),
		Messages: messages,
	}
}

// ToolState 是 Basic/ReAct 配方的运行状态。
type ToolState struct {
	baseState
	MaxLLMCalls int    `json:"max_llm_calls"`
	LLMCalls    int    `json:"llm_calls,omitempty"`
	Calls       []Call `json:"calls,omitempty"`
	HasCalls    bool   `json:"has_calls,omitempty"`
}

// toolStateWire 组合公共会话视图与配方字段；嵌入 baseStateWire 不实现
// Marshaler，字段按原样平铺以保持既有线格式。
type toolStateWire struct {
	baseStateWire
	MaxLLMCalls int    `json:"max_llm_calls"`
	LLMCalls    int    `json:"llm_calls,omitempty"`
	Calls       []Call `json:"calls,omitempty"`
	HasCalls    bool   `json:"has_calls,omitempty"`
}

func (s *ToolState) MarshalJSON() ([]byte, error) {
	return json.Marshal(toolStateWire{
		baseStateWire: s.baseState.wire(),
		MaxLLMCalls:   s.MaxLLMCalls,
		LLMCalls:      s.LLMCalls,
		Calls:         s.Calls,
		HasCalls:      s.HasCalls,
	})
}

func (s *ToolState) UnmarshalJSON(data []byte) error {
	var wire toolStateWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	s.baseState = wire.baseStateWire.toState()
	s.MaxLLMCalls, s.LLMCalls, s.Calls, s.HasCalls = wire.MaxLLMCalls, wire.LLMCalls, wire.Calls, wire.HasCalls
	return nil
}

type Recipe struct {
	graph *graph.CompiledGraph[[]schema.Message, []schema.Message]
}

// runConfigKey 以配方实例区分，同一种配方嵌套时也读不到外层配置。
type runConfigKey struct{ recipe *Recipe }

// withConfig 把只读配置挂进 ctx；Input/History 由调用方在装配边界完成
// 隔离，这里不重复深拷贝。
func (r *Recipe) withConfig(ctx context.Context, cfg RunConfig) context.Context {
	return context.WithValue(ctx, runConfigKey{recipe: r}, cfg)
}

func (r *Recipe) config(ctx context.Context) RunConfig {
	cfg, _ := ctx.Value(runConfigKey{recipe: r}).(RunConfig)
	return cfg
}

// CheckpointMessages 返回完整会话，模型工作消息的裁剪不会影响历史。
// 只装配新切片、共享不可变元素；持久化隔离由 Store.Save 边界负责。
func (s *baseState) CheckpointMessages() []schema.Message {
	messages := make([]schema.Message, 0, len(s.History)+len(s.Response))
	messages = append(messages, s.History...)
	for _, message := range s.Response {
		if message.Role != schema.RoleSystem {
			messages = append(messages, message)
		}
	}
	return messages
}

func (r *Recipe) Snapshot() graph.Snapshot {
	if r == nil || r.graph == nil {
		return graph.Snapshot{}
	}
	return r.graph.Snapshot()
}

func (r *Recipe) Stream(ctx context.Context, messages []schema.Message, cfg RunConfig, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	if ctx == nil {
		return nil, errors.New("agent recipe context is nil")
	}
	return r.graph.Stream(r.withConfig(ctx, cfg), messages, options...)
}

func (r *Recipe) ResumeStream(ctx context.Context, cfg RunConfig, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	if r == nil || r.graph == nil {
		return nil, errors.New("agent recipe is not configured")
	}
	if ctx == nil {
		return nil, errors.New("agent recipe context is nil")
	}
	return r.graph.ResumeStream(r.withConfig(ctx, cfg), options...)
}

// newRecipeGraph 创建以 *T 为 State 的配方 Graph，并安装对应版本的 JSON Codec。
func newRecipeGraph[T any](name string, factory func(context.Context, []schema.Message) (*T, error)) (*graph.Graph[[]schema.Message, []schema.Message], error) {
	g := graph.NewGraph[[]schema.Message, []schema.Message](name)
	if err := graph.SetStateFactory(g, factory); err != nil {
		return nil, err
	}
	codec, err := graph.NewStateCodec[*T](
		"agentrecipe:"+name,
		stateCodecVersion,
		func(_ context.Context, state *T) ([]byte, error) { return json.Marshal(state) },
		func(_ context.Context, data []byte) (*T, error) {
			state := new(T)
			if err := json.Unmarshal(data, state); err != nil {
				return nil, err
			}
			return state, nil
		},
	)
	if err != nil {
		return nil, err
	}
	if err := g.SetStateCodec(codec); err != nil {
		return nil, err
	}
	return g, nil
}

// addFinishNode 挂载配方的结束节点：PreHandler 通知完成并产出最终响应，节点本身是恒等映射。
func addFinishNode[T any](g *graph.Graph[[]schema.Message, []schema.Message], output func(context.Context, *T) []schema.Message) error {
	finish := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, response []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(response), nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(ctx context.Context, state *T) ([]schema.Message, error) {
		return output(ctx, state), nil
	})); err != nil {
		return err
	}
	return g.AddEdge("finish", graph.End)
}

type Hooks struct {
	Model        runnable.Runnable[llm.Request, llm.Response]
	Tools        runnable.Runnable[[]schema.Message, []schema.Message]
	PrepareModel func(context.Context, *ToolState) (llm.Request, error)
	DetectCalls  func([]schema.Message) []Call
	Observation  func(ToolResult) schema.Message
	NameMessages func([]schema.Message) []schema.Message
}

// definitionVersion 是应用声明的 Agent 定义版本：不为空时记入图的
// 执行记录，恢复时先于指纹比对（graph.ErrDefinitionVersionMismatch）。
func NewToolRecipe(name string, hooks Hooks, tools bool, definitionVersion string) (*Recipe, error) {
	if hooks.Model == nil {
		return nil, errors.New("agent recipe requires a model node")
	}
	if hooks.PrepareModel == nil {
		return nil, errors.New("agent recipe requires a model input handler")
	}
	if tools && hooks.Tools == nil {
		return nil, errors.New("agent recipe requires a tools node")
	}
	if tools && hooks.Observation == nil {
		return nil, errors.New("agent recipe observation hook is missing")
	}
	r := &Recipe{}
	g, err := newRecipeGraph(name, func(ctx context.Context, input []schema.Message) (*ToolState, error) {
		cfg := r.config(ctx)
		if cfg.MaxLLMCalls < 1 {
			return nil, errors.New("agent recipe max LLM calls must be positive")
		}
		return &ToolState{baseState: newBaseState(input, cfg), MaxLLMCalls: cfg.MaxLLMCalls}, nil
	})
	if err != nil {
		return nil, err
	}
	if definitionVersion != "" {
		if err := g.SetDefinitionVersion(definitionVersion); err != nil {
			return nil, err
		}
	}
	// NameMessages 约定返回新视图、不改输入；未设置时原样传递。
	nameMessages := func(messages []schema.Message) []schema.Message {
		if hooks.NameMessages != nil {
			return hooks.NameMessages(messages)
		}
		return messages
	}
	if err := g.AddNode(ModelNodeID, hooks.Model,
		graph.WithPreHandler(func(ctx context.Context, state *ToolState) (llm.Request, error) {
			// state.Messages 在图内按不可变约定传递；PrepareModel 只读，
			// 模型输入的隔离由 ChatModelNode 的 request.Clone() 负责。
			return hooks.PrepareModel(ctx, state)
		}),
		graph.WithPostHandler(func(_ context.Context, state *ToolState, response llm.Response) error {
			// 节点输出归图所有；State 只追加切片、共享不可变元素。
			output := response.Messages
			state.LLMCalls++
			state.Messages = append(state.Messages, output...)
			state.Response = append(state.Response, output...)
			state.Calls = nil
			if hooks.DetectCalls != nil {
				state.Calls = hooks.DetectCalls(output)
			}
			state.HasCalls = tools && len(state.Calls) > 0 && state.LLMCalls < state.MaxLLMCalls
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if err := graph.AddBranch(g, graph.Start, "finish", "llm_limit", func(_ context.Context, state *ToolState) (bool, error) {
		return state.LLMCalls >= state.MaxLLMCalls, nil
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, ModelNodeID); err != nil {
		return nil, err
	}

	if tools {
		if err := g.AddNode("tools", hooks.Tools,
			graph.WithPreHandler(func(ctx context.Context, state *ToolState) ([]schema.Message, error) {
				return toolNodeInput(ctx, state.Messages, state.Calls), nil
			}),
			graph.WithPostHandler(func(ctx context.Context, state *ToolState, results []schema.Message) error {
				calls := cloneCalls(state.Calls)
				if len(results) != len(calls) {
					return fmt.Errorf("tools node returned %d results for %d calls", len(results), len(calls))
				}
				observations := make([]schema.Message, 0, len(results))
				for index, message := range results {
					result := ToolResult{Call: calls[index]}
					if typed := message.FunctionToolResults(); len(typed) == 1 {
						result.ContentBlocks = append([]schema.ContentBlock(nil), typed[0].ContentBlocks...)
						if typed[0].IsError {
							result.Err = errors.New(joinTextBlocks(typed[0].ContentBlocks))
						}
					} else if failed, _ := message.Extra["tool_error"].(bool); failed {
						result.Err = errors.New(message.JoinedText(""))
					}
					observations = append(observations, hooks.Observation(result))
				}
				stream.EnsureMessageIDs(observations)
				state.Messages = append(state.Messages, observations...)
				state.Response = append(state.Response, observations...)
				state.Calls = nil
				state.HasCalls = false
				return stream.PublishSnapshots(ctx, observations)
			}),
		); err != nil {
			return nil, err
		}
		if err := graph.AddBranch(g, ModelNodeID, "tools", "tool_calls", func(_ context.Context, state *ToolState) (bool, error) {
			return state.HasCalls, nil
		}); err != nil {
			return nil, err
		}
		if err := g.AddEdge("tools", ModelNodeID); err != nil {
			return nil, err
		}
	}
	if err := g.AddEdge(ModelNodeID, "finish"); err != nil {
		return nil, err
	}
	if err := addFinishNode(g, func(ctx context.Context, state *ToolState) []schema.Message {
		state.Response = nameMessages(state.Response)
		return state.Response
	}); err != nil {
		return nil, err
	}
	if tools {
		if err := g.SetMaxSteps(201); err != nil {
			return nil, err
		}
	}
	compiled, err := g.Compile()
	if err != nil {
		return nil, err
	}
	r.graph = compiled
	return r, nil
}

func joinTextBlocks(blocks []schema.ContentBlock) string {
	parts := make([]string, 0)
	for _, block := range blocks {
		if block.Type == schema.ContentBlockTypeText && block.Text != nil {
			parts = append(parts, block.Text.Text)
		}
	}
	return strings.Join(parts, "")
}

func cloneCalls(calls []Call) []Call {
	return append([]Call(nil), calls...)
}

func toolNodeInput(ctx context.Context, messages []schema.Message, calls []Call) []schema.Message {
	// 只复制切片头：下方所有修改都写入本切片的消息结构副本或新建的
	// Extra 映射，不会回写调用方的消息；ToolsNode 入口另有严格克隆。
	prepared := append([]schema.Message(nil), messages...)
	if len(calls) == 0 {
		return prepared
	}
	separateTail := len(prepared) >= len(calls)
	if separateTail {
		start := len(prepared) - len(calls)
		for index, call := range calls {
			blocks := prepared[start+index].FunctionToolCalls()
			if !call.Structured || len(blocks) != 1 || blocks[0].Name != call.Name {
				separateTail = false
				break
			}
		}
	}
	structuredTail := !separateTail && len(prepared) > 0
	if structuredTail {
		last := prepared[len(prepared)-1]
		blocks := last.FunctionToolCalls()
		structuredTail = len(blocks) == len(calls)
		for index, call := range calls {
			if !call.Structured || !structuredTail || blocks[index].Name != call.Name {
				structuredTail = false
				break
			}
		}
	}
	metadata := observe.MetadataFromContext(ctx)
	if separateTail {
		start := len(prepared) - len(calls)
		for index, call := range calls {
			prepared[start+index].Extra = observe.CloneMetadata(prepared[start+index].Extra)
			for key, value := range metadata {
				prepared[start+index].Extra[key] = value
			}
			prepared[start+index].Extra["function_id"] = call.FunctionID
			prepared[start+index].Extra["tool_call_id"] = call.FunctionID
		}
		return prepared
	}
	if structuredTail {
		last := prepared[len(prepared)-1]
		prepared = prepared[:len(prepared)-1]
		for _, call := range calls {
			message := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(call.FunctionID, call.Name, call.Arguments))
			message.Extra = observe.CloneMetadata(last.Extra)
			for key, value := range metadata {
				message.Extra[key] = value
			}
			message.Extra["function_id"] = call.FunctionID
			message.Extra["tool_call_id"] = call.FunctionID
			prepared = append(prepared, message)
		}
		return prepared
	}
	for _, call := range calls {
		extra := observe.CloneMetadata(metadata)
		extra["function_id"] = call.FunctionID
		extra["tool_call_id"] = call.FunctionID
		message := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(call.FunctionID, call.Name, call.Arguments))
		message.Extra = extra
		prepared = append(prepared, message)
	}
	return prepared
}

// concatMessages 装配新切片、共享不可变元素；用于快照等只读视图，
// 不写回任何调用方的切片。
func concatMessages(left, right []schema.Message) []schema.Message {
	combined := make([]schema.Message, 0, len(left)+len(right))
	combined = append(combined, left...)
	return append(combined, right...)
}
