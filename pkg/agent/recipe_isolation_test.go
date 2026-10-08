package agent_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

func messageTexts(messages []schema.Message) []string {
	texts := make([]string, 0, len(messages))
	for _, message := range messages {
		if results := message.FunctionToolResults(); len(results) == 1 {
			text := ""
			for _, block := range results[0].ContentBlocks {
				if block.Text != nil {
					text += block.Text.Text
				}
			}
			texts = append(texts, text)
			continue
		}
		texts = append(texts, message.JoinedText(""))
	}
	return texts
}

// 子 Agent 必须拥有独立的配方 State，不能继承父级 GroupChat 已累计的响应。
func TestGroupChatMemberDoesNotInheritParentResponse(t *testing.T) {
	first, err := agent.NewBasicAgent(agent.BaseConfig{Name: "first", Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("one")})})
	if err != nil {
		t.Fatal(err)
	}
	second, err := agent.NewBasicAgent(agent.BaseConfig{Name: "second", Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("two")})})
	if err != nil {
		t.Fatal(err)
	}
	group, err := agent.NewGroupChat(agent.GroupChatConfig{Agents: []agent.Agent{first, second}, MaxRounds: 2})
	if err != nil {
		t.Fatal(err)
	}
	output, err := group.Invoke(context.Background(), []schema.Message{schema.UserMessage("discuss")})
	if err != nil {
		t.Fatal(err)
	}
	if texts := messageTexts(output); fmt.Sprint(texts) != "[one two]" {
		t.Fatalf("group output=%q, want [one two]", texts)
	}
}

// outputRecorder 记录被包装 Agent 每次运行的原始输出。
type outputRecorder struct {
	agent.Agent
	mu      sync.Mutex
	outputs [][]schema.Message
}

func (r *outputRecorder) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	output, err := r.Agent.Invoke(ctx, messages, options...)
	r.mu.Lock()
	r.outputs = append(r.outputs, schema.CloneMessages(output))
	r.mu.Unlock()
	return output, err
}

func (r *outputRecorder) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.StreamFromInvoke(ctx, messages, r.Invoke, options...)
}

// 子 Agent 必须拥有独立的配方 State，不能继承父级 Supervisor 已累计的观察结果。
func TestSupervisorChildDoesNotInheritParentResponse(t *testing.T) {
	var childCalls atomic.Int32
	basic, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker",
		Model: llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
			return []schema.Message{schema.AssistantMessage(fmt.Sprintf("worker result %d", childCalls.Add(1)))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := &outputRecorder{Agent: basic}
	var supervisorCalls atomic.Int32
	supervisor, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name: "supervisor",
			Model: llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
				if supervisorCalls.Add(1) <= 2 {
					return []schema.Message{routingCall("worker")}, nil
				}
				return []schema.Message{schema.AssistantMessage("final")}, nil
			}),
		},
		SubAgents: []agent.Agent{worker},
		Mode:      agent.RoutingModeSupervisor,
		MaxSteps:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := supervisor.Invoke(context.Background(), []schema.Message{schema.UserMessage("route twice")})
	if err != nil {
		t.Fatal(err)
	}
	if childCalls.Load() != 2 {
		t.Fatalf("worker model calls=%d, want 2", childCalls.Load())
	}
	for index, recorded := range worker.outputs {
		if texts := messageTexts(recorded); len(texts) != 1 || texts[0] != fmt.Sprintf("worker result %d", index+1) {
			t.Fatalf("worker run %d raw output=%q", index+1, texts)
		}
	}
	if texts := messageTexts(output); fmt.Sprint(texts) != "[worker result 1 worker result 2 final]" {
		t.Fatalf("supervisor output=%q", texts)
	}
}

// Resume 沿用首次运行落盘的 MaxLLMCalls 预算，而不是恢复调用的默认值。
func TestReactResumeKeepsPersistedLLMCallBudget(t *testing.T) {
	executionStore := &agentExecutionStore{}
	var approved atomic.Bool
	var toolCalls, modelCalls atomic.Int32
	hook := tool.BeforeBatchFunc(func(context.Context, tool.Batch) error {
		if !approved.Load() {
			return control.RequestInterrupt("approval required", nil)
		}
		return nil
	})
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		toolCalls.Add(1)
		return tool.TextResult("ok"), nil
	})
	value, err := agent.New(agent.BaseConfig{
		Name: "budgeted-react",
		Model: llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
			return []schema.Message{functionCallMessage("echo", fmt.Sprintf("call-%d", modelCalls.Add(1)))}, nil
		}),
		Tools: []tool.Tool{echo},
	}, agent.WithBeforeToolBatch(hook))
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")},
		agent.WithCheckpoint(executionStore, "budget-run"), agent.WithRunMaxLLMCalls(2))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	approved.Store(true)
	if _, err := agent.Resume(context.Background(), value, agent.WithCheckpoint(executionStore, "budget-run")); err != nil {
		t.Fatal(err)
	}
	if modelCalls.Load() != 2 || toolCalls.Load() != 1 {
		t.Fatalf("model calls=%d tool calls=%d, want 2/1", modelCalls.Load(), toolCalls.Load())
	}
}

// 同一 Agent 实例并发运行时，各次运行的配方状态互不影响。
func TestConcurrentReactRunsKeepIndependentState(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	value, err := agent.New(agent.BaseConfig{
		Name: "concurrent-react",
		Model: llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
			if request.Messages[len(request.Messages)-1].Role == schema.RoleTool {
				return []schema.Message{schema.AssistantMessage("done:" + request.Messages[len(request.Messages)-2].Extra["run"].(string))}, nil
			}
			call := functionCallMessage("echo", "call-1")
			call.Extra = map[string]any{"run": request.Messages[len(request.Messages)-1].JoinedText("")}
			return []schema.Message{call}, nil
		}),
		Tools: []tool.Tool{echo},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for index := 0; index < 16; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			run := fmt.Sprintf("run-%d", index)
			output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage(run)})
			if err != nil {
				errs <- err
				return
			}
			if len(output) != 3 || output[2].JoinedText("") != "done:"+run {
				errs <- fmt.Errorf("%s output=%q", run, messageTexts(output))
			}
		}(index)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// 运行不得修改调用方传入的消息：工具往返后嵌套 Extra 与内容块保持原值。
func TestReactRunDoesNotMutateCallerInput(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	var calls atomic.Int32
	value, err := agent.New(agent.BaseConfig{
		Name: "immutable-input",
		Model: llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
			if request.Messages[len(request.Messages)-1].Role == schema.RoleTool {
				return []schema.Message{schema.AssistantMessage("done")}, nil
			}
			calls.Add(1)
			return []schema.Message{functionCallMessage("echo", "call-1")}, nil
		}),
		Tools: []tool.Tool{echo},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := schema.UserMessage("run tools")
	input.Extra = map[string]any{"nested": map[string]any{"k": "v"}, "tags": []any{"a"}}
	pristine := schema.CloneMessages([]schema.Message{input})[0]
	if _, err := value.Invoke(context.Background(), []schema.Message{input}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(input, pristine) {
		t.Fatalf("caller input mutated: got %+v want %+v", input, pristine)
	}
}

// 修改运行输出不得污染同一对话已保存的历史与后续模型请求。
func TestMutatedOutputDoesNotCorruptConversation(t *testing.T) {
	store := state.NewMemoryStore()
	var runs atomic.Int32
	var sawHistory []string
	value, err := agent.New(agent.BaseConfig{
		Name: "output-isolation",
		Model: llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
			run := runs.Add(1)
			if run == 2 {
				for _, message := range request.Messages {
					sawHistory = append(sawHistory, message.JoinedText(""))
				}
			}
			return []schema.Message{schema.AssistantMessage(fmt.Sprintf("answer-%d", run))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	output, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("first")}, agent.WithCheckpoint(store, "iso-conv"))
	if err != nil {
		t.Fatal(err)
	}
	// 就地破坏输出：写 Extra 与内容块文本，运行态与已存检查点都不得被污染。
	for i := range output {
		if output[i].Extra == nil {
			output[i].Extra = map[string]any{}
		}
		output[i].Extra["corrupted"] = true
		for j := range output[i].ContentBlocks {
			if output[i].ContentBlocks[j].Text != nil {
				output[i].ContentBlocks[j].Text.Text = "corrupted"
			}
		}
	}
	if _, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("second")}, agent.WithCheckpoint(store, "iso-conv")); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(sawHistory, "|")
	if strings.Contains(joined, "corrupted") {
		t.Fatalf("model observed corrupted history %q", joined)
	}
	if !strings.Contains(joined, "answer-1") || !strings.Contains(joined, "first") {
		t.Fatalf("model request history missing earlier conversation: %q", joined)
	}
}
