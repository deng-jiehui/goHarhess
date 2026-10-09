package agent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// newApprovalChild 构造一个在工具执行前发起动态交互中断的子 Agent。
func newApprovalChild(t *testing.T, toolCalls *atomic.Int32) agent.Agent {
	t.Helper()
	var modelCalls atomic.Int32
	child, err := agent.New(llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		if modelCalls.Add(1) == 1 {
			return []schema.Message{functionCallMessage("echo", "call-1")}, nil
		}
		return []schema.Message{schema.AssistantMessage("child done")}, nil
	}), agent.WithName("worker"), agent.WithTools(tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		toolCalls.Add(1)
		return tool.TextResult("ok"), nil
	})), agent.WithBeforeToolBatch(tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
		_, err := graph.Interrupt(ctx, map[string]any{"approve": len(batch.Calls)})
		return err
	})))
	if err != nil {
		t.Fatal(err)
	}
	return child
}

// 子 Agent 的控制类错误必须穿透为控制信号：挂起不能被转成模型可见的观察
// 结果，整棵执行树由根检查点挂起，恢复后子调用从已保存的 frame 继续。
func TestChildAgentControlErrorsFailParentRun(t *testing.T) {
	for _, router := range []bool{false, true} {
		name := "supervisor"
		if router {
			name = "router"
		}
		t.Run(name, func(t *testing.T) {
			var toolCalls, supervisorCalls atomic.Int32
			parent := newSupervisorApp(t, "parent", llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
				if supervisorCalls.Add(1) == 1 {
					return []schema.Message{routingCall("worker")}, nil
				}
				return []schema.Message{schema.AssistantMessage("parent final")}, nil
			}), router, 2, newApprovalChild(t, &toolCalls))
			checkpoint := graph.WithCheckpoint(state.NewMemoryStore(), "nested")
			output, err := parent.Invoke(context.Background(), supervisorState{Messages: []schema.Message{schema.UserMessage("go")}}, checkpoint)
			var interrupted *graph.InterruptedError
			if !errors.As(err, &interrupted) {
				t.Fatalf("err=%v output=%q, want InterruptedError", err, messageTexts(output.Messages))
			}
			pending := interrupted.PendingInterrupts()
			if len(pending) != 1 {
				t.Fatalf("pending=%v", pending)
			}
			if toolCalls.Load() != 0 || supervisorCalls.Load() != 1 {
				t.Fatalf("tool calls=%d parent model calls=%d", toolCalls.Load(), supervisorCalls.Load())
			}
			output, err = parent.Resume(context.Background(), checkpoint, graph.WithResume(pending[0].ID, true))
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			// Router 把子 Agent 结果直接作为输出，Supervisor 交还决策模型继续。
			want := "parent final"
			if router {
				want = "child done"
			}
			if got := fmt.Sprint(messageTexts(output.Messages)); !strings.Contains(got, want) {
				t.Fatalf("output=%q want %q", got, want)
			}
			if toolCalls.Load() != 1 {
				t.Fatalf("tool calls=%d after resume", toolCalls.Load())
			}
		})
	}
}

// completedChildGate 只在写入携带已完成子调用记录的快照时注入存储故障，
// 模拟"子 Agent 已成功、结果落盘失败"的瞬态存储错误。
type completedChildGate struct {
	state.Store
	armed atomic.Bool
}

func (s *completedChildGate) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	if s.armed.Load() && strings.Contains(string(c.Execution), `"status":"completed"`) {
		return state.Checkpoint{}, errors.New("injected child-save failure")
	}
	return s.Store.Save(ctx, c)
}

// 子结果保存失败属于框架控制错误：不得作为业务失败观察交给主 Agent，
// 恢复后按至少一次语义重放子调用。
func TestChildAgentSaveFailureIsNotBusinessError(t *testing.T) {
	var workerCalls, supervisorCalls atomic.Int32
	worker, err := agent.New(llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		workerCalls.Add(1)
		return []schema.Message{schema.AssistantMessage("worker done")}, nil
	}), agent.WithName("worker"))
	if err != nil {
		t.Fatal(err)
	}
	supervisor := newSupervisorApp(t, "supervisor", llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		if supervisorCalls.Add(1) == 1 {
			return []schema.Message{routingCall("worker")}, nil
		}
		return []schema.Message{schema.AssistantMessage("recovered")}, nil
	}), false, 2, worker)
	gate := &completedChildGate{Store: state.NewMemoryStore()}
	gate.armed.Store(true)
	checkpoint := graph.WithCheckpoint(gate, "save-fail")
	_, err = supervisor.Invoke(context.Background(), supervisorState{Messages: []schema.Message{schema.UserMessage("go")}}, checkpoint)
	var checkpointErr *graph.CheckpointError
	if !errors.As(err, &checkpointErr) {
		t.Fatalf("err=%v, want CheckpointError (must not reach supervisor as observation)", err)
	}
	if supervisorCalls.Load() != 1 {
		t.Fatalf("supervisor calls=%d (save failure must not be decided by recipe)", supervisorCalls.Load())
	}
	gate.armed.Store(false)
	output, err := supervisor.Resume(context.Background(), checkpoint)
	if err != nil || len(output.Messages) != 4 || output.Messages[3].JoinedText("") != "recovered" {
		t.Fatalf("resume output=%q err=%v", messageTexts(output.Messages), err)
	}
	// 子调用从已保存的边界继续：模型调用不重跑，只补交缺失的结果保存。
	if workerCalls.Load() != 1 {
		t.Fatalf("worker calls=%d (replay must resume from saved boundary)", workerCalls.Load())
	}
}

// newApprovalChildVariant 是与 newApprovalChild 同名 "worker" 但内部配方图
// 不同的子 Agent：ReAct 配方换成 basic 配方后子图名/revision 变化，用于模拟
// "同名子图换版本"的恢复不兼容场景。
func newApprovalChildVariant(t *testing.T) agent.Agent {
	t.Helper()
	child, err := agent.New(llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("child done")}), agent.WithName("worker"))
	if err != nil {
		t.Fatal(err)
	}
	return child
}

// 问题2（复查三）：恢复兼容性错误是运行/控制错误，必须穿透 AgentNode——
// 不得包装成观察交给主配方。子 Agent 挂起后以不同配方重建同名 worker，
// 提交答案应返回 ErrResumeIncompatible，根记录仍未完成，Supervisor 模型
// 只跑最初那一次选择。
func TestChildAgentResumeIncompatibleFailsRun(t *testing.T) {
	for _, router := range []bool{false, true} {
		name := "supervisor"
		if router {
			name = "router"
		}
		t.Run(name, func(t *testing.T) {
			var toolCalls, supervisorCalls atomic.Int32
			buildParent := func(child agent.Agent) *graph.CompiledGraph[supervisorState, *supervisorState] {
				return newSupervisorApp(t, "parent", llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
					if supervisorCalls.Add(1) == 1 {
						return []schema.Message{routingCall("worker")}, nil
					}
					return []schema.Message{schema.AssistantMessage("parent final")}, nil
				}), router, 2, child)
			}
			parent := buildParent(newApprovalChild(t, &toolCalls))
			store := state.NewMemoryStore()
			checkpoint := graph.WithCheckpoint(store, "incompatible")
			_, err := parent.Invoke(context.Background(), supervisorState{Messages: []schema.Message{schema.UserMessage("go")}}, checkpoint)
			var interrupted *graph.InterruptedError
			if !errors.As(err, &interrupted) {
				t.Fatalf("err=%v, want InterruptedError", err)
			}
			pending := interrupted.PendingInterrupts()
			if len(pending) != 1 {
				t.Fatalf("pending=%v", pending)
			}
			// 同名 worker 换成不同配方：恢复在返回保存结果之前必须先过身份校验。
			rebuilt := buildParent(newApprovalChildVariant(t))
			_, err = rebuilt.Resume(context.Background(), checkpoint, graph.WithResume(pending[0].ID, true))
			if !errors.Is(err, graph.ErrResumeIncompatible) {
				t.Fatalf("resume err=%v, want ErrResumeIncompatible", err)
			}
			if supervisorCalls.Load() != 1 {
				t.Fatalf("supervisor calls=%d (recovery error must not reach the recipe)", supervisorCalls.Load())
			}
			record, found, err := store.Load(context.Background(), "incompatible")
			if err != nil || !found {
				t.Fatal("checkpoint missing")
			}
			if record.Status == state.StatusCompleted {
				t.Fatal("root run must not be marked completed")
			}
		})
	}
}

// 普通业务失败仍按原语义处理：Supervisor 把失败作为观察交还给决策模型。
func TestChildAgentBusinessErrorStaysObservationInSupervisor(t *testing.T) {
	failing, err := agent.New(llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		return nil, errors.New("worker provider failed")
	}), agent.WithName("worker"))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	supervisor := newSupervisorApp(t, "supervisor", llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		if calls.Add(1) == 1 {
			return []schema.Message{routingCall("worker")}, nil
		}
		return []schema.Message{schema.AssistantMessage("recovered")}, nil
	}), false, 2, failing)
	output, err := supervisor.Invoke(context.Background(), supervisorState{Messages: []schema.Message{schema.UserMessage("go")}})
	if err != nil || len(output.Messages) != 4 || output.Messages[3].JoinedText("") != "recovered" {
		t.Fatalf("output=%q err=%v", messageTexts(output.Messages), err)
	}
}
