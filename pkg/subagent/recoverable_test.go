package subagent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/subagent"
)

var durabilityModes = []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync}

type taskState struct{ Task string }

// approvalGraph 是自建子图：任务文本取自运行输入（最后一条用户消息）；任务含
// "approve" 时先请求审批，最终回复 "built-for:<任务>"。
func approvalGraph() (*graph.CompiledGraph[[]schema.Message, []schema.Message], error) {
	g := graph.NewGraph[[]schema.Message, []schema.Message]("approval-sub")
	if err := g.SetStateFactory(func(_ context.Context, in []schema.Message) (*taskState, error) {
		return &taskState{Task: schema.LastUserText(in)}, nil
	}); err != nil {
		return nil, err
	}
	ask := runnable.Func[string, string](func(ctx context.Context, task string, _ ...runnable.Option) (string, error) {
		if strings.Contains(task, "approve") {
			if _, err := graph.Interrupt(ctx, "approve "+task); err != nil {
				return "", err
			}
		}
		return task, nil
	})
	finish := runnable.Func[string, []schema.Message](func(_ context.Context, task string, _ ...runnable.Option) ([]schema.Message, error) {
		return []schema.Message{schema.AssistantMessage("built-for:" + task)}, nil
	})
	if err := g.AddNode("ask", ask, graph.WithPreHandler(func(_ context.Context, s *taskState) (string, error) { return s.Task, nil })); err != nil {
		return nil, err
	}
	if err := g.AddNode("finish", finish); err != nil {
		return nil, err
	}
	for _, edge := range [][2]string{{graph.Start, "ask"}, {"ask", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			return nil, err
		}
	}
	return g.Compile()
}

func customConfig(builder subagent.Builder) subagent.Config {
	return subagent.Config{
		Models: map[string]llm.ChatModel{"child": fixedModel("unused")}, Builder: builder,
		BuilderID: "custom-graph", BuilderVersion: "1",
	}
}

func runSpawn(t *testing.T, cfg subagent.Config, store state.Store, mode graph.Durability, args map[string]any) (*parentRun, error) {
	t.Helper()
	m := newManager(t, cfg)
	run := &parentRun{}
	_, err := newParent(t, m, run, spawnStep(args), waitStep(0)).
		Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m, store, "conv", agent.WithDurability(mode))...)
	return run, err
}

// 构建器返回的框架错误原样上交，根运行不会被写成成功完成；模型可修正的配置错误
// 作为模型可见的反馈，且不留下任务。
func TestBuilderErrorsKeepTheirClass(t *testing.T) {
	for _, mode := range durabilityModes {
		t.Run(string(mode), func(t *testing.T) {
			cases := []struct {
				name      string
				err       error
				frame     func(error) bool
				wantText  string
				wantError bool
			}{
				{"resume incompatible", fmt.Errorf("sub graph check: %w", graph.ErrResumeIncompatible),
					func(err error) bool { return errors.Is(err, graph.ErrResumeIncompatible) }, "", true},
				{"checkpoint error", &graph.CheckpointError{Key: "k", Err: errors.New("disk full")},
					func(err error) bool { var c *graph.CheckpointError; return errors.As(err, &c) }, "", true},
				{"config error", &subagent.ConfigError{Message: "bad role and tools combination"}, nil, "bad role and tools combination", false},
				{"plain failure", errors.New("secret internal detail"), nil, "could not be created", false},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					store := state.NewMemoryStore()
					cfg := customConfig(func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) { return nil, tc.err })
					m := newManager(t, cfg)
					run := &parentRun{}
					_, err := newParent(t, m, run, spawnStep(map[string]any{"task": "t"})).
						Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m, store, "conv", agent.WithDurability(mode))...)
					if tc.wantError {
						if err == nil || !tc.frame(err) {
							t.Fatalf("err=%v, want the original framework error", err)
						}
						if record, _, _ := store.Load(context.Background(), "conv"); record.Status == state.StatusCompleted {
							t.Fatal("root run was recorded as completed")
						}
					} else {
						if err != nil {
							t.Fatalf("invoke: %v", err)
						}
						seen := run.seen()
						if len(seen) != 1 || !strings.Contains(seen[0].Text, tc.wantText) || strings.Contains(seen[0].Text, "secret internal detail") {
							t.Fatalf("results=%+v", seen)
						}
					}
					if got := agentRecordsOrNone(t, store, "conv"); len(got) != 0 {
						t.Fatalf("rejected creation left sub-agents: %+v", got)
					}
				})
			}
		})
	}
}

func agentRecordsOrNone(t *testing.T, store state.Store, key string) []graph.AgentInfo {
	t.Helper()
	record, found, err := store.Load(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		return nil
	}
	agents, err := graph.SessionAgentsOf(record)
	if err != nil {
		t.Fatal(err)
	}
	return agents
}

// 任务文本通过运行输入传递：同一任务审批后重建实例，输出不变；相同组件配置下的
// 不同任务（含追加任务）复用同一份构建结果，各得到自己的输出。
func TestTaskTextTravelsThroughRunInput(t *testing.T) {
	for _, mode := range durabilityModes {
		t.Run(string(mode), func(t *testing.T) {
			store := state.NewMemoryStore()
			var builds int
			cfg := customConfig(func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
				builds++
				return approvalGraph()
			})

			// 审批后重建管理器与父 Agent，再恢复。
			m1 := newManager(t, cfg)
			steps := []step{spawnStep(map[string]any{"task": "audit login approve"}), waitStep(0)}
			_, err := newParent(t, m1, &parentRun{}, steps...).
				Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m1, store, "conv", agent.WithDurability(mode))...)
			var interrupted *graph.InterruptedError
			if !errors.As(err, &interrupted) || len(interrupted.PendingInterrupts()) != 1 {
				t.Fatalf("want one approval, got %v", err)
			}
			m2 := newManager(t, cfg)
			if _, err := agent.Resume(context.Background(), newParent(t, m2, &parentRun{}, steps...),
				runOpts(m2, store, "conv", agent.WithDurability(mode), agent.WithResume(interrupted.PendingInterrupts()[0].ID, "yes"))...); err != nil {
				t.Fatalf("resume: %v", err)
			}
			agents := agentRecords(t, store, "conv")
			if len(agents) != 1 || string(agents[0].Tasks[0].Result) != `"built-for:audit login approve"` {
				t.Fatalf("after rebuild: %+v", agents)
			}

			// 同配置的另一个新子 Agent 与对原子 Agent 的追加任务，各自得到自己的输出。
			m3 := newManager(t, cfg)
			run := &parentRun{}
			parent := newParent(t, m3, run,
				spawnStep(map[string]any{"task": "second agent"}), waitStep(0),
				func([]toolOut) schema.Message {
					return toolCall("f", "followup_task", map[string]any{"agent_id": "agent-1", "task": "follow up"})
				},
				waitStep(2),
			)
			if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("round2")}, runOpts(m3, store, "conv", agent.WithDurability(mode))...); err != nil {
				t.Fatalf("round2: %v", err)
			}
			agents = agentRecords(t, store, "conv")
			if len(agents) != 2 || string(agents[1].Tasks[0].Result) != `"built-for:second agent"` || string(agents[0].Tasks[1].Result) != `"built-for:follow up"` {
				t.Fatalf("agents=%+v", agents)
			}
		})
	}
}

type plainWrapper struct{ subagent.Runner }

type forwardingWrapper struct {
	subagent.Runner
	inner *graph.CompiledGraph[[]schema.Message, []schema.Message]
}

func (w forwardingWrapper) Recoverable() error { return w.inner.Recoverable() }

func statelessGraph() (*graph.CompiledGraph[[]schema.Message, []schema.Message], error) {
	g := graph.NewGraph[[]schema.Message, []schema.Message]("stateless")
	node := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, in []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return in, nil
	})
	if err := g.AddNode("work", node); err != nil {
		return nil, err
	}
	if err := g.AddEdge(graph.Start, "work"); err != nil {
		return nil, err
	}
	if err := g.AddEdge("work", graph.End); err != nil {
		return nil, err
	}
	return g.Compile()
}

// 恢复能力必须明确声明：直接返回图、显式转发的包装、WithRecovery 声明的包装可以接入；
// 没有声明的普通包装与不可恢复的图在登记前被拒绝，不留下子 Agent。
func TestRecoveryContractIsExplicit(t *testing.T) {
	for _, mode := range durabilityModes {
		t.Run(string(mode), func(t *testing.T) {
			rejected := map[string]subagent.Builder{
				"stateless graph": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) { return statelessGraph() },
				"plain wrapper over a recoverable graph": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
					g, err := approvalGraph()
					return plainWrapper{g}, err
				},
				"plain wrapper over a stateless graph": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
					g, err := statelessGraph()
					return plainWrapper{g}, err
				},
				"forwarding wrapper over a stateless graph": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
					g, err := statelessGraph()
					return forwardingWrapper{g, g}, err
				},
				"WithRecovery forwarding a stateless graph": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
					g, err := statelessGraph()
					return subagent.WithRecovery(g, g.Recoverable), err
				},
			}
			for name, builder := range rejected {
				t.Run("rejected/"+name, func(t *testing.T) {
					store := state.NewMemoryStore()
					_, err := runSpawn(t, customConfig(builder), store, mode, map[string]any{"task": "t"})
					if !errors.Is(err, subagent.ErrNotRecoverable) {
						t.Fatalf("err=%v, want ErrNotRecoverable", err)
					}
					if got := agentRecordsOrNone(t, store, "conv"); len(got) != 0 {
						t.Fatalf("rejected creation left sub-agents: %+v", got)
					}
				})
			}

			accepted := map[string]subagent.Builder{
				"direct graph": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) { return approvalGraph() },
				"forwarding wrapper": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
					g, err := approvalGraph()
					return forwardingWrapper{g, g}, err
				},
				"WithRecovery forwarding": func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
					g, err := approvalGraph()
					return subagent.WithRecovery(g, g.Recoverable), err
				},
			}
			for name, builder := range accepted {
				t.Run("accepted/"+name, func(t *testing.T) {
					store := state.NewMemoryStore()
					cfg := customConfig(builder)
					m1 := newManager(t, cfg)
					steps := []step{spawnStep(map[string]any{"task": "audit approve"}), waitStep(0)}
					_, err := newParent(t, m1, &parentRun{}, steps...).
						Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m1, store, "conv", agent.WithDurability(mode))...)
					var interrupted *graph.InterruptedError
					if !errors.As(err, &interrupted) || len(interrupted.PendingInterrupts()) != 1 {
						t.Fatalf("want one approval, got %v", err)
					}
					m2 := newManager(t, cfg)
					if _, err := agent.Resume(context.Background(), newParent(t, m2, &parentRun{}, steps...),
						runOpts(m2, store, "conv", agent.WithDurability(mode), agent.WithResume(interrupted.PendingInterrupts()[0].ID, "yes"))...); err != nil {
						t.Fatalf("resume: %v", err)
					}
					agents := agentRecords(t, store, "conv")
					if len(agents) != 1 || string(agents[0].Tasks[0].Result) != `"built-for:audit approve"` {
						t.Fatalf("agents=%+v", agents)
					}
				})
			}
		})
	}
}
