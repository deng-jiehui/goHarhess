package graph_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// 两节点子图：第一个节点导出上下文，第二个节点请求审批。输入为 "first" 的任务走
// 子图；其余任务直接返回当前子 Agent 上下文，用来观察下一轮读到了什么。
func approvalContextBuilder(t *testing.T, exports *atomic.Int32) graph.ManagedBuilder {
	return func(_ context.Context, _ graph.ManagedSpec) (graph.ManagedRunner, error) {
		g := newManagedStateGraph(t, "ctx-approval")
		export := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
			exports.Add(1)
			if err := graph.SetManagedAgentContext(ctx, json.RawMessage(`"remembered"`)); err != nil {
				return "", err
			}
			return input, nil
		})
		ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
			if _, err := graph.Interrupt(ctx, "approve?"); err != nil {
				return "", err
			}
			return "approved:" + input, nil
		})
		if err := g.AddNode("export", export,
			graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil })); err != nil {
			return nil, err
		}
		if err := g.AddNode("ask", ask,
			graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil })); err != nil {
			return nil, err
		}
		for _, edge := range [][2]string{{graph.Start, "export"}, {"export", "ask"}, {"ask", graph.End}} {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				return nil, err
			}
		}
		compiled, err := g.Compile()
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var task string
			_ = json.Unmarshal(input, &task)
			if task != "first" {
				raw, err := graph.ManagedAgentContext(ctx)
				if err != nil {
					return nil, err
				}
				return json.Marshal("ctx=" + string(raw))
			}
			out, err := compiled.Invoke(ctx, task)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
}

func buildAgentRoot(t *testing.T, act func(ctx context.Context) (string, error)) *graph.CompiledGraph[string, string] {
	t.Helper()
	g := newManagedStateGraph(t, "session-agents")
	node := runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) { return act(ctx) })
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil })); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "main"}, {"main", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	return mustCompile(t, g)
}

// 前一节点导出上下文、后一节点审批挂起；重建实例回答后任务成功结束，
// 导出节点只执行一次，下一轮追加任务读到的是已导出的上下文。
func TestSessionAgentContextSurvivesApproval(t *testing.T) {
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			store := state.NewMemoryStore()
			var exports atomic.Int32
			builder := approvalContextBuilder(t, &exports)
			opts := func(extra ...runnable.Option) []runnable.Option {
				return append([]runnable.Option{
					graph.WithCheckpoint(store, "ctx"),
					graph.WithDurability(mode),
					graph.WithManagedBuilder("agent", "v1", builder),
				}, extra...)
			}
			var ref graph.AgentRef
			act := func(ctx context.Context) (string, error) {
				var err error
				if ref, err = graph.SpawnAgent(ctx, spawnSpec("first"), ""); err != nil {
					return "", err
				}
				return waitResult(ctx, ref.TaskID)
			}
			_, err := buildAgentRoot(t, act).Invoke(context.Background(), "go", opts()...)
			var interrupted *graph.InterruptedError
			if !errors.As(err, &interrupted) {
				t.Fatalf("want interrupt, got %v", err)
			}
			pending := interrupted.PendingInterrupts()
			if len(pending) != 1 {
				t.Fatalf("pending=%+v", pending)
			}
			out, err := buildAgentRoot(t, act).Resume(context.Background(), opts(graph.WithResume(pending[0].ID, "ok"))...)
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			if out != `approved:first` {
				t.Fatalf("out=%q", out)
			}
			if exports.Load() != 1 {
				t.Fatalf("export node ran %d times, want 1", exports.Load())
			}
			var follow string
			if _, err := runAgentRound(t, store, "ctx", builder, "v1", func(ctx context.Context) (string, error) {
				r, err := graph.FollowupAgent(ctx, ref.AgentID, jsonString("read"))
				if err != nil {
					return "", err
				}
				follow, err = waitResult(ctx, r.TaskID)
				return follow, err
			}, graph.WithDurability(mode)); err != nil {
				t.Fatalf("round2: %v", err)
			}
			if follow != `ctx="remembered"` {
				t.Fatalf("next task read %q, want the exported context", follow)
			}
		})
	}
}

// 创建返回后调用方复用 Config 切片，不得改变已登记子 Agent 的配置，
// 也不得影响下一轮追加任务时构建器收到的配置。
func TestSessionAgentConfigFrozen(t *testing.T) {
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			store := state.NewMemoryStore()
			var started atomic.Int32
			inner := sessionAgentBuilder(&started, 60*time.Millisecond, "")
			var configs []string
			builder := func(ctx context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
				configs = append(configs, string(spec.Config))
				return inner(ctx, spec)
			}
			var ref graph.AgentRef
			var seen graph.AgentInfo
			probed := false
			if _, err := runAgentRound(t, store, "cfg", builder, "v1", func(ctx context.Context) (string, error) {
				cfg := []byte(`{"role":"reviewer"}`)
				spec := graph.ManagedSpec{Builder: "agent", Version: "v1", Config: cfg, Input: jsonString("t")}
				var err error
				if ref, err = graph.SpawnAgent(ctx, spec, ""); err != nil {
					return "", err
				}
				copy(cfg[bytes.Index(cfg, []byte("reviewer")):], "attacker")
				if !probed {
					probed = true
					if seen, err = graph.GetAgent(ctx, ref.AgentID); err != nil {
						return "", err
					}
				}
				return waitResult(ctx, ref.TaskID)
			}, graph.WithDurability(mode)); err != nil {
				t.Fatal(err)
			}
			if string(seen.Config) != `{"role":"reviewer"}` {
				t.Fatalf("registered config follows caller slice: %s", seen.Config)
			}
			record, _, _ := store.Load(context.Background(), "cfg")
			agents, _ := graph.SessionAgentsOf(record)
			if len(agents) != 1 || string(agents[0].Config) != `{"role":"reviewer"}` {
				t.Fatalf("saved config=%+v", agents)
			}
			configs = nil
			if _, err := runAgentRound(t, store, "cfg", builder, "v1", func(ctx context.Context) (string, error) {
				r, err := graph.FollowupAgent(ctx, ref.AgentID, jsonString("again"))
				if err != nil {
					return "", err
				}
				return waitResult(ctx, r.TaskID)
			}, graph.WithDurability(mode)); err != nil {
				t.Fatal(err)
			}
			if len(configs) == 0 || configs[0] != `{"role":"reviewer"}` {
				t.Fatalf("followup builder saw %v", configs)
			}
		})
	}
}
