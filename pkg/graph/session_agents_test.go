package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

type agentCtx struct {
	N    int    `json:"n"`
	Last string `json:"last"`
}

// sessionAgentBuilder 是带上下文的测试执行器：读取子 Agent 此前导出的上下文，
// 返回 "done:<任务>|ctx=<此前任务数>"，并导出累计后的新上下文。
// started 统计 runner 实际执行次数；delay 让任务保持运行；fail 非空的任务以业务错误结束。
func sessionAgentBuilder(started *atomic.Int32, delay time.Duration, failOn string) graph.ManagedBuilder {
	return func(_ context.Context, _ graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			started.Add(1)
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			var task string
			_ = json.Unmarshal(input, &task)
			if failOn != "" && task == failOn {
				return nil, errors.New("boom")
			}
			var prev agentCtx
			raw, err := graph.ManagedAgentContext(ctx)
			if err != nil {
				return nil, err
			}
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &prev); err != nil {
					return nil, err
				}
			}
			next, _ := json.Marshal(agentCtx{N: prev.N + 1, Last: task})
			if err := graph.SetManagedAgentContext(ctx, next); err != nil {
				return nil, err
			}
			return json.Marshal(fmt.Sprintf("done:%s|ctx=%d", task, prev.N))
		}, nil
	}
}

// runAgentRound 用全新的图实例执行一轮：单节点图，节点行为由 act 决定。
func runAgentRound(t *testing.T, store state.Store, key string, builder graph.ManagedBuilder, version string, act func(ctx context.Context) (string, error), extra ...runnable.Option) (string, error) {
	t.Helper()
	g := newManagedStateGraph(t, "session-agents")
	node := runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) {
		return act(ctx)
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "main"}, {"main", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	options := append([]runnable.Option{
		graph.WithCheckpoint(store, key),
		graph.WithManagedBuilder("agent", version, builder),
	}, extra...)
	return mustCompile(t, g).Invoke(context.Background(), "go", options...)
}

func jsonString(s string) json.RawMessage { b, _ := json.Marshal(s); return b }

func spawnSpec(task string) graph.ManagedSpec {
	return graph.ManagedSpec{Builder: "agent", Version: "v1", Config: json.RawMessage(`{"role":"reviewer"}`), Input: jsonString(task)}
}

func waitResult(ctx context.Context, taskID string) (string, error) {
	results, err := graph.WaitManaged(ctx, []string{taskID})
	if err != nil {
		return "", err
	}
	if results[0].Status != graph.ManagedStatusCompleted {
		return "", fmt.Errorf("task %s status %s: %s", taskID, results[0].Status, results[0].Error)
	}
	var out string
	_ = json.Unmarshal(results[0].Result, &out)
	return out, nil
}

// 同一会话两轮：第二轮能查询第一轮创建的子 Agent 与结果，第一轮任务不重跑；
// 追加任务沿用上下文、AgentID 不变、TaskID 更新，父 Agent 只拿到概览。
func TestSessionAgentsPersistAcrossRounds(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	builder := sessionAgentBuilder(&started, 0, "")

	var first graph.AgentRef
	if _, err := runAgentRound(t, store, "s1", builder, "v1", func(ctx context.Context) (string, error) {
		ref, err := graph.SpawnAgent(ctx, spawnSpec("audit-login"), "登录模块审查")
		if err != nil {
			return "", err
		}
		first = ref
		return waitResult(ctx, ref.TaskID)
	}); err != nil {
		t.Fatalf("round1: %v", err)
	}
	if first.AgentID == "" || first.TaskID == "" || first.AgentID == first.TaskID {
		t.Fatalf("bad ref %+v", first)
	}
	record, found, err := store.Load(context.Background(), "s1")
	if err != nil || !found {
		t.Fatalf("load: %v %v", found, err)
	}
	if record.Status != state.StatusCompleted || len(record.SubAgents) == 0 {
		t.Fatalf("record status=%s subagents=%s", record.Status, record.SubAgents)
	}
	runID1 := record.RunID

	var second graph.AgentRef
	var listed []graph.AgentSummary
	var info graph.AgentInfo
	var out string
	_, err = runAgentRound(t, store, "s1", builder, "v1", func(ctx context.Context) (string, error) {
		var err error
		// WaitManaged 挂起后节点会被重放：查询只记录首次观察。
		if listed == nil {
			if listed, err = graph.ListAgents(ctx); err != nil {
				return "", err
			}
			if info, err = graph.GetAgent(ctx, first.AgentID); err != nil {
				return "", err
			}
		}
		if second, err = graph.FollowupAgent(ctx, first.AgentID, jsonString("verify-fix")); err != nil {
			return "", err
		}
		out, err = waitResult(ctx, second.TaskID)
		return out, err
	})
	if err != nil {
		t.Fatalf("round2: %v", err)
	}
	if started.Load() != 2 {
		t.Fatalf("runner started %d times, want 2 (round1 task must not rerun)", started.Load())
	}
	if len(listed) != 1 || listed[0].AgentID != first.AgentID || listed[0].Status != graph.AgentStatusIdle ||
		listed[0].Description != "登录模块审查" || listed[0].LastTask != first.TaskID ||
		listed[0].LastStatus != graph.ManagedStatusCompleted || listed[0].Summary != "done:audit-login|ctx=0" {
		t.Fatalf("listed=%+v", listed)
	}
	if len(info.Tasks) != 1 || string(info.Tasks[0].Result) != `"done:audit-login|ctx=0"` || info.Builder != "agent" {
		t.Fatalf("info=%+v", info)
	}
	if second.AgentID != first.AgentID || second.TaskID == first.TaskID {
		t.Fatalf("followup ref %+v vs %+v", second, first)
	}
	if out != "done:verify-fix|ctx=1" {
		t.Fatalf("followup did not reuse agent context: %q", out)
	}

	record, _, _ = store.Load(context.Background(), "s1")
	if record.RunID == runID1 {
		t.Fatal("round 2 should have a new run id")
	}
	agents, err := graph.SessionAgentsOf(record)
	if err != nil || len(agents) != 1 || len(agents[0].Tasks) != 2 || agents[0].Status != graph.AgentStatusIdle {
		t.Fatalf("agents=%+v err=%v", agents, err)
	}
	if agents[0].Tasks[0].TaskID != first.TaskID || agents[0].Tasks[1].TaskID != second.TaskID {
		t.Fatalf("task index=%+v", agents[0].Tasks)
	}
}

// 重放追加任务的调用不重复登记或执行；新请求即使文本相同也各自执行。
func TestSessionAgentFollowupReplayAndRepeat(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	builder := sessionAgentBuilder(&started, 0, "")
	var ref graph.AgentRef
	if _, err := runAgentRound(t, store, "s2", builder, "v1", func(ctx context.Context) (string, error) {
		var err error
		ref, err = graph.SpawnAgent(ctx, spawnSpec("t0"), "")
		if err != nil {
			return "", err
		}
		return waitResult(ctx, ref.TaskID)
	}); err != nil {
		t.Fatal(err)
	}

	var replays atomic.Int32
	var refs []graph.AgentRef
	opts := []runnable.Option{}
	act := func(ctx context.Context) (string, error) {
		// 每次进入节点都追加同一位点的任务；第一次执行后挂起审批，Resume 重放整个节点。
		r, err := graph.FollowupAgent(ctx, ref.AgentID, jsonString("same"))
		if err != nil {
			return "", err
		}
		replays.Add(1)
		refs = append(refs, r)
		if _, err := graph.WaitManaged(ctx, []string{r.TaskID}); err != nil {
			return "", err
		}
		if _, err := graph.Interrupt(ctx, "approve?"); err != nil {
			return "", err
		}
		return r.TaskID, nil
	}
	_, err := runAgentRound(t, store, "s2", builder, "v1", act, opts...)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want interrupt, got %v", err)
	}
	pending := interrupted.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("pending=%+v", pending)
	}
	startedBefore := started.Load()

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
	out, err := mustCompile(t, g).Resume(context.Background(),
		graph.WithCheckpoint(store, "s2"),
		graph.WithManagedBuilder("agent", "v1", builder),
		graph.WithResume(pending[0].ID, "ok"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(refs) < 2 || refs[0] != refs[len(refs)-1] || out != refs[0].TaskID {
		t.Fatalf("replay changed identity: refs=%+v out=%s", refs, out)
	}
	if started.Load() != startedBefore {
		t.Fatalf("replayed followup re-executed the runner: %d -> %d", startedBefore, started.Load())
	}
	record, _, _ := store.Load(context.Background(), "s2")
	agents, _ := graph.SessionAgentsOf(record)
	if len(agents) != 1 || len(agents[0].Tasks) != 2 {
		t.Fatalf("followup registered more than once: %+v", agents)
	}

	// 下一轮：相同文本的新请求再次执行，TaskID 新增。
	var again graph.AgentRef
	if _, err := runAgentRound(t, store, "s2", builder, "v1", func(ctx context.Context) (string, error) {
		var err error
		if again, err = graph.FollowupAgent(ctx, ref.AgentID, jsonString("same")); err != nil {
			return "", err
		}
		return waitResult(ctx, again.TaskID)
	}); err != nil {
		t.Fatal(err)
	}
	if again.TaskID == refs[0].TaskID || again.TaskID == ref.TaskID {
		t.Fatalf("new request reused a task id: %+v", again)
	}
	if want := startedBefore + 1; started.Load() != want {
		t.Fatalf("started=%d want %d", started.Load(), want)
	}
}

// 活动任务期间拒绝追加；审批挂起属于原任务；任务失败不影响已有结果；
// 关闭后拒绝追加但结果可读；其他会话看不到这些子 Agent。
func TestSessionAgentLifecycleRules(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	slow := sessionAgentBuilder(&started, 80*time.Millisecond, "boom-task")

	var ref graph.AgentRef
	var busyErr, closeBusyErr error
	probed := false
	if _, err := runAgentRound(t, store, "s3", slow, "v1", func(ctx context.Context) (string, error) {
		var err error
		if ref, err = graph.SpawnAgent(ctx, spawnSpec("ok-task"), "worker"); err != nil {
			return "", err
		}
		// WaitManaged 挂起后节点会被重放：只记录首次执行时的观察。
		if !probed {
			probed = true
			_, busyErr = graph.FollowupAgent(ctx, ref.AgentID, jsonString("second"))
			_, closeBusyErr = graph.CloseAgent(ctx, ref.AgentID)
		}
		return waitResult(ctx, ref.TaskID)
	}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(busyErr, graph.ErrAgentBusy) || !errors.Is(closeBusyErr, graph.ErrAgentBusy) {
		t.Fatalf("busy=%v closeBusy=%v", busyErr, closeBusyErr)
	}

	// 失败任务：业务失败进入索引，已有结果与上下文保持。
	var failed graph.AgentRef
	if _, err := runAgentRound(t, store, "s3", slow, "v1", func(ctx context.Context) (string, error) {
		var err error
		if failed, err = graph.FollowupAgent(ctx, ref.AgentID, jsonString("boom-task")); err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{failed.TaskID})
		if err != nil {
			return "", err
		}
		return string(results[0].Status), nil
	}); err != nil {
		t.Fatal(err)
	}
	record, _, _ := store.Load(context.Background(), "s3")
	agents, _ := graph.SessionAgentsOf(record)
	if len(agents) != 1 || agents[0].Status != graph.AgentStatusIdle || len(agents[0].Tasks) != 2 ||
		agents[0].Tasks[1].Status != graph.ManagedStatusFailed || !strings.Contains(agents[0].Tasks[1].Error, "boom") ||
		string(agents[0].Tasks[0].Result) != `"done:ok-task|ctx=0"` {
		t.Fatalf("agents=%+v", agents)
	}

	// 失败不改写上下文：下一次成功任务看到的仍是第一次任务的上下文。
	var next string
	if _, err := runAgentRound(t, store, "s3", slow, "v1", func(ctx context.Context) (string, error) {
		r, err := graph.FollowupAgent(ctx, ref.AgentID, jsonString("third"))
		if err != nil {
			return "", err
		}
		next, err = waitResult(ctx, r.TaskID)
		return next, err
	}); err != nil {
		t.Fatal(err)
	}
	if next != "done:third|ctx=1" {
		t.Fatalf("context changed by failed task: %q", next)
	}

	// 其他会话不可见。
	var otherList []graph.AgentSummary
	var otherGet error
	if _, err := runAgentRound(t, store, "other", slow, "v1", func(ctx context.Context) (string, error) {
		var err error
		otherList, err = graph.ListAgents(ctx)
		_, otherGet = graph.GetAgent(ctx, ref.AgentID)
		return "x", err
	}); err != nil {
		t.Fatal(err)
	}
	if len(otherList) != 0 || !errors.Is(otherGet, graph.ErrAgentNotFound) {
		t.Fatalf("other session sees agents: %+v %v", otherList, otherGet)
	}

	// 关闭：拒绝追加，结果仍可读，重复关闭幂等；空闲期间 runner 不再执行。
	var followErr, closeErr, closeAgain error
	var info graph.AgentInfo
	before := started.Load()
	if _, err := runAgentRound(t, store, "s3", slow, "v1", func(ctx context.Context) (string, error) {
		_, closeErr = graph.CloseAgent(ctx, ref.AgentID)
		_, closeAgain = graph.CloseAgent(ctx, ref.AgentID)
		_, followErr = graph.FollowupAgent(ctx, ref.AgentID, jsonString("late"))
		var err error
		info, err = graph.GetAgent(ctx, ref.AgentID)
		return "x", err
	}); err != nil {
		t.Fatal(err)
	}
	if closeErr != nil || closeAgain != nil || !errors.Is(followErr, graph.ErrAgentClosed) {
		t.Fatalf("close=%v again=%v follow=%v", closeErr, closeAgain, followErr)
	}
	if info.Status != graph.AgentStatusClosed || len(info.Tasks) != 3 {
		t.Fatalf("info=%+v", info)
	}
	time.Sleep(120 * time.Millisecond)
	if started.Load() != before {
		t.Fatalf("idle/closed agent executed work: %d -> %d", before, started.Load())
	}
}

// 取消任务不关闭子 Agent：取消后回到空闲，可继续追加，取消原因进入索引。
func TestSessionAgentCancelKeepsAgent(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	slow := sessionAgentBuilder(&started, 5*time.Second, "")
	var ref graph.AgentRef
	var followBusy error
	if _, err := runAgentRound(t, store, "s4", slow, "v1", func(ctx context.Context) (string, error) {
		var err error
		if ref, err = graph.SpawnAgent(ctx, spawnSpec("long"), ""); err != nil {
			return "", err
		}
		if _, err = graph.CancelManaged(ctx, ref.TaskID); err != nil {
			return "", err
		}
		list, err := graph.ListAgents(ctx)
		if err != nil || len(list) != 1 || list[0].Status != graph.AgentStatusIdle || list[0].LastStatus != graph.ManagedStatusCancelled {
			return "", fmt.Errorf("after cancel: %+v %v", list, err)
		}
		_, followBusy = graph.FollowupAgent(ctx, ref.AgentID, jsonString("again"))
		return "x", nil
	}); err != nil {
		t.Fatal(err)
	}
	if followBusy != nil {
		t.Fatalf("followup after cancel: %v", followBusy)
	}
}

// 追加任务沿用登记的构建身份：版本不符明确拒绝，不换实现继续运行。
func TestSessionAgentFollowupBuilderMismatch(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	builder := sessionAgentBuilder(&started, 0, "")
	var ref graph.AgentRef
	if _, err := runAgentRound(t, store, "s5", builder, "v1", func(ctx context.Context) (string, error) {
		var err error
		if ref, err = graph.SpawnAgent(ctx, spawnSpec("t"), ""); err != nil {
			return "", err
		}
		return waitResult(ctx, ref.TaskID)
	}); err != nil {
		t.Fatal(err)
	}
	var followErr error
	if _, err := runAgentRound(t, store, "s5", builder, "v2", func(ctx context.Context) (string, error) {
		_, followErr = graph.FollowupAgent(ctx, ref.AgentID, jsonString("x"))
		return "x", nil
	}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(followErr, graph.ErrManagedBuilder) {
		t.Fatalf("followErr=%v", followErr)
	}
	record, _, _ := store.Load(context.Background(), "s5")
	agents, _ := graph.SessionAgentsOf(record)
	if len(agents[0].Tasks) != 1 || agents[0].Status != graph.AgentStatusIdle {
		t.Fatalf("rejected followup left residue: %+v", agents)
	}
}

// 放弃未完成的运行把子 Agent 集合回退到运行开始前。
func TestSessionAgentsRolledBackByAbandon(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	builder := sessionAgentBuilder(&started, 0, "")
	var first graph.AgentRef
	if _, err := runAgentRound(t, store, "s6", builder, "v1", func(ctx context.Context) (string, error) {
		var err error
		if first, err = graph.SpawnAgent(ctx, spawnSpec("a"), ""); err != nil {
			return "", err
		}
		return waitResult(ctx, first.TaskID)
	}); err != nil {
		t.Fatal(err)
	}
	// 第二轮：创建新子 Agent 后停在审批上。
	_, err := runAgentRound(t, store, "s6", builder, "v1", func(ctx context.Context) (string, error) {
		ref, err := graph.SpawnAgent(ctx, spawnSpec("b"), "")
		if err != nil {
			return "", err
		}
		if _, err := waitResult(ctx, ref.TaskID); err != nil {
			return "", err
		}
		_, err = graph.Interrupt(ctx, "approve?")
		return "", err
	})
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want interrupt, got %v", err)
	}
	record, _, _ := store.Load(context.Background(), "s6")
	if agents, _ := graph.SessionAgentsOf(record); len(agents) != 2 {
		t.Fatalf("mid-run agents=%d", len(agents))
	}
	if _, err := state.AbandonRun(context.Background(), store, "s6"); err != nil {
		t.Fatal(err)
	}
	record, _, _ = store.Load(context.Background(), "s6")
	agents, err := graph.SessionAgentsOf(record)
	if err != nil || len(agents) != 1 || agents[0].AgentID != first.AgentID {
		t.Fatalf("after abandon: %+v %v", agents, err)
	}
}

// 子 Agent 创建的孙 Agent 只对创建者可见；根看不到，创建者在下一轮追加任务时仍能找到。
func TestSessionAgentCreatorScope(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	var rootView, childView atomic.Value
	leaf := sessionAgentBuilder(&started, 0, "")
	// "agent" 构建器根据输入分流：输入为 "spawner" 的任务内部创建孙 Agent 并回报其可见性。
	builder := func(ctx context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		inner, err := leaf(ctx, spec)
		if err != nil {
			return nil, err
		}
		return func(rctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			if string(input) != `"spawner"` {
				return inner(rctx, input)
			}
			// 受管运行内没有任务现场；经一张图运行节点来获得调用上下文。
			g := newManagedStateGraph(t, "grand")
			node := runnable.Func[string, string](func(nctx context.Context, _ string, _ ...runnable.Option) (string, error) {
				ref, err := graph.SpawnAgent(nctx, spawnSpec("grand"), "孙")
				if err != nil {
					return "", err
				}
				list, err := graph.ListAgents(nctx)
				if err != nil {
					return "", err
				}
				childView.Store(len(list))
				return waitResult(nctx, ref.TaskID)
			})
			if err := g.AddNode("n", node, graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil })); err != nil {
				return nil, err
			}
			if err := g.AddEdge(graph.Start, "n"); err != nil {
				return nil, err
			}
			if err := g.AddEdge("n", graph.End); err != nil {
				return nil, err
			}
			compiled, err := g.Compile()
			if err != nil {
				return nil, err
			}
			out, err := compiled.Invoke(rctx, "x")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	if _, err := runAgentRound(t, store, "s7", builder, "v1", func(ctx context.Context) (string, error) {
		ref, err := graph.SpawnAgent(ctx, spawnSpec("spawner"), "子")
		if err != nil {
			return "", err
		}
		if _, err := waitResult(ctx, ref.TaskID); err != nil {
			return "", err
		}
		list, err := graph.ListAgents(ctx)
		rootView.Store(len(list))
		return "x", err
	}); err != nil {
		t.Fatal(err)
	}
	if rootView.Load() != 1 || childView.Load() != 1 {
		t.Fatalf("root sees %v, child sees %v; want 1 and 1", rootView.Load(), childView.Load())
	}
	record, _, _ := store.Load(context.Background(), "s7")
	agents, _ := graph.SessionAgentsOf(record)
	if len(agents) != 2 || agents[0].Creator != "" || agents[1].Creator != agents[0].AgentID {
		t.Fatalf("creator relation: %+v", agents)
	}
}

// 损坏的集合记录在新一轮入口被拒绝，不悄悄丢弃。
func TestSessionAgentsCorruptRecordRejected(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	builder := sessionAgentBuilder(&started, 0, "")
	if _, err := runAgentRound(t, store, "s8", builder, "v1", func(ctx context.Context) (string, error) {
		ref, err := graph.SpawnAgent(ctx, spawnSpec("a"), "")
		if err != nil {
			return "", err
		}
		return waitResult(ctx, ref.TaskID)
	}); err != nil {
		t.Fatal(err)
	}
	record, _, _ := store.Load(context.Background(), "s8")
	record.SubAgents = json.RawMessage(`{"format":1,"agents":[{"id":"agent-1","spec":{"builder":"agent"},"status":"weird"}]}`)
	if _, err := store.Save(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	_, err := runAgentRound(t, store, "s8", builder, "v1", func(ctx context.Context) (string, error) { return "x", nil })
	if !errors.Is(err, state.ErrInvalidCheckpoint) {
		t.Fatalf("err=%v", err)
	}
}
