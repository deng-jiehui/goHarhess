package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// --- 阶段 2 复核回归：七项 P1/P2 场景 ---

// TestManagedRootCompletionAfterConverge（P1-1）：受管任务未结束时根运行
// 不得标记完成。子任务阻塞期间轮询检查点：Completed 只能在受管全部收敛
// 后出现。
func TestManagedRootCompletionAfterConverge(t *testing.T) {
	store := state.NewMemoryStore()
	release := make(chan struct{})
	builder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return json.Marshal("done")
		}, nil
	}
	g := newManagedStateGraph(t, "managed-tail")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "block", Version: "v1", Input: json.RawMessage(`"t"`)}); err != nil {
			return "", err
		}
		return input, nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	done := make(chan error, 1)
	go func() {
		out, err := compiled.Invoke(context.Background(), "x",
			graph.WithCheckpoint(store, "tail1"),
			graph.WithManagedBuilder("block", "v1", builder),
		)
		if err == nil && out != "x" {
			err = fmt.Errorf("unexpected output %q", out)
		}
		done <- err
	}()
	// 子任务仍阻塞时：记录不得呈现 Completed。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		record, found, err := store.Load(context.Background(), "tail1")
		if err == nil && found && record.Status == state.StatusCompleted {
			t.Fatal("root marked completed while managed task still running")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("invoke: %v", err)
	}
	record, found, err := store.Load(context.Background(), "tail1")
	if err != nil || !found {
		t.Fatalf("load checkpoint: %v found=%v", err, found)
	}
	if record.Status != state.StatusCompleted {
		t.Fatalf("final status %q, want completed", record.Status)
	}
}

// TestManagedFrameworkErrorPropagates（P1-2）：受管执行返回恢复类框架
// 错误时按框架错误上报——任务保持 running+fatal 供修正后恢复，不折叠为
// 业务失败结果。修正后 Resume 能从保存的边界重试并完成。
func TestManagedFrameworkErrorPropagates(t *testing.T) {
	store := state.NewMemoryStore()
	var fixed atomic.Bool
	builder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			if !fixed.Load() {
				return nil, fmt.Errorf("persist write: %w", graph.ErrResumeIncompatible)
			}
			return json.Marshal("recovered")
		}, nil
	}
	g := newManagedStateGraph(t, "managed-fatal")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "flaky", Version: "v1", Input: json.RawMessage(`"t"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	_, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "fatal1"),
		graph.WithManagedBuilder("flaky", "v1", builder),
	)
	if !errors.Is(err, graph.ErrResumeIncompatible) {
		t.Fatalf("want framework error ErrResumeIncompatible, got %v", err)
	}
	// 框架错误不落为业务失败：任务保持 running + fatal 现场。
	record, found, lerr := store.Load(context.Background(), "fatal1")
	if lerr != nil || !found {
		t.Fatalf("load checkpoint: %v found=%v", lerr, found)
	}
	var tree struct {
		Managed []struct {
			Status string `json:"status"`
			Fatal  bool   `json:"fatal"`
		} `json:"managed"`
	}
	if err := json.Unmarshal(record.Execution, &tree); err != nil {
		t.Fatalf("decode execution: %v", err)
	}
	if len(tree.Managed) != 1 || tree.Managed[0].Status != "running" || !tree.Managed[0].Fatal {
		t.Fatalf("fatal task should stay running+fatal, got %+v", tree.Managed)
	}
	// 修正后恢复：重试已保存边界，任务完成、根运行收敛。
	fixed.Store(true)
	out, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "fatal1"),
		graph.WithManagedBuilder("flaky", "v1", builder),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != `"recovered"` {
		t.Fatalf("unexpected output %q", out)
	}
}

// TestManagedInboxAckReplayAcrossSuspend（P1-3）：已确认消息按任务批次
// 记账——任务挂起未提交时确认不算数，恢复重放把批次重新投递。
func TestManagedInboxAckReplayAcrossSuspend(t *testing.T) {
	store := state.NewMemoryStore()
	var deliveries atomic.Int32
	inner := graph.NewGraph[string, string]("managed-inbox-inner")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	worker := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		msgs, err := graph.ManagedInbox(ctx)
		if err != nil {
			return "", err
		}
		processed := ""
		for _, msg := range msgs {
			deliveries.Add(1)
			processed = string(msg.Payload)
		}
		if len(msgs) > 0 {
			if err := graph.AckManagedInbox(ctx, len(msgs)); err != nil {
				return "", err
			}
		}
		// 确认后挂起：恢复重放本节点时，未提交的确认必须重新投递。
		answer, err := graph.Interrupt(ctx, "proceed")
		if err != nil {
			return "", err
		}
		return "seen:" + processed + ":" + fmt.Sprint(answer), nil
	})
	if err := inner.AddNode("worker", worker,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(graph.Start, "worker"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("worker", graph.End); err != nil {
		t.Fatal(err)
	}
	innerCompiled := mustCompile(t, inner)
	innerRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			out, err := innerCompiled.Invoke(ctx, "task")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	g := newManagedStateGraph(t, "managed-inbox-ack")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "inner", Version: "v1", Input: json.RawMessage(`"t"`)})
		if err != nil {
			return "", err
		}
		if _, err := graph.SendManagedMessage(ctx, id, "payload-1"); err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	_, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "ack1"),
		graph.WithManagedBuilder("inner", "v1", innerRunner),
	)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want InterruptedError, got %v", err)
	}
	out, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "ack1"),
		graph.WithManagedBuilder("inner", "v1", innerRunner),
		graph.WithResume(interrupted.PendingInterrupts()[0].ID, "go"),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	// 消息在挂起前与恢复重放各投递一次——已确认批次未提交不丢失。
	if deliveries.Load() != 2 {
		t.Fatalf("message delivered %d times, want 2 (suspend + replay)", deliveries.Load())
	}
	if !strings.Contains(out, "payload-1") {
		t.Fatalf("unexpected output %q", out)
	}
}

// TestManagedRunScopeUnique（P1-4）：独立受管子执行的外部幂等键互不
// 相同——每个任务的运行 scope 必须叠加自己的任务路径。
func TestManagedRunScopeUnique(t *testing.T) {
	store := state.NewMemoryStore()
	var scopesMu sync.Mutex
	scopes := map[string]bool{}
	inner := graph.NewGraph[string, string]("managed-scope-probe")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	probe := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		if recorder, ok := control.CallRecorderFromContext(ctx); ok {
			scopesMu.Lock()
			scopes[recorder.BeginBatch().Namespace()] = true
			scopesMu.Unlock()
		}
		return input, nil
	})
	if err := inner.AddNode("probe", probe); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(graph.Start, "probe"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("probe", graph.End); err != nil {
		t.Fatal(err)
	}
	innerCompiled := mustCompile(t, inner)
	innerRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			out, err := innerCompiled.Invoke(ctx, "t")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	g := newManagedStateGraph(t, "managed-scope-key")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		var ids []string
		for i := 0; i < 2; i++ {
			id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "inner", Version: "v1", Input: json.RawMessage(`"t"`)})
			if err != nil {
				return "", err
			}
			ids = append(ids, id)
		}
		if _, err := graph.WaitManaged(ctx, ids); err != nil {
			return "", err
		}
		return input, nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "scope1"),
		graph.WithManagedBuilder("inner", "v1", innerRunner),
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	scopesMu.Lock()
	defer scopesMu.Unlock()
	if len(scopes) != 2 {
		t.Fatalf("managed tasks shared external idempotency scope: %v", scopes)
	}
	for scope := range scopes {
		if !strings.Contains(scope, "mt/") {
			t.Fatalf("scope %q does not carry managed task path", scope)
		}
	}
}

// TestManagedSharedConcurrencyQuota（P2-5）：父批内任务与受管协程从同一
// 并发额度取额度——上限 1 时两者不得重叠执行。
func TestManagedSharedConcurrencyQuota(t *testing.T) {
	store := state.NewMemoryStore()
	var cur, maxSeen atomic.Int32
	timed := func() func() {
		cur.Add(1)
		for {
			seen := maxSeen.Load()
			if now := cur.Load(); now > seen && !maxSeen.CompareAndSwap(seen, now) {
				continue
			}
			break
		}
		return func() { cur.Add(-1) }
	}
	builder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			defer timed()()
			time.Sleep(80 * time.Millisecond)
			return json.Marshal("done")
		}, nil
	}
	g := newManagedStateGraph(t, "managed-quota")
	if err := g.SetMaxConcurrency(2); err != nil {
		t.Fatal(err)
	}
	spawner := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "slow", Version: "v1", Input: json.RawMessage(`"t"`)}); err != nil {
			return "", err
		}
		return input + ":a", nil
	})
	branch := func(name string) graph.ParallelBranch {
		return graph.ParallelBranch{
			Name: name,
			Node: runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
				defer timed()()
				time.Sleep(60 * time.Millisecond)
				return input + ":" + name, nil
			}),
			Pre:  func(_ context.Context, s *managedState) (string, error) { return s.In, nil },
			Post: func(_ context.Context, _ *managedState, _ string) error { return nil },
		}
	}
	if err := g.AddNode("a", spawner,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddParallelNode("fan", []graph.ParallelBranch{branch("b1"), branch("b2")},
		graph.WithStateMerger(func(_ context.Context, base *managedState, _ []*managedState) error { return nil }),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("finish", finish,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "a"}, {"a", "fan"}, {"fan", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "q1"),
		graph.WithManagedBuilder("slow", "v1", builder),
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	// 共享额度：受管协程与批内分支从同一池取额度——上限 2 时三者
	// （受管 + 两分支）不得全部同时运行；受管睡眠更长，必然与至少
	// 一个分支重叠，峰值恰为 2。旧实现各持独立池时峰值为 3。
	if maxSeen.Load() != 2 {
		t.Fatalf("concurrent executions %d, want exactly 2 (shared quota)", maxSeen.Load())
	}
}

// TestManagedIDHierarchyUnique（P2-6）：任务 ID 携带完整 owner 路径——
// 不同层级的同序创建不产生重复 ID。
func TestManagedIDHierarchyUnique(t *testing.T) {
	store := state.NewMemoryStore()
	inner := graph.NewGraph[string, string]("managed-id-inner")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	spawnLeaf := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "leaf", Version: "v1", Input: json.RawMessage(`"l"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := inner.AddNode("spawn_leaf", spawnLeaf); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(graph.Start, "spawn_leaf"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("spawn_leaf", graph.End); err != nil {
		t.Fatal(err)
	}
	innerCompiled := mustCompile(t, inner)
	innerRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			out, err := innerCompiled.Invoke(ctx, "t")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	leaf := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			return input, nil
		}, nil
	}
	g := newManagedStateGraph(t, "managed-id")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "inner", Version: "v1", Input: json.RawMessage(`"t"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "id1"),
		graph.WithManagedBuilder("inner", "v1", innerRunner),
		graph.WithManagedBuilder("leaf", "v1", leaf),
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	record, found, lerr := store.Load(context.Background(), "id1")
	if lerr != nil || !found {
		t.Fatalf("load checkpoint: %v found=%v", lerr, found)
	}
	var tree struct {
		Managed []struct {
			ID    string `json:"id"`
			Frame struct {
				Managed []struct {
					ID string `json:"id"`
				} `json:"managed"`
			} `json:"frame"`
		} `json:"managed"`
	}
	if err := json.Unmarshal(record.Execution, &tree); err != nil {
		t.Fatalf("decode execution: %v", err)
	}
	if len(tree.Managed) != 1 || len(tree.Managed[0].Frame.Managed) != 1 {
		t.Fatalf("expected parent+child managed tasks, got %+v", tree.Managed)
	}
	parentID := tree.Managed[0].ID
	childID := tree.Managed[0].Frame.Managed[0].ID
	if parentID == childID {
		t.Fatalf("parent and child share task ID %q", parentID)
	}
	if !strings.Contains(childID, parentID) {
		t.Fatalf("child ID %q should carry owner path %q", childID, parentID)
	}
}

// TestManagedWithoutCheckpoint（P2-7）：无检查点运行可以创建并等待受管
// 任务（内存协调、运行内收敛）；需要可恢复中断的受管任务按
// ErrCheckpointRequired 拒绝。
func TestManagedWithoutCheckpoint(t *testing.T) {
	var started atomic.Int32
	g := newManagedStateGraph(t, "managed-memory")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "echo", Version: "v1", Input: json.RawMessage(`"t"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	out, err := compiled.Invoke(context.Background(), "x",
		graph.WithManagedBuilder("echo", "v1", managedEcho(&started, 0, nil)),
	)
	if err != nil {
		t.Fatalf("memory-mode invoke: %v", err)
	}
	if out != `"done:t"` || started.Load() != 1 {
		t.Fatalf("unexpected output %q runs=%d", out, started.Load())
	}

	// 需要审批中断的受管任务在无检查点运行下按框架错误拒绝。
	suspInner := graph.NewGraph[string, string]("managed-memory-inner")
	if err := suspInner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		if _, err := graph.Interrupt(ctx, "approve"); err != nil {
			return "", err
		}
		return input, nil
	})
	if err := suspInner.AddNode("ask", ask,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := suspInner.AddEdge(graph.Start, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := suspInner.AddEdge("ask", graph.End); err != nil {
		t.Fatal(err)
	}
	suspCompiled := mustCompile(t, suspInner)
	suspendRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			if _, err := suspCompiled.Invoke(ctx, "t"); err != nil {
				return nil, err
			}
			return json.Marshal("ok")
		}, nil
	}
	g2 := newManagedStateGraph(t, "managed-memory-susp")
	node2 := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "susp", Version: "v1", Input: json.RawMessage(`"t"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := g2.AddNode("main", node2,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g2.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g2.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled2 := mustCompile(t, g2)
	_, err = compiled2.Invoke(context.Background(), "x",
		graph.WithManagedBuilder("susp", "v1", suspendRunner),
	)
	if !errors.Is(err, graph.ErrCheckpointRequired) {
		t.Fatalf("want ErrCheckpointRequired, got %v", err)
	}
}

// --- 阶段 2 第二轮复核回归 ---

// TestReviewManagedSharedConcurrency（18.1 复现一）：上限 1 时普通顺序
// 节点也是工作单元——父节点创建工作单元后继续干活，与子任务不得重叠。
func TestReviewManagedSharedConcurrency(t *testing.T) {
	store := state.NewMemoryStore()
	var cur, maxSeen atomic.Int32
	timed := func() func() {
		cur.Add(1)
		for {
			seen := maxSeen.Load()
			if now := cur.Load(); now > seen && !maxSeen.CompareAndSwap(seen, now) {
				continue
			}
			break
		}
		return func() { cur.Add(-1) }
	}
	builder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			defer timed()()
			time.Sleep(80 * time.Millisecond)
			return json.Marshal("done")
		}, nil
	}
	g := newManagedStateGraph(t, "review-quota-plain")
	if err := g.SetMaxConcurrency(1); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "slow", Version: "v1", Input: json.RawMessage(`"t"`)}); err != nil {
			return "", err
		}
		// 父节点自身的工作也是工作单元：上限 1 时受管任务必须等本节点
		// 结束后才能领取额度。
		defer timed()()
		time.Sleep(80 * time.Millisecond)
		return input, nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := compiled.Invoke(ctx, "x",
		graph.WithCheckpoint(store, "rq1"),
		graph.WithManagedBuilder("slow", "v1", builder),
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if maxSeen.Load() != 1 {
		t.Fatalf("concurrent executions %d exceeded shared quota 1", maxSeen.Load())
	}
}

// TestReviewManagedParallelSubgraphQuota（18.1 复现二）：上限 1 时受管
// 任务执行含并行节点的子图不得卡死——驱动协程不持额度包围整图，子图
// 内部分支向同一额度池领取。
func TestReviewManagedParallelSubgraphQuota(t *testing.T) {
	store := state.NewMemoryStore()
	var ran atomic.Int32
	inner := graph.NewGraph[string, string]("review-quota-inner")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	branch := func(name string) graph.ParallelBranch {
		return graph.ParallelBranch{
			Name: name,
			Node: runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
				ran.Add(1)
				time.Sleep(40 * time.Millisecond)
				return input + ":" + name, nil
			}),
			Pre:  func(_ context.Context, s *managedState) (string, error) { return s.In, nil },
			Post: func(_ context.Context, _ *managedState, _ string) error { return nil },
		}
	}
	if err := inner.AddParallelNode("fan", []graph.ParallelBranch{branch("b1"), branch("b2")},
		graph.WithStateMerger(func(_ context.Context, base *managedState, _ []*managedState) error { return nil }),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return "inner-done", nil
	})
	if err := inner.AddNode("finish", finish,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "fan"}, {"fan", "finish"}, {"finish", graph.End}} {
		if err := inner.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	innerCompiled := mustCompile(t, inner)
	innerRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			out, err := innerCompiled.Invoke(ctx, "t")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	g := newManagedStateGraph(t, "review-quota-sub")
	if err := g.SetMaxConcurrency(1); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "inner", Version: "v1", Input: json.RawMessage(`"t"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := g.AddNode("main", node,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	// 旧实现：驱动协程持有额度调 runner，子图内分支再向同一池领取——
	// 上限 1 时互相等待直到 ctx 超时。给足时限，完成即修复。
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := compiled.Invoke(ctx, "x",
		graph.WithCheckpoint(store, "rq2"),
		graph.WithManagedBuilder("inner", "v1", innerRunner),
	)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if ran.Load() != 2 {
		t.Fatalf("parallel branches ran %d, want 2", ran.Load())
	}
	if out != `"inner-done"` {
		t.Fatalf("unexpected output %q", out)
	}
}

// TestReviewManagedNestedTailApproval（18.2 复现）：根到 End 时的全树
// 收敛等待引用孙任务——等待记录可保存、可校验、回答后可恢复。
func TestReviewManagedNestedTailApproval(t *testing.T) {
	store := state.NewMemoryStore()
	var mainRuns atomic.Int32
	// 孙图：节点请求审批，回答后产出结果。
	grand := graph.NewGraph[string, string]("review-grand")
	if err := grand.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, "approve")
		if err != nil {
			return "", err
		}
		return "grand:" + fmt.Sprint(answer), nil
	})
	if err := grand.AddNode("ask", ask,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := grand.AddEdge(graph.Start, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := grand.AddEdge("ask", graph.End); err != nil {
		t.Fatal(err)
	}
	grandCompiled := mustCompile(t, grand)
	// 子图：创建孙任务并等待——等待期间子任务挂起。
	child := graph.NewGraph[string, string]("review-child")
	if err := child.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	spawn := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "grand", Version: "v1", Input: json.RawMessage(`"g"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return "child:" + string(results[0].Result), nil
	})
	if err := child.AddNode("spawn", spawn,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge(graph.Start, "spawn"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("spawn", graph.End); err != nil {
		t.Fatal(err)
	}
	childCompiled := mustCompile(t, child)
	childRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			out, err := childCompiled.Invoke(ctx, "t")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	grandRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			out, err := grandCompiled.Invoke(ctx, "g")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	// 根图：创建子任务后不等待，直接到 End——由根收敛统一等待。
	g := newManagedStateGraph(t, "review-tail")
	main := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		mainRuns.Add(1)
		if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "child", Version: "v1", Input: json.RawMessage(`"t"`)}); err != nil {
			return "", err
		}
		return input + ":root", nil
	})
	if err := g.AddNode("main", main,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	_, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "nt1"),
		graph.WithManagedBuilder("child", "v1", childRunner),
		graph.WithManagedBuilder("grand", "v1", grandRunner),
	)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want InterruptedError (grandchild approval), got %v", err)
	}
	pending := interrupted.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("pending interrupts %d, want 1", len(pending))
	}
	// 回答孙任务问题：等待记录按全树范围解除，根收敛后产出原输出。
	out, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "nt1"),
		graph.WithManagedBuilder("child", "v1", childRunner),
		graph.WithManagedBuilder("grand", "v1", grandRunner),
		graph.WithResume(pending[0].ID, "yes"),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != "x:root" {
		t.Fatalf("unexpected output %q", out)
	}
	if mainRuns.Load() != 1 {
		t.Fatalf("root tail node re-ran %d times, want 1", mainRuns.Load())
	}
}

// --- 阶段 2 第三轮复核回归：分批审批 ---

// approvalGraph 构造一个「节点请求审批、回答后返回 tagged 输出」的子图；
// runs 记录节点实际执行次数，用于验证已获答任务不重跑。
func approvalGraph(t *testing.T, name string, runs *atomic.Int32) *graph.CompiledGraph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string](name)
	if err := g.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		runs.Add(1)
		answer, err := graph.Interrupt(ctx, "approve:"+name)
		if err != nil {
			return "", err
		}
		return name + ":" + fmt.Sprint(answer), nil
	})
	if err := g.AddNode("ask", ask,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("ask", graph.End); err != nil {
		t.Fatal(err)
	}
	return mustCompile(t, g)
}

func graphRunnerOf(compiled *graph.CompiledGraph[string, string]) graph.ManagedBuilder {
	return func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			out, err := compiled.Invoke(ctx, "t")
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
}

// managedTreeStatuses 解码检查点执行树中根 frame 直属受管任务的状态。
func managedTreeStatuses(t *testing.T, record state.Checkpoint) map[string]string {
	t.Helper()
	var tree struct {
		Managed []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Frame  struct {
				Interrupts []struct {
					ID       string `json:"id"`
					Answered bool   `json:"answered"`
				} `json:"interrupts"`
				Waiting struct {
					Kind  string   `json:"kind"`
					Tasks []string `json:"tasks"`
					Tree  bool     `json:"tree"`
				} `json:"waiting"`
				Managed []struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				} `json:"managed"`
			} `json:"frame"`
		} `json:"managed"`
	}
	if err := json.Unmarshal(record.Execution, &tree); err != nil {
		t.Fatalf("decode execution: %v", err)
	}
	statuses := map[string]string{}
	for _, m := range tree.Managed {
		unanswered := 0
		for _, r := range m.Frame.Interrupts {
			if !r.Answered {
				unanswered++
			}
		}
		statuses[m.ID] = fmt.Sprintf("%s(q=%d,w=%s%v)", m.Status, unanswered, m.Frame.Waiting.Kind, m.Frame.Waiting.Tasks)
		for _, nested := range m.Frame.Managed {
			statuses[nested.ID] = nested.Status
		}
	}
	return statuses
}

// TestManagedPartialTailApprovalDirect（20.1 复现一）：两个直属子任务
// 各挂审批，只回答 A——A 先继续并收敛为 completed，根返回 B 的原问题；
// 再回答 B，A 不重跑，根返回冻结输出。
func TestManagedPartialTailApprovalDirect(t *testing.T) {
	store := state.NewMemoryStore()
	var runsA, runsB, mainRuns atomic.Int32
	runnerA := graphRunnerOf(approvalGraph(t, "pa", &runsA))
	runnerB := graphRunnerOf(approvalGraph(t, "pb", &runsB))
	g := newManagedStateGraph(t, "partial-tail")
	main := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		mainRuns.Add(1)
		for _, b := range []string{"A", "B"} {
			if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: b, Version: "v1", Input: json.RawMessage(`"t"`)}); err != nil {
				return "", err
			}
		}
		return input + ":root", nil
	})
	if err := g.AddNode("main", main,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	opts := func() []runnable.Option {
		return []runnable.Option{
			graph.WithCheckpoint(store, "pt1"),
			graph.WithManagedBuilder("A", "v1", runnerA),
			graph.WithManagedBuilder("B", "v1", runnerB),
		}
	}
	_, err := compiled.Invoke(context.Background(), "x", opts()...)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want InterruptedError, got %v", err)
	}
	pending := interrupted.PendingInterrupts()
	if len(pending) != 2 {
		for i, r := range pending {
			t.Logf("pending[%d] id=%s node=%s call=%s payload=%s", i, r.ID, r.Node, r.Call, string(r.Payload))
		}
		if rec, found, _ := store.Load(context.Background(), "pt1"); found {
			t.Logf("statuses: %v", managedTreeStatuses(t, rec))
		}
		t.Fatalf("pending %d, want 2", len(pending))
	}
	// 按问题文本区分 A/B 的审批记录。
	var qa, qb string
	for _, record := range pending {
		switch {
		case strings.Contains(string(record.Payload), "pa"):
			qa = record.ID
		case strings.Contains(string(record.Payload), "pb"):
			qb = record.ID
		}
	}
	if qa == "" || qb == "" {
		t.Fatalf("cannot identify A/B questions in %+v", pending)
	}
	// 只回答 A：A 继续执行完成；根仍挂起并返回 B 的原问题 ID。
	_, err = compiled.Resume(context.Background(), append(opts(), graph.WithResume(qa, "yes"))...)
	if !errors.As(err, &interrupted) {
		t.Fatalf("resume A: want InterruptedError, got %v", err)
	}
	rest := interrupted.PendingInterrupts()
	if len(rest) != 1 || rest[0].ID != qb {
		t.Fatalf("after answering A, pending = %+v, want only B's question", rest)
	}
	record, found, lerr := store.Load(context.Background(), "pt1")
	if lerr != nil || !found {
		t.Fatalf("load checkpoint: %v found=%v", lerr, found)
	}
	if record.Status == state.StatusCompleted {
		t.Fatal("root marked completed while B still suspended")
	}
	statuses := managedTreeStatuses(t, record)
	completedA := false
	suspendedB := false
	// 无法从 ID 直接区分 A/B——按执行计数核对：A 已跑完一轮并产出，
	// B 尚未重跑（仍 suspended）。
	if runsA.Load() != 2 {
		t.Fatalf("A node ran %d times, want 2 (suspend + resume)", runsA.Load())
	}
	if runsB.Load() != 1 {
		t.Fatalf("B node ran %d times, want 1 (still suspended)", runsB.Load())
	}
	for _, st := range statuses {
		if strings.HasPrefix(st, "completed") {
			completedA = true
		}
		if strings.HasPrefix(st, "suspended") {
			suspendedB = true
		}
	}
	if !completedA || !suspendedB {
		t.Fatalf("want one completed + one suspended task, got %v", statuses)
	}
	// 再回答 B：A 不重跑，根收敛返回冻结输出。
	out, err := compiled.Resume(context.Background(), append(opts(), graph.WithResume(qb, "ok"))...)
	if err != nil {
		t.Fatalf("resume B: %v", err)
	}
	if out != "x:root" {
		t.Fatalf("unexpected output %q", out)
	}
	if runsA.Load() != 2 || runsB.Load() != 2 || mainRuns.Load() != 1 {
		t.Fatalf("runs A=%d B=%d main=%d, want 2/2/1", runsA.Load(), runsB.Load(), mainRuns.Load())
	}
}

// TestManagedPartialTailApprovalNested（20.1 复现二）：子任务已完成后
// 其下两个孙任务待审批——只回答孙一，孙一先推进完成，根返回孙二的
// 原问题；不带新答案的 Resume 不再推进；回答孙二后根收敛。
func TestManagedPartialTailApprovalNested(t *testing.T) {
	store := state.NewMemoryStore()
	var runsG1, runsG2, childRuns, mainRuns atomic.Int32
	runnerG1 := graphRunnerOf(approvalGraph(t, "g1", &runsG1))
	runnerG2 := graphRunnerOf(approvalGraph(t, "g2", &runsG2))
	// 子图：只创建两个孙任务就返回（自身结束，孙任务由根收敛接管）。
	child := graph.NewGraph[string, string]("review-nested-child")
	if err := child.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	spawn := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		childRuns.Add(1)
		for _, b := range []string{"G1", "G2"} {
			if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: b, Version: "v1", Input: json.RawMessage(`"g"`)}); err != nil {
				return "", err
			}
		}
		return input + ":child", nil
	})
	if err := child.AddNode("spawn", spawn,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge(graph.Start, "spawn"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("spawn", graph.End); err != nil {
		t.Fatal(err)
	}
	childRunner := graphRunnerOf(mustCompile(t, child))
	g := newManagedStateGraph(t, "partial-tail-nested")
	main := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		mainRuns.Add(1)
		if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "child", Version: "v1", Input: json.RawMessage(`"t"`)}); err != nil {
			return "", err
		}
		return input + ":root", nil
	})
	if err := g.AddNode("main", main,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("main", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	opts := func() []runnable.Option {
		return []runnable.Option{
			graph.WithCheckpoint(store, "pt2"),
			graph.WithManagedBuilder("child", "v1", childRunner),
			graph.WithManagedBuilder("G1", "v1", runnerG1),
			graph.WithManagedBuilder("G2", "v1", runnerG2),
		}
	}
	_, err := compiled.Invoke(context.Background(), "x", opts()...)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want InterruptedError, got %v", err)
	}
	pending := interrupted.PendingInterrupts()
	if len(pending) != 2 {
		t.Fatalf("pending %d, want 2 grandchild questions", len(pending))
	}
	var q1, q2 string
	for _, record := range pending {
		switch {
		case strings.Contains(string(record.Payload), "g1"):
			q1 = record.ID
		case strings.Contains(string(record.Payload), "g2"):
			q2 = record.ID
		}
	}
	if q1 == "" || q2 == "" {
		t.Fatalf("cannot identify grandchild questions in %+v", pending)
	}
	// 只回答孙一：孙一完成；根仍挂起返回孙二的原问题。
	_, err = compiled.Resume(context.Background(), append(opts(), graph.WithResume(q1, "yes"))...)
	if !errors.As(err, &interrupted) {
		t.Fatalf("resume g1: want InterruptedError, got %v", err)
	}
	rest := interrupted.PendingInterrupts()
	if len(rest) != 1 || rest[0].ID != q2 {
		t.Fatalf("after answering g1, pending = %+v, want only g2's question", rest)
	}
	if runsG1.Load() != 2 || runsG2.Load() != 1 {
		t.Fatalf("runs g1=%d g2=%d, want 2/1", runsG1.Load(), runsG2.Load())
	}
	// 不带新答案的 Resume：无可推进工作时返回剩余审批，不空跑。
	_, err = compiled.Resume(context.Background(), opts()...)
	if !errors.As(err, &interrupted) {
		t.Fatalf("resume without answers: want InterruptedError, got %v", err)
	}
	if rest = interrupted.PendingInterrupts(); len(rest) != 1 || rest[0].ID != q2 {
		t.Fatalf("no-answer resume pending = %+v, want g2's question", rest)
	}
	// 回答孙二：根收敛返回冻结输出；已结束子任务与孙一不重跑。
	out, err := compiled.Resume(context.Background(), append(opts(), graph.WithResume(q2, "ok"))...)
	if err != nil {
		t.Fatalf("resume g2: %v", err)
	}
	if out != "x:root" {
		t.Fatalf("unexpected output %q", out)
	}
	if mainRuns.Load() != 1 || childRuns.Load() != 1 || runsG1.Load() != 2 || runsG2.Load() != 2 {
		t.Fatalf("runs main=%d child=%d g1=%d g2=%d, want 1/1/2/2",
			mainRuns.Load(), childRuns.Load(), runsG1.Load(), runsG2.Load())
	}
}
