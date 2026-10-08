package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// managedState 记录创建的受管任务 ID 与等待结果，随检查点持久化。
type managedState struct {
	In      string
	Spawned string
	Out     string
}

// managedEcho 是测试用构建函数：输入原样包装返回；started 记录 runner
// 实际执行次数；ran 标记 runner 曾推进到终点。
func managedEcho(started *atomic.Int32, delay time.Duration, ran *atomic.Bool) graph.ManagedBuilder {
	return func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			if started != nil {
				started.Add(1)
			}
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			if ran != nil {
				ran.Store(true)
			}
			var task string
			_ = json.Unmarshal(input, &task)
			return json.Marshal("done:" + task)
		}, nil
	}
}

func newManagedStateGraph(t *testing.T, name string) *graph.Graph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string](name)
	if err := g.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

// TestManagedSpawnAndWait：任务内创建子任务并等待——等待期间任务挂起
// 释放槽位；子任务完成后同一次调用内唤醒并收敛到结果。
func TestManagedSpawnAndWait(t *testing.T) {
	store := state.NewMemoryStore()
	var started atomic.Int32
	g := newManagedStateGraph(t, "managed-wait")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		in, _ := json.Marshal(input)
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "echo", Version: "v1", Input: in})
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
	out, err := compiled.Invoke(context.Background(), "audit",
		graph.WithCheckpoint(store, "m1"),
		graph.WithManagedBuilder("echo", "v1", managedEcho(&started, 40*time.Millisecond, nil)),
	)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if started.Load() != 1 {
		t.Fatalf("runner started %d times, want 1", started.Load())
	}
	if out != `"done:audit"` {
		t.Fatalf("unexpected output %q", out)
	}
}

// TestManagedSurvivesParentNode：子任务在创建它的节点结束后继续存在；
// 根图结束时运行时收敛未结束的受管任务后才产出。
func TestManagedSurvivesParentNode(t *testing.T) {
	store := state.NewMemoryStore()
	var ran atomic.Bool
	g := newManagedStateGraph(t, "managed-survive")
	// 受管任务延迟完成：创建它的节点早已返回，证明其生命周期独立。
	builder := managedEcho(nil, 60*time.Millisecond, &ran)
	spawner := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		in, _ := json.Marshal(input)
		if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "echo", Version: "v1", Input: in}); err != nil {
			return "", err
		}
		return input + ":a", nil
	})
	trailer := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		// 子任务此时仍在后台执行；不做任何等待。
		return input + ":b", nil
	})
	if err := g.AddNode("a", spawner,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("b", trailer); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "a"}, {"a", "b"}, {"b", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled := mustCompile(t, g)
	out, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "sv1"),
		graph.WithManagedBuilder("echo", "v1", builder),
	)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out != "x:a:b" {
		t.Fatalf("unexpected output %q", out)
	}
	// 根收敛等待未结束受管任务：返回时 runner 必然已完成。
	if !ran.Load() {
		t.Fatal("managed task not converged before run returned")
	}
}

// TestManagedWaitScope：未知任务与非本 frame 任务在登记前被拒绝。受管
// 任务内部等待根 frame 中的自身记录属于越权（自等待），返回 Scope；
// 不存在的 ID 返回 NotFound。同 frame 内跨节点等待是合法形态。
func TestManagedWaitScope(t *testing.T) {
	store := state.NewMemoryStore()
	var scopeErr, sendErr, cancelErr atomic.Value
	var selfID atomic.Value
	// 内层子执行是一张图：节点在受管 frame 内拿到任务上下文，再去
	// 触碰根 frame 中的自身记录——直属校验按执行树判定。
	inner := graph.NewGraph[string, string]("managed-scope-inner")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	probe := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, _ := selfID.Load().(string)
		_, err := graph.WaitManaged(ctx, []string{id})
		scopeErr.Store(err)
		_, err = graph.SendManagedMessage(ctx, id, "x")
		sendErr.Store(err)
		_, err = graph.CancelManaged(ctx, id)
		cancelErr.Store(err)
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
	builder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			// 等外层节点把任务 ID 写回后再做越权校验。
			for i := 0; i < 200; i++ {
				if v, ok := selfID.Load().(string); ok && v != "" {
					break
				}
				select {
				case <-time.After(5 * time.Millisecond):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			var task string
			_ = json.Unmarshal(input, &task)
			out, err := innerCompiled.Invoke(ctx, task)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}
	g := newManagedStateGraph(t, "managed-scope")
	a := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		in, _ := json.Marshal(input)
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "echo", Version: "v1", Input: in})
		if err != nil {
			return "", err
		}
		selfID.Store(id)
		if _, err := graph.WaitManaged(ctx, []string{"mt/none#0"}); !errors.Is(err, graph.ErrManagedNotFound) {
			t.Fatalf("unknown wait: %v", err)
		}
		return input, nil
	})
	b := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, _ := selfID.Load().(string)
		// 同一 frame 内的另一节点等待该任务合法——直属以执行树为准，
		// 不以创建它的任务槽位为准。
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		if results[0].Status != graph.ManagedStatusCompleted {
			return "", errors.New("cross-node wait should complete")
		}
		return input, nil
	})
	for name, node := range map[string]runnable.Func[string, string]{"a": a, "b": b} {
		if err := g.AddNode(name, node); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range [][2]string{{graph.Start, "a"}, {"a", "b"}, {"b", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "s1"),
		graph.WithManagedBuilder("echo", "v1", builder),
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	for name, slot := range map[string]atomic.Value{"wait": scopeErr, "send": sendErr, "cancel": cancelErr} {
		err, _ := slot.Load().(error)
		if !errors.Is(err, graph.ErrManagedScope) {
			t.Fatalf("inner %s on out-of-frame task: %v", name, err)
		}
	}
}

// TestManagedMessageInbox：消息登记后确认；执行器读取未消费消息并推进
// 消费游标。
func TestManagedMessageInbox(t *testing.T) {
	store := state.NewMemoryStore()
	var inboxSeen atomic.Int32
	builder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			for i := 0; i < 200; i++ {
				msgs, err := graph.ManagedInbox(ctx)
				if err != nil {
					return nil, err
				}
				if len(msgs) > 0 {
					inboxSeen.Add(1)
					if err := graph.AckManagedInbox(ctx, len(msgs)); err != nil {
						return nil, err
					}
					return msgs[0].Payload, nil
				}
				select {
				case <-time.After(10 * time.Millisecond):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return json.Marshal("timeout")
		}, nil
	}
	g := newManagedStateGraph(t, "managed-inbox")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "echo", Version: "v1", Input: json.RawMessage(`"t"`)})
		if err != nil {
			return "", err
		}
		if _, err := graph.SendManagedMessage(ctx, id, "hello"); err != nil {
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
		graph.WithCheckpoint(store, "inbox1"),
		graph.WithManagedBuilder("echo", "v1", builder),
	)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out != `"hello"` {
		t.Fatalf("inbox message not consumed: %q", out)
	}
	if inboxSeen.Load() == 0 {
		t.Fatal("runner never saw inbox message")
	}
}

// TestManagedCancelCascade：显式取消父受管任务级联取消未结束后代，重复
// 取消幂等；任务与后代均落为取消终态。
func TestManagedCancelCascade(t *testing.T) {
	store := state.NewMemoryStore()
	parentRunning := make(chan struct{}, 1)
	// 内层子执行是一张图：节点创建孙任务后阻塞直到被取消。检查点协调
	// 下的嵌套 frame 同样需要 State 工厂才可恢复。
	inner := graph.NewGraph[string, string]("managed-inner")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	leafRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil
	}
	spawnLeaf := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		in, _ := json.Marshal(input)
		if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "leaf", Version: "v1", Input: in}); err != nil {
			return "", err
		}
		select {
		case parentRunning <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return "", ctx.Err()
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
	innerBuilder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var task string
			_ = json.Unmarshal(input, &task)
			out, err := innerCompiled.Invoke(ctx, task)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}

	g := newManagedStateGraph(t, "managed-cancel")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "inner", Version: "v1", Input: json.RawMessage(`"p"`)})
		if err != nil {
			return "", err
		}
		select {
		case <-parentRunning:
		case <-time.After(10 * time.Second):
			return "", errors.New("inner managed task never reached spawn_leaf")
		case <-ctx.Done():
			return "", ctx.Err()
		}
		res, err := graph.CancelManaged(ctx, id)
		if err != nil {
			return "", err
		}
		if res.Status != graph.ManagedStatusCancelled {
			return "", errors.New("cancel did not reach terminal")
		}
		if again, err := graph.CancelManaged(ctx, id); err != nil || again.Status != graph.ManagedStatusCancelled {
			return "", errors.New("repeated cancel not idempotent")
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		if results[0].Status != graph.ManagedStatusCancelled {
			return "", errors.New("wait result should report cancelled")
		}
		return "cancelled", nil
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
		graph.WithCheckpoint(store, "c1"),
		graph.WithManagedBuilder("inner", "v1", innerBuilder),
		graph.WithManagedBuilder("leaf", "v1", leafRunner),
	)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out != "cancelled" {
		t.Fatalf("unexpected output %q", out)
	}
	// 级联断言：检查点中父任务与其后代均为 cancelled。
	record, found, lerr := store.Load(context.Background(), "c1")
	if lerr != nil || !found {
		t.Fatalf("load checkpoint: %v found=%v", lerr, found)
	}
	var tree struct {
		Managed []struct {
			Status string `json:"status"`
			Frame  struct {
				Managed []struct {
					Status string `json:"status"`
				} `json:"managed"`
			} `json:"frame"`
		} `json:"managed"`
	}
	if err := json.Unmarshal(record.Execution, &tree); err != nil {
		t.Fatalf("decode execution: %v", err)
	}
	if len(tree.Managed) != 1 || tree.Managed[0].Status != "cancelled" {
		t.Fatalf("parent managed not cancelled: %+v", tree.Managed)
	}
	if len(tree.Managed[0].Frame.Managed) != 1 || tree.Managed[0].Frame.Managed[0].Status != "cancelled" {
		t.Fatalf("descendant not cancelled: %+v", tree.Managed[0].Frame.Managed)
	}
}

// TestManagedSuspendedTaskInterrupts：受管子执行内部挂起（嵌套图审批）时，
// 根运行在收敛点返回统一挂起结果并携带子任务问题；回答后恢复继续，
// 子任务从自身进度推进，不重答已答问题。
func TestManagedSuspendedTaskInterrupts(t *testing.T) {
	store := state.NewMemoryStore()
	var askSeen atomic.Int32
	sub := graph.NewGraph[string, string]("managed-sub")
	if err := sub.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		askSeen.Add(1)
		answer, err := graph.Interrupt(ctx, "approve sub task")
		if err != nil {
			return "", err
		}
		answerStr, _ := answer.(string)
		return "approved:" + answerStr, nil
	})
	if err := sub.AddNode("ask", ask,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := sub.AddEdge(graph.Start, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := sub.AddEdge("ask", graph.End); err != nil {
		t.Fatal(err)
	}
	subCompiled := mustCompile(t, sub)
	subRunner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var task string
			_ = json.Unmarshal(input, &task)
			out, err := subCompiled.Invoke(ctx, task)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}

	g := newManagedStateGraph(t, "managed-susp")
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		in, _ := json.Marshal(input)
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "sub", Version: "v1", Input: in})
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
	_, err := compiled.Invoke(context.Background(), "audit",
		graph.WithCheckpoint(store, "susp1"),
		graph.WithManagedBuilder("sub", "v1", subRunner),
	)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want InterruptedError, got %v", err)
	}
	pending := interrupted.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("want 1 pending question, got %v", pending)
	}
	out, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "susp1"),
		graph.WithManagedBuilder("sub", "v1", subRunner),
		graph.WithResume(pending[0].ID, "yes"),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(out, "approved:yes") {
		t.Fatalf("unexpected output %q", out)
	}
	// 子图节点从入口重放一次：初执行 + 恢复重放共两次。
	if askSeen.Load() != 2 {
		t.Fatalf("suspended node ran %d times, want 2", askSeen.Load())
	}
}

// TestManagedReplayDeduplicatesSpawn：父任务挂起后重放同一创建调用命中
// 原任务记录——不重复登记，已完成的执行不重跑。
func TestManagedReplayDeduplicatesSpawn(t *testing.T) {
	store := state.NewMemoryStore()
	var builds, runs atomic.Int32
	builder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		builds.Add(1)
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			runs.Add(1)
			return input, nil
		}, nil
	}
	g := newManagedStateGraph(t, "managed-replay")
	spawner := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		in, _ := json.Marshal(input)
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "slow", Version: "v1", Input: in})
		if err != nil {
			return "", err
		}
		// 创建后挂起：恢复重放本节点时 SpawnManaged 必须命中原记录。
		if _, err := graph.Interrupt(ctx, "confirm"); err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := g.AddNode("spawner", spawner,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "spawner"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("spawner", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompile(t, g)
	_, err := compiled.Invoke(context.Background(), "x",
		graph.WithCheckpoint(store, "r1"),
		graph.WithManagedBuilder("slow", "v1", builder),
	)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("want InterruptedError, got %v", err)
	}
	out, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "r1"),
		graph.WithManagedBuilder("slow", "v1", builder),
		graph.WithResume(interrupted.PendingInterrupts()[0].ID, "go"),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != `"x"` {
		t.Fatalf("unexpected output %q", out)
	}
	// 子任务在首次挂起前已完成：恢复重放命中原记录，不再构建或执行。
	if builds.Load() != 1 || runs.Load() != 1 {
		t.Fatalf("builds=%d runs=%d, want 1/1", builds.Load(), runs.Load())
	}
	record, found, lerr := store.Load(context.Background(), "r1")
	if lerr != nil || !found {
		t.Fatalf("load checkpoint: %v found=%v", lerr, found)
	}
	var tree struct {
		Managed []struct {
			ID string `json:"id"`
		} `json:"managed"`
	}
	if err := json.Unmarshal(record.Execution, &tree); err != nil {
		t.Fatalf("decode execution: %v", err)
	}
	if len(tree.Managed) != 1 {
		t.Fatalf("replayed spawn duplicated task records: %+v", tree.Managed)
	}
}

// TestManagedBuilderMissingOnResume：恢复时构建函数未绑定或版本不符即
// 拒绝；正确绑定后从已保存进度继续。
func TestManagedBuilderMissingOnResume(t *testing.T) {
	store := state.NewMemoryStore()
	proceed := make(chan struct{}, 1)
	started := make(chan struct{}, 1)
	inner := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			started <- struct{}{}
			select {
			case <-proceed:
				return input, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, nil
	}
	compiled := mustCompile(t, func() *graph.Graph[string, string] {
		gg := newManagedStateGraph(t, "managed-builder")
		node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
			in, _ := json.Marshal(input)
			id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "inner", Version: "v1", Input: in})
			if err != nil {
				return "", err
			}
			results, err := graph.WaitManaged(ctx, []string{id})
			if err != nil {
				return "", err
			}
			return string(results[0].Result), nil
		})
		if err := gg.AddNode("main", node,
			graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
		); err != nil {
			t.Fatal(err)
		}
		if err := gg.AddEdge(graph.Start, "main"); err != nil {
			t.Fatal(err)
		}
		if err := gg.AddEdge("main", graph.End); err != nil {
			t.Fatal(err)
		}
		return gg
	}())
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(runCtx, "x",
			graph.WithCheckpoint(store, "mb1"),
			graph.WithManagedBuilder("inner", "v1", inner),
		)
		done <- err
	}()
	<-started
	cancel()
	<-done
	if _, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "mb1")); err == nil || !strings.Contains(err.Error(), "builder") {
		t.Fatalf("missing builder resume should fail, got %v", err)
	}
	_, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "mb1"),
		graph.WithManagedBuilder("inner", "v2", inner),
	)
	if err == nil || !strings.Contains(err.Error(), "builder") {
		t.Fatalf("version mismatch resume should fail, got %v", err)
	}
	proceed <- struct{}{}
	out, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "mb1"),
		graph.WithManagedBuilder("inner", "v1", inner),
	)
	if err != nil {
		t.Fatalf("resume with builder: %v", err)
	}
	if !strings.Contains(out, `"x"`) {
		t.Fatalf("unexpected output %q", out)
	}
}

// TestManagedNestedConcurrencyLimit：并发上限为 1 时，父受管任务内部再
// 创建并等待孙任务不得死锁——挂起的任务释放执行额度，恢复时从检查点
// 重新驱动，最终沿调用链收敛。
func TestManagedNestedConcurrencyLimit(t *testing.T) {
	store := state.NewMemoryStore()
	// 孙任务：原样回显。
	leafBuilder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var task string
			_ = json.Unmarshal(input, &task)
			return json.Marshal("leaf:" + task)
		}, nil
	}
	// 父受管任务是一张内层图：节点创建孙任务并等待——等待期间挂起
	// 释放唯一并发额度，孙任务得以执行。
	inner := graph.NewGraph[string, string]("managed-nested-inner")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
		return &managedState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	spawnWait := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		in, _ := json.Marshal(input)
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "leaf", Version: "v1", Input: in})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
	})
	if err := inner.AddNode("spawn_wait", spawnWait,
		graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(graph.Start, "spawn_wait"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("spawn_wait", graph.End); err != nil {
		t.Fatal(err)
	}
	innerCompiled := mustCompile(t, inner)
	innerBuilder := func(_ context.Context, spec graph.ManagedSpec) (graph.ManagedRunner, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var task string
			_ = json.Unmarshal(input, &task)
			out, err := innerCompiled.Invoke(ctx, task)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, nil
	}

	g := newManagedStateGraph(t, "managed-nested")
	if err := g.SetMaxConcurrency(1); err != nil {
		t.Fatal(err)
	}
	main := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		id, err := graph.SpawnManaged(ctx, graph.ManagedSpec{Builder: "outer", Version: "v1", Input: json.RawMessage(`"x"`)})
		if err != nil {
			return "", err
		}
		results, err := graph.WaitManaged(ctx, []string{id})
		if err != nil {
			return "", err
		}
		return string(results[0].Result), nil
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := compiled.Invoke(ctx, "x",
		graph.WithCheckpoint(store, "n1"),
		graph.WithManagedBuilder("outer", "v1", innerBuilder),
		graph.WithManagedBuilder("leaf", "v1", leafBuilder),
	)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !strings.Contains(out, `leaf:x`) {
		t.Fatalf("unexpected output %q", out)
	}
}
