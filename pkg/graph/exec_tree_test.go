package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// childAskGraph 构造一个带提问节点的子图：提问经动态 Interrupt 发出。
func childAskGraph(t *testing.T, question string) *graph.CompiledGraph[string, string] {
	t.Helper()
	child := graph.NewGraph[string, string]("child-ask")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(child.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, question)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(child.AddEdge(graph.Start, "ask"))
	must(child.AddEdge("ask", graph.End))
	return mustCompile(t, child)
}

// execTreeProgress 是测试侧的执行树解码视图。
type execTreeProgress struct {
	GraphName     string                     `json:"graph_name"`
	GraphRevision string                     `json:"graph_revision"`
	ResumeNode    string                     `json:"resume_node"`
	Step          int                        `json:"step"`
	State         graph.GraphStateCheckpoint `json:"state"`
	Tasks         []json.RawMessage          `json:"tasks"`
	Children      []execTreeChild            `json:"children"`
}

type execTreeChild struct {
	TaskID string           `json:"task_id"`
	Seq    int              `json:"seq"`
	Status string           `json:"status"`
	Frame  execTreeProgress `json:"frame"`
}

func loadExecTree(t *testing.T, store state.Store, key string) execTreeProgress {
	t.Helper()
	record, found, err := store.Load(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("load %q: found=%v err=%v", key, found, err)
	}
	var progress execTreeProgress
	if err := json.Unmarshal(record.Execution, &progress); err != nil {
		t.Fatalf("decode execution tree: %v", err)
	}
	return progress
}

// 子图挂起：整棵树以根 InterruptedError 返回，待答列表递归包含子 frame。
func TestExecTreeNestedGraphSuspendsAndResumes(t *testing.T) {
	child := childAskGraph(t, "child question")
	parent := graph.NewGraph[string, string]("parent")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("child", child,
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddEdge(graph.Start, "child"))
	must(parent.AddEdge("child", graph.End))
	compiled := mustCompile(t, parent)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "tree")
	_, err := compiled.Invoke(context.Background(), "root", opt)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("invoke err=%v", err)
	}
	pending := interrupted.PendingInterrupts()
	if len(pending) != 1 || pending[0].Node != "ask" {
		t.Fatalf("pending=%v", pending)
	}
	// PendingInterrupts 从检查点递归读取子 frame 的待答记录。
	saved, err := compiled.PendingInterrupts(context.Background(), opt)
	if err != nil || len(saved) != 1 || saved[0].ID != pending[0].ID {
		t.Fatalf("saved pending=%v err=%v", saved, err)
	}
	tree := loadExecTree(t, store, "tree")
	if tree.GraphName != "parent" || len(tree.Children) != 1 || tree.Children[0].Status != "suspended" || tree.Children[0].Frame.GraphName != "child-ask" {
		t.Fatalf("tree=%+v", tree)
	}
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "approved"))
	if err != nil || out != "approved" {
		t.Fatalf("resume out=%q err=%v", out, err)
	}
}

// 子调用结果复用：父图在子图之后的节点挂起，恢复后已完成的子调用不重放。
func TestExecTreeCompletedChildResultIsReused(t *testing.T) {
	var childCalls atomic.Int32
	child := graph.NewGraph[string, string]("child-done")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(child.AddNode("work", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		childCalls.Add(1)
		return s + ":child", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(child.AddEdge(graph.Start, "work"))
	must(child.AddEdge("work", graph.End))
	compiledChild := mustCompile(t, child)

	parent := graph.NewGraph[string, string]("parent-reuse")
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("child", compiledChild,
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, "confirm")
		if err != nil {
			return "", err
		}
		return s + ":" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(parent.AddEdge(graph.Start, "child"))
	must(parent.AddEdge("child", "ask"))
	must(parent.AddEdge("ask", graph.End))
	compiled := mustCompile(t, parent)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "reuse")
	_, err := compiled.Invoke(context.Background(), "root", opt)
	id := inputID(t, err)
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(id, true))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != "root:child:true" || childCalls.Load() != 1 {
		t.Fatalf("out=%q child calls=%d (result must be reused, not re-executed)", out, childCalls.Load())
	}
}

// 同一任务内两次子调用：任务内序号区分调用，恢复时第一条已完成直接复用。
func TestExecTreeMultipleChildCallsPerTask(t *testing.T) {
	var childRuns atomic.Int32
	child := childAskGraph(t, "pick")
	parent := graph.NewGraph[string, string]("parent-multi")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("double", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		childRuns.Add(1)
		first, err := child.Invoke(ctx, s+":1")
		if err != nil {
			return "", err
		}
		second, err := child.Invoke(ctx, s+":2")
		if err != nil {
			return "", err
		}
		return first + "|" + second, nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(parent.AddEdge(graph.Start, "double"))
	must(parent.AddEdge("double", graph.End))
	compiled := mustCompile(t, parent)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "multi")
	_, err := compiled.Invoke(context.Background(), "root", opt)
	// 第一次子调用提问挂起。
	pending := interruptedPending(t, err)
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	// 答第一问：第二次子调用发出新问题再次挂起。
	_, err = compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "a"))
	pending = interruptedPending(t, err)
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "b"))
	if err != nil || out != "a|b" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if childRuns.Load() != 3 {
		// 三次节点重放：初次(第1问)、恢复(第2问)、恢复(完成)。子图本身
		// 只在第三次真正恢复执行，前两次之间已登记结果直接复用。
		t.Fatalf("node replays=%d", childRuns.Load())
	}
}

// 并行分支中的子调用挂起：整棵树挂起，恢复时兄弟分支已完成结果被复用。
func TestExecTreeForkBranchChildSuspends(t *testing.T) {
	child := childAskGraph(t, "branch approval")
	var aRuns atomic.Int32
	g := graph.NewGraph[string, string]("parent-fork")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*forkState, error) { return &forkState{}, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		for _, r := range rs {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	must(g.AddNode("a", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
		aRuns.Add(1)
		return in, nil
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "a", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("b", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		return child.Invoke(ctx, in)
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "b", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
		return in, nil
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return strings.Join(s.Values, ","), nil })))
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "join"))
	must(g.AddEdge("join", graph.End))
	compiled := mustCompile(t, g)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "fork-tree")
	_, err := compiled.Invoke(context.Background(), "go", opt)
	pending := interruptedPending(t, err)
	if aRuns.Load() != 1 {
		t.Fatalf("a ran %d times", aRuns.Load())
	}
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "yes"))
	if err != nil || out != "a,yes" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("completed branch re-executed: a=%d", aRuns.Load())
	}
}

// 多层嵌套：子图的子图挂起，pending 递归汇集，恢复逐层继续。
func TestExecTreeMultiLevelSuspendAndResume(t *testing.T) {
	grandchild := childAskGraph(t, "deep question")
	child := graph.NewGraph[string, string]("child-mid")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(child.AddNode("inner", grandchild,
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(child.AddEdge(graph.Start, "inner"))
	must(child.AddEdge("inner", graph.End))
	compiledChild := mustCompile(t, child)

	parent := graph.NewGraph[string, string]("root")
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("mid", compiledChild,
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddEdge(graph.Start, "mid"))
	must(parent.AddEdge("mid", graph.End))
	compiled := mustCompile(t, parent)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "deep")
	_, err := compiled.Invoke(context.Background(), "root", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 1 || pending[0].Node != "ask" {
		t.Fatalf("pending=%v", pending)
	}
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "deep-answer"))
	if err != nil || out != "deep-answer" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// 无检查点：嵌套调用中的动态 Interrupt 仍然被拒绝，不产生半截进度。
func TestExecTreeNestedInterruptRequiresCheckpoint(t *testing.T) {
	child := childAskGraph(t, "q")
	parent := graph.NewGraph[string, string]("no-checkpoint")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("child", child,
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddEdge(graph.Start, "child"))
	must(parent.AddEdge("child", graph.End))
	_, err := mustCompile(t, parent).Invoke(context.Background(), "x")
	if !errors.Is(err, graph.ErrInterruptUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

// 旧版单图检查点（无 format/tasks/children 字段）仍然可读可恢复。
func TestExecTreeLegacyFlatCheckpointStillResumes(t *testing.T) {
	var runs atomic.Int32
	g := graph.NewGraph[string, string]("legacy")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		runs.Add(1)
		answer, err := graph.Interrupt(ctx, "question")
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v:done", answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "ask"))
	must(g.AddEdge("ask", graph.End))
	compiled := mustCompile(t, g)

	store := state.NewMemoryStore()
	key := "legacy"
	opt := graph.WithCheckpoint(store, key)
	_, err := compiled.Invoke(context.Background(), "root", opt)
	id := inputID(t, err)
	record, found, err := store.Load(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("load: found=%v err=%v", found, err)
	}
	// 剥掉新格式字段，模拟旧版扁平执行进度。
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(record.Execution, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "format")
	delete(fields, "tasks")
	delete(fields, "children")
	record.Execution, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	record.Status = state.StatusInterrupted
	if _, err = store.Save(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(id, "ok"))
	if err != nil || out != "ok:done" {
		t.Fatalf("legacy resume out=%q err=%v", out, err)
	}
	if runs.Load() != 2 {
		t.Fatalf("node executions=%d", runs.Load())
	}
}

// 畸形执行树被拒绝：重复子调用身份、非法状态、嵌套过深。
func TestExecTreeRejectsMalformedStructures(t *testing.T) {
	store := state.NewMemoryStore()
	g := inputGraph(t, func(_ context.Context, s string) (string, error) { return s, nil })
	opt := graph.WithCheckpoint(store, "bad-tree")
	if _, err := g.Invoke(context.Background(), "x", opt); err != nil {
		t.Fatalf("setup invoke: %v", err)
	}
	record, found, err := store.Load(context.Background(), "bad-tree")
	if err != nil || !found {
		t.Fatal("checkpoint missing")
	}
	var root map[string]json.RawMessage
	if err = json.Unmarshal(record.Execution, &root); err != nil {
		t.Fatal(err)
	}
	child := func(status string, seq int) map[string]any {
		return map[string]any{"task_id": "task-1", "seq": seq, "status": status}
	}
	cases := map[string][]map[string]any{
		"duplicate-key": {child("running", 0), child("running", 0)},
		"bad-status":    {child("bogus", 0)},
		"negative-seq":  {child("running", -1)},
		"missing-task":  {{"seq": 0, "status": "running"}},
	}
	for name, children := range cases {
		t.Run(name, func(t *testing.T) {
			current, found, e := store.Load(context.Background(), "bad-tree")
			if e != nil || !found {
				t.Fatalf("reload: found=%v err=%v", found, e)
			}
			mutated := map[string]json.RawMessage{}
			for k, v := range root {
				mutated[k] = v
			}
			data, e := json.Marshal(children)
			if e != nil {
				t.Fatal(e)
			}
			mutated["children"] = data
			bad, e := json.Marshal(mutated)
			if e != nil {
				t.Fatal(e)
			}
			current.Execution = bad
			if _, e = store.Save(context.Background(), current); e != nil {
				t.Fatal(e)
			}
			if _, e = g.Resume(context.Background(), opt); !errors.Is(e, state.ErrInvalidCheckpoint) {
				t.Fatalf("resume err=%v", e)
			}
		})
	}
	// 超深嵌套：用根 frame 自身的合法字段做模板，递归挂接超过深度上限。
	var template map[string]any
	if err = json.Unmarshal(record.Execution, &template); err != nil {
		t.Fatal(err)
	}
	delete(template, "children")
	delete(template, "tasks")
	deepNode := map[string]any{"task_id": "t", "seq": 0, "status": "running"}
	current := deepNode
	for i := 0; i < 80; i++ {
		inner := map[string]any{"task_id": "t", "seq": 0, "status": "running"}
		f := map[string]any{}
		for k, v := range template {
			f[k] = v
		}
		f["children"] = []map[string]any{inner}
		current["frame"] = f
		current = inner
	}
	data, err := json.Marshal([]map[string]any{deepNode})
	if err != nil {
		t.Fatal(err)
	}
	deep := map[string]json.RawMessage{}
	for k, v := range root {
		deep[k] = v
	}
	deep["children"] = data
	bad, err := json.Marshal(deep)
	if err != nil {
		t.Fatal(err)
	}
	fresh, found, err := store.Load(context.Background(), "bad-tree")
	if err != nil || !found {
		t.Fatalf("reload: found=%v err=%v", found, err)
	}
	fresh.Execution = bad
	if _, err = store.Save(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err = g.Resume(context.Background(), opt); !errors.Is(err, state.ErrInvalidCheckpoint) {
		t.Fatalf("deep tree resume err=%v", err)
	}
}

// 问题1：子图完成后父节点失败，根记录不得被标为 completed；恢复继续父任务。
func TestExecTreeChildCompletionDoesNotMarkRootDone(t *testing.T) {
	var childCalls, failerCalls atomic.Int32
	var failOnce atomic.Bool
	failOnce.Store(true)
	child := graph.NewGraph[string, string]("child-first")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(child.AddNode("work", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		childCalls.Add(1)
		return s + ":child", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(child.AddEdge(graph.Start, "work"))
	must(child.AddEdge("work", graph.End))
	compiledChild := mustCompile(t, child)

	parent := graph.NewGraph[string, string]("parent-after")
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("child", compiledChild,
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddNode("failer", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		failerCalls.Add(1)
		if failOnce.Swap(false) {
			return "", errors.New("parent node failed")
		}
		return s + ":done", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(parent.AddEdge(graph.Start, "child"))
	must(parent.AddEdge("child", "failer"))
	must(parent.AddEdge("failer", graph.End))
	compiled := mustCompile(t, parent)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "parent-fail")
	if _, err := compiled.Invoke(context.Background(), "root", opt); err == nil {
		t.Fatal("expected parent failure")
	}
	record, found, err := store.Load(context.Background(), "parent-fail")
	if err != nil || !found {
		t.Fatal("checkpoint missing")
	}
	if record.Status == state.StatusCompleted || len(record.Result) > 0 {
		t.Fatalf("root marked completed by child: status=%s result=%s", record.Status, record.Result)
	}
	out, err := compiled.Resume(context.Background(), opt)
	if err != nil || out != "root:child:done" {
		t.Fatalf("resume out=%q err=%v", out, err)
	}
	if childCalls.Load() != 1 || failerCalls.Load() != 2 {
		t.Fatalf("child=%d failer=%d (child must be reused, parent node retried)", childCalls.Load(), failerCalls.Load())
	}
}

// childSaveGate 只在写入携带已完成子调用的快照时失败——模拟子结果落盘故障。
type childSaveGate struct {
	state.Store
	armed atomic.Bool
}

func (s *childSaveGate) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	if s.armed.Load() {
		var tree struct {
			Children []struct {
				Status string `json:"status"`
			} `json:"children"`
		}
		if json.Unmarshal(c.Execution, &tree) == nil {
			for _, child := range tree.Children {
				if child.Status == "completed" {
					return state.Checkpoint{}, errors.New("injected child-save failure")
				}
			}
		}
	}
	return s.Store.Save(ctx, c)
}

// 问题2：子结果保存失败是检查点错误而非业务失败；结果在内存中保留，未落盘
// 崩溃时按至少一次语义重放。
func TestExecTreeChildSaveFailureIsCheckpointError(t *testing.T) {
	var childCalls atomic.Int32
	child := graph.NewGraph[string, string]("child-save")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(child.AddNode("work", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		childCalls.Add(1)
		return s + ":child", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(child.AddEdge(graph.Start, "work"))
	must(child.AddEdge("work", graph.End))

	parent := graph.NewGraph[string, string]("parent-save")
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("child", mustCompile(t, child),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddEdge(graph.Start, "child"))
	must(parent.AddEdge("child", graph.End))
	compiled := mustCompile(t, parent)

	gate := &childSaveGate{Store: state.NewMemoryStore()}
	gate.armed.Store(true)
	opt := graph.WithCheckpoint(gate, "save-fail")
	_, err := compiled.Invoke(context.Background(), "root", opt)
	var checkpointErr *graph.CheckpointError
	if !errors.As(err, &checkpointErr) {
		t.Fatalf("expected CheckpointError, got %v", err)
	}
	// 落盘失败时检查点仍指向最后一个成功边界：恢复按至少一次语义重放子图。
	gate.armed.Store(false)
	out, err := compiled.Resume(context.Background(), opt)
	if err != nil || out != "root:child" {
		t.Fatalf("resume out=%q err=%v", out, err)
	}
	if childCalls.Load() != 2 {
		t.Fatalf("child replayed %d times, want at-least-once replay", childCalls.Load())
	}
}

// 问题3+8：固定并行分支拥有独立任务记录与审批现场——已完成分支不重跑，
// 挂起分支按自身日志恢复。
func TestExecTreeParallelBranchKeepsCompletedWork(t *testing.T) {
	var fastRuns, slowRuns atomic.Int32
	g := graph.NewGraph[string, string]("parallel-tree")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(g.AddNode("fast", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		fastRuns.Add(1)
		return s + ":fast", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(g.AddNode("slow", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		slowRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "slow approval")
		if err != nil {
			return "", err
		}
		return s + ":" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(g.AddNode("end", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "fast", "slow"))
	must(g.AddJoin("q", "end"))
	must(g.AddEdge("fast", "end"))
	must(g.AddEdge("slow", "end"))
	must(g.AddEdge("end", graph.End))
	must(graph.SetForkMerger(g, "q", func(_ context.Context, s *inputState, rs []graph.StepResult[*inputState]) error {
		for _, r := range rs {
			s.Text += "|" + r.State.Text
		}
		return nil
	}))
	compiled := mustCompile(t, g)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "parallel-tree")
	_, err := compiled.Invoke(context.Background(), "root", opt)
	pending := interruptedPending(t, err)
	// 扇出组内未答问题归属成员入口节点。
	if len(pending) != 1 || pending[0].Node != "slow" {
		t.Fatalf("pending=%v", pending)
	}
	// 两个分支都有任务记录：fast 已完成，slow 挂起待答。
	tree := loadExecTree(t, store, "parallel-tree")
	status := map[string]string{}
	for _, raw := range tree.Tasks {
		var task struct {
			Branch string `json:"branch"`
			Status string `json:"status"`
		}
		if json.Unmarshal(raw, &task) == nil {
			status[task.Branch] = task.Status
		}
	}
	if status["fast"] != "completed" || status["slow"] != "suspended" {
		t.Fatalf("branch records=%v", status)
	}
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "yes"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(out, ":fast") || !strings.Contains(out, ":yes") {
		t.Fatalf("merged=%q", out)
	}
	if fastRuns.Load() != 1 || slowRuns.Load() != 2 {
		t.Fatalf("fast=%d slow=%d (completed branch must not re-run)", fastRuns.Load(), slowRuns.Load())
	}
}

// 问题4：失败子执行恢复时从最近边界重试，旧错误不作永久缓存。
func TestExecTreeFailedChildRetriesFromBoundary(t *testing.T) {
	var childCalls atomic.Int32
	var failOnce atomic.Bool
	failOnce.Store(true)
	child := graph.NewGraph[string, string]("child-flaky")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(child.AddNode("work", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		childCalls.Add(1)
		if failOnce.Swap(false) {
			return "", errors.New("child transient failure")
		}
		return s + ":child", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(child.AddEdge(graph.Start, "work"))
	must(child.AddEdge("work", graph.End))

	parent := graph.NewGraph[string, string]("parent-retry")
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(parent.AddNode("child", mustCompile(t, child),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddEdge(graph.Start, "child"))
	must(parent.AddEdge("child", graph.End))
	compiled := mustCompile(t, parent)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "retry")
	if _, err := compiled.Invoke(context.Background(), "root", opt); err == nil {
		t.Fatal("expected child failure")
	}
	out, err := compiled.Resume(context.Background(), opt)
	if err != nil || out != "root:child" {
		t.Fatalf("resume out=%q err=%v", out, err)
	}
	if childCalls.Load() != 2 {
		t.Fatalf("child runs=%d (failed child must retry, not reuse stale error)", childCalls.Load())
	}
}

// 问题5+6：并行分支各自提问；只回答 A 时 B 不重跑；无答案 Resume 返回原挂起。
func TestExecTreeForkBranchesWaitIndependently(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	g := graph.NewGraph[string, string]("fork-input")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*forkState, error) { return &forkState{}, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		for _, r := range rs {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	ask := func(name string, counter *atomic.Int32) (runnable.Runnable[string, string], graph.NodeOption) {
		return runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
			counter.Add(1)
			answer, err := graph.Interrupt(ctx, "approve "+name)
			if err != nil {
				return "", err
			}
			return name + ":" + fmt.Sprint(answer), nil
		}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return name, nil })
	}
	aNode, aPre := ask("a", &aRuns)
	bNode, bPre := ask("b", &bRuns)
	must(g.AddNode("a", aNode, aPre, graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("b", bNode, bPre, graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) { return in, nil }),
		graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return strings.Join(s.Values, ","), nil })))
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "join"))
	must(g.AddEdge("join", graph.End))
	compiled := mustCompile(t, g)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "fork-input")
	_, err := compiled.Invoke(context.Background(), "go", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 2 {
		t.Fatalf("pending=%v", pending)
	}
	var aID, bID string
	for _, p := range pending {
		if p.Node == "a" {
			aID = p.ID
		} else {
			bID = p.ID
		}
	}
	// 无答案 Resume：全部任务仍等待，直接返回原挂起结果。
	_, err = compiled.Resume(context.Background(), opt)
	var again *graph.InterruptedError
	if !errors.As(err, &again) || len(again.PendingInterrupts()) != 2 {
		t.Fatalf("empty resume=%v", err)
	}
	// 只回答 A：A 重放完成，B 保持等待且不重跑。
	_, err = compiled.Resume(context.Background(), opt, graph.WithResume(aID, "yes"))
	again = nil
	if !errors.As(err, &again) || len(again.PendingInterrupts()) != 1 || again.PendingInterrupts()[0].ID != bID {
		t.Fatalf("partial resume=%v", err)
	}
	if aRuns.Load() != 2 || bRuns.Load() != 1 {
		t.Fatalf("a=%d b=%d (answered branch replays, unanswered must not)", aRuns.Load(), bRuns.Load())
	}
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(bID, "ok"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(out, "a:yes") || !strings.Contains(out, "b:ok") {
		t.Fatalf("merged=%q", out)
	}
	if aRuns.Load() != 2 || bRuns.Load() != 2 {
		t.Fatalf("a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
}

// 问题7：子图版本变化后已完成子结果不得复用，明确拒绝恢复。
// 父节点先调用子图、再 Interrupt：恢复时节点从入口重放必经子调用点，
// 身份校验在返回保存结果之前执行。
func TestExecTreeRejectsChangedChildRevision(t *testing.T) {
	var childPtr atomic.Pointer[graph.CompiledGraph[string, string]]
	childVersion := func(extra bool) *graph.CompiledGraph[string, string] {
		child := graph.NewGraph[string, string]("child-ver")
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
		must(child.AddNode("work", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s + ":child", nil
		}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
		if extra {
			must(child.AddNode("extra", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
			must(child.AddEdge(graph.Start, "work"))
			must(child.AddEdge("work", "extra"))
			must(child.AddEdge("extra", graph.End))
		} else {
			must(child.AddEdge(graph.Start, "work"))
			must(child.AddEdge("work", graph.End))
		}
		return mustCompile(t, child)
	}
	childPtr.Store(childVersion(false))
	parent := graph.NewGraph[string, string]("parent-ver")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(parent.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	// 父节点体内先调用子图再发起审批：重放时先经子调用身份校验。
	must(parent.AddNode("call", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		out, err := childPtr.Load().Invoke(ctx, s)
		if err != nil {
			return "", err
		}
		answer, err := graph.Interrupt(ctx, "confirm")
		if err != nil {
			return "", err
		}
		return out + ":" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(parent.AddEdge(graph.Start, "call"))
	must(parent.AddEdge("call", graph.End))
	compiled := mustCompile(t, parent)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "versioned")
	_, err := compiled.Invoke(context.Background(), "root", opt)
	id := inputID(t, err)
	// 子图结构变化：重放到调用点时身份校验必须先于结果复用并拒绝。
	childPtr.Store(childVersion(true))
	_, err = compiled.Resume(context.Background(), opt, graph.WithResume(id, "yes"))
	if !errors.Is(err, graph.ErrResumeIncompatible) {
		t.Fatalf("resume err=%v", err)
	}
}

// armedSaveGate 在布防期间拒绝一切保存，模拟挂起后的存储故障。
type armedSaveGate struct {
	state.Store
	armed atomic.Bool
}

func (s *armedSaveGate) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	if s.armed.Load() {
		return state.Checkpoint{}, errors.New("injected save failure")
	}
	return s.Store.Save(ctx, c)
}

// 问题1（复查三）：挂起快照必须包含同批已完成兄弟的任务记录；即使随后
// 的保存被注入故障，恢复时已完成分支仍不重跑。
func TestExecTreeSuspendRetainsSiblingCompletion(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	g := graph.NewGraph[string, string]("suspend-retain")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*forkState, error) { return &forkState{}, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		for _, r := range rs {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	must(g.AddNode("a", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		aRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "approve a")
		if err != nil {
			return "", err
		}
		return "a:" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "a", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("b", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
		bRuns.Add(1)
		return in, nil
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "b", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) { return in, nil }),
		graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return strings.Join(s.Values, ","), nil })))
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "join"))
	must(g.AddEdge("join", graph.End))
	compiled := mustCompile(t, g)

	gate := &armedSaveGate{Store: state.NewMemoryStore()}
	opt := graph.WithCheckpoint(gate, "suspend-retain")
	_, err := compiled.Invoke(context.Background(), "go", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	// 挂起快照里兄弟任务 b 的完成记录必须存在（否则恢复会重跑 b）。
	tree := loadExecTree(t, gate.Store, "suspend-retain")
	foundB := false
	for _, raw := range tree.Tasks {
		var task struct {
			Branch string `json:"branch"`
			Status string `json:"status"`
		}
		if json.Unmarshal(raw, &task) == nil && task.Branch == "b" {
			foundB = task.Status == "completed"
		}
	}
	if !foundB {
		t.Fatalf("suspended snapshot lost completed sibling: %+v", tree.Tasks)
	}
	// 保存故障期：恢复以 CheckpointError 返回，不落半成品。
	gate.armed.Store(true)
	_, err = compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "yes"))
	var checkpointErr *graph.CheckpointError
	if !errors.As(err, &checkpointErr) {
		t.Fatalf("resume err=%v, want CheckpointError", err)
	}
	gate.armed.Store(false)
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "yes"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(out, "a:yes") || !strings.Contains(out, "b") {
		t.Fatalf("merged=%q", out)
	}
	if bRuns.Load() != 1 {
		t.Fatalf("completed sibling re-executed: b=%d", bRuns.Load())
	}
}

// 问题3（复查三）：挂起任务的等待位置有明确记录。A 已答完本层问题、正阻塞
// 在子调用上时，回答兄弟 B 的问题不得唤醒 A 重新执行。
func TestExecTreeStaleAnswerDoesNotWakeBlockedBranch(t *testing.T) {
	child := childAskGraph(t, "deep approval")
	var aRuns, bRuns atomic.Int32
	g := graph.NewGraph[string, string]("stale-answer")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*forkState, error) { return &forkState{}, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		for _, r := range rs {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	must(g.AddNode("a", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		aRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "approve a")
		if err != nil {
			return "", err
		}
		out, err := child.Invoke(ctx, in+":child")
		if err != nil {
			return "", err
		}
		return "a:" + fmt.Sprint(answer) + ":" + out, nil
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "a", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("b", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		bRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "approve b")
		if err != nil {
			return "", err
		}
		return "b:" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "b", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) { return in, nil }),
		graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return strings.Join(s.Values, ","), nil })))
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "join"))
	must(g.AddEdge("join", graph.End))
	compiled := mustCompile(t, g)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "stale-answer")
	_, err := compiled.Invoke(context.Background(), "go", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 2 {
		t.Fatalf("pending=%v", pending)
	}
	var aID, bID string
	for _, p := range pending {
		if p.Node == "a" {
			aID = p.ID
		} else if p.Node == "b" {
			bID = p.ID
		}
	}
	if aID == "" || bID == "" {
		t.Fatalf("pending ids missing: %v", pending)
	}
	// 回答 A：A 重放后继续调用子图，停在子图的新问题上。
	_, err = compiled.Resume(context.Background(), opt, graph.WithResume(aID, "yes"))
	pending = interruptedPending(t, err)
	if aRuns.Load() != 2 {
		t.Fatalf("a replayed %d times", aRuns.Load())
	}
	var childID string
	for _, p := range pending {
		if p.ID != bID {
			childID = p.ID
		}
	}
	if childID == "" || len(pending) != 2 {
		t.Fatalf("pending after child suspend=%v", pending)
	}
	// 只回答 B：A 仍阻塞在子调用上，不得因旧答案已答而重跑。
	_, err = compiled.Resume(context.Background(), opt, graph.WithResume(bID, "ok"))
	pending = interruptedPending(t, err)
	if aRuns.Load() != 2 {
		t.Fatalf("stale answer woke blocked branch: a=%d", aRuns.Load())
	}
	if len(pending) != 1 || pending[0].ID != childID {
		t.Fatalf("pending=%v, want only child question", pending)
	}
	// 回答子图问题：A 重放（本层答案直接重放），子调用恢复并完成。
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(childID, "deep"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(out, "a:yes:deep") || !strings.Contains(out, "b:ok") {
		t.Fatalf("merged=%q", out)
	}
	if aRuns.Load() != 3 || bRuns.Load() != 2 {
		t.Fatalf("a=%d b=%d", aRuns.Load(), bRuns.Load())
	}
}

// 问题4（复查三）：无可推进工作时，无答案 Resume 不重复保存同一检查点。
func TestExecTreeEmptyResumeDoesNotResave(t *testing.T) {
	g := graph.NewGraph[string, string]("no-resave")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*forkState, error) { return &forkState{}, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		for _, r := range rs {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	ask := func(name string) (runnable.Runnable[string, string], graph.NodeOption) {
		return runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
			answer, err := graph.Interrupt(ctx, "approve "+name)
			if err != nil {
				return "", err
			}
			return name + ":" + fmt.Sprint(answer), nil
		}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return name, nil })
	}
	aNode, aPre := ask("a")
	bNode, bPre := ask("b")
	post := func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil }
	must(g.AddNode("a", aNode, aPre, graph.WithPostHandler(post)))
	must(g.AddNode("b", bNode, bPre, graph.WithPostHandler(post)))
	must(g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) { return in, nil }),
		graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return strings.Join(s.Values, ","), nil })))
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "join"))
	must(g.AddEdge("join", graph.End))
	compiled := mustCompile(t, g)

	store := &byteCheckpointStore{}
	opt := graph.WithCheckpoint(store, "no-resave")
	_, err := compiled.Invoke(context.Background(), "go", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 2 {
		t.Fatalf("pending=%v", pending)
	}
	record, found, err := store.Load(context.Background(), "no-resave")
	if err != nil || !found {
		t.Fatal("checkpoint missing")
	}
	_, saves := store.counts()
	// 连续两次无答案 Resume：返回同一组待答问题，不新增保存、版本不变。
	for i := 0; i < 2; i++ {
		_, err = compiled.Resume(context.Background(), opt)
		var again *graph.InterruptedError
		if !errors.As(err, &again) {
			t.Fatalf("empty resume %d err=%v", i, err)
		}
		got := again.PendingInterrupts()
		if len(got) != len(pending) || got[0].ID != pending[0].ID || got[1].ID != pending[1].ID {
			t.Fatalf("empty resume %d pending=%v, want %v", i, got, pending)
		}
		_, after := store.counts()
		if after != saves {
			t.Fatalf("empty resume %d saved a checkpoint: %d -> %d", i, saves, after)
		}
	}
	current, _, err := store.Load(context.Background(), "no-resave")
	if err != nil || current.Version != record.Version {
		t.Fatalf("version changed: %v -> %v", record.Version, current.Version)
	}
}

// 问题4（复查三）：全部任务完成但合并未提交的检查点，无答案恢复仍能完成合并。
func TestExecTreeAllCompletedResumeStillMerges(t *testing.T) {
	var aRuns, bRuns, merges atomic.Int32
	var mergeFail atomic.Bool
	mergeFail.Store(true)
	g := graph.NewGraph[string, string]("unmerged")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*forkState, error) { return &forkState{}, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		merges.Add(1)
		n := len(base.Values)
		for _, r := range rs {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		if len(rs) > 1 && mergeFail.Swap(false) {
			return errors.New("merge failed once")
		}
		return nil
	}))
	complete := func(name string, counter *atomic.Int32) (runnable.Runnable[string, string], graph.NodeOption, graph.NodeOption) {
		return runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
				counter.Add(1)
				return in, nil
			}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return name, nil }),
			graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })
	}
	aNode, aPre, aPost := complete("a", &aRuns)
	bNode, bPre, bPost := complete("b", &bRuns)
	must(g.AddNode("a", aNode, aPre, aPost))
	must(g.AddNode("b", bNode, bPre, bPost))
	must(g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) { return in, nil }),
		graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return strings.Join(s.Values, ","), nil })))
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "join"))
	must(g.AddEdge("join", graph.End))
	compiled := mustCompile(t, g)

	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "unmerged")
	if _, err := compiled.Invoke(context.Background(), "go", opt); err == nil {
		t.Fatal("expected merge failure")
	}
	// 任务已全部完成但合并未提交：无答案恢复直接继续合并与提交。
	out, err := compiled.Resume(context.Background(), opt)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(out, "a") || !strings.Contains(out, "b") {
		t.Fatalf("merged=%q", out)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Fatalf("a=%d b=%d (completed tasks must be reused)", aRuns.Load(), bRuns.Load())
	}
	// 合并共三次：失败一次 + 恢复重试一次 + join 步合并一次。
	if merges.Load() != 3 {
		t.Fatalf("merges=%d", merges.Load())
	}
}

// 问题2（复查四）：外层任务阻塞在固定并行子图上时，并行节点的调度任务 ID
// 与分支记录 ID（task-N/branch）的对应关系必须被识别——只回答兄弟 B 或
// 不答时，外层 A 不得重跑，slow 的问题 ID 不变；重复保存也随判断修正消失。
func TestExecTreeParallelSubgraphDoesNotWakeWaitingParent(t *testing.T) {
	var aRuns, bRuns, fastRuns, slowRuns atomic.Int32
	// 内层子图：固定并行节点 fast 完成、slow 审批。
	inner := graph.NewGraph[string, string]("inner-parallel")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(inner.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
	must(inner.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(inner.AddNode("fast", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		fastRuns.Add(1)
		return s + ":fast", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(inner.AddNode("slow", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		slowRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "slow approval")
		if err != nil {
			return "", err
		}
		return s + ":" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })))
	must(inner.AddNode("end", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(inner.AddEdge(graph.Start, "q"))
	must(inner.AddFork("q", "fast", "slow"))
	must(inner.AddJoin("q", "end"))
	must(inner.AddEdge("fast", "end"))
	must(inner.AddEdge("slow", "end"))
	must(inner.AddEdge("end", graph.End))
	must(graph.SetForkMerger(inner, "q", func(_ context.Context, s *inputState, rs []graph.StepResult[*inputState]) error {
		for _, r := range rs {
			s.Text += "|" + r.State.Text
		}
		return nil
	}))
	compiledInner := mustCompile(t, inner)

	// 外层 fork：A 调用内层固定并行子图，B 独立审批。
	g := graph.NewGraph[string, string]("outer-fork")
	must(g.SetStateFactory(func(_ context.Context, _ string) (*forkState, error) { return &forkState{}, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		for _, r := range rs {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	must(g.AddNode("a", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		aRuns.Add(1)
		return compiledInner.Invoke(ctx, in)
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "a", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("b", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		bRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "approve b")
		if err != nil {
			return "", err
		}
		return "b:" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return "b", nil }),
		graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	must(g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) { return in, nil }),
		graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) { return strings.Join(s.Values, ","), nil })))
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "join"))
	must(g.AddEdge("join", graph.End))
	compiled := mustCompile(t, g)

	store := &byteCheckpointStore{}
	opt := graph.WithCheckpoint(store, "nested-parallel")
	_, err := compiled.Invoke(context.Background(), "go", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 2 {
		t.Fatalf("pending=%v", pending)
	}
	var bID, slowID string
	for _, p := range pending {
		if p.Node == "b" {
			bID = p.ID
		} else {
			slowID = p.ID
		}
	}
	if bID == "" || slowID == "" {
		t.Fatalf("pending=%v", pending)
	}
	// 只回答 B：A 的子图内 slow 仍在等待，A 不得重跑。
	_, err = compiled.Resume(context.Background(), opt, graph.WithResume(bID, "ok"))
	pending = interruptedPending(t, err)
	if aRuns.Load() != 1 {
		t.Fatalf("outer branch A re-ran while its parallel child waits: a=%d", aRuns.Load())
	}
	if len(pending) != 1 || pending[0].ID != slowID {
		t.Fatalf("pending=%v, want only slow question", pending)
	}
	// 不带答案 Resume：无可推进工作，不再产生新的保存。
	_, saves := store.counts()
	_, err = compiled.Resume(context.Background(), opt)
	var again *graph.InterruptedError
	if !errors.As(err, &again) || len(again.PendingInterrupts()) != 1 || again.PendingInterrupts()[0].ID != slowID {
		t.Fatalf("empty resume=%v", err)
	}
	_, after := store.counts()
	if after != saves {
		t.Fatalf("empty resume saved checkpoint: %d -> %d", saves, after)
	}
	// 回答 slow：A 才继续；fast 的完成结果直接复用。
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(slowID, "yes"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(out, ":fast") || !strings.Contains(out, ":yes") || !strings.Contains(out, "b:ok") {
		t.Fatalf("merged=%q", out)
	}
	if aRuns.Load() != 2 || bRuns.Load() != 2 || fastRuns.Load() != 1 || slowRuns.Load() != 2 {
		t.Fatalf("a=%d b=%d fast=%d slow=%d", aRuns.Load(), bRuns.Load(), fastRuns.Load(), slowRuns.Load())
	}
}

func interruptedPending(t *testing.T, err error) []control.InterruptRecord {
	t.Helper()
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("expected saved interruption, got %v", err)
	}
	pending := interrupted.PendingInterrupts()
	if len(pending) == 0 {
		t.Fatal("missing pending records")
	}
	return pending
}
