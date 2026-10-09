package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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

type forkState struct{ Values []string }

// forkGraph 构造 start→{a,b}，a→join、b→c→join 的扇出图。b 失败前可经
// gate 等待外部条件（如兄弟任务的完成记录落盘），消除并发时序差异。
func forkGraph(t *testing.T, fail *bool, calls map[string]int, mu *sync.Mutex, gate func() bool) *graph.Graph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("fork")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(context.Context, string) (*forkState, error) { return &forkState{}, nil }))
	// 每轮只追加各任务相对于入口新增的内容。
	must(g.SetStepMerger(func(_ context.Context, base *forkState, results []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		for _, r := range results {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	for _, name := range []string{"a", "b", "c", "join"} {
		n := name
		must(g.AddNode(n, runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			mu.Lock()
			calls[n]++
			mu.Unlock()
			if n == "b" && *fail {
				// 释放 mu 之后再等待 gate：兄弟任务仍在持锁计数，否则会死锁。
				if gate != nil {
					deadline := time.Now().Add(10 * time.Second)
					for !gate() {
						if time.Now().After(deadline) {
							return "", errors.New("fork gate timeout")
						}
						time.Sleep(time.Millisecond)
					}
				}
				return "", errors.New("branch failed")
			}
			return input, nil
		}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) {
			if n == "join" {
				return "joined", nil
			}
			return n, nil
		}), graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	}
	must(g.AddFork(graph.Start, "a", "b"))
	must(g.AddJoin(graph.Start, "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "c"))
	must(g.AddEdge("c", "join"))
	must(g.AddEdge("join", graph.End))
	return g
}

type stepFixture struct {
	base      *forkState
	mu        sync.Mutex
	calls     map[string]int
	entries   map[string][][]string
	groups    []string
	failNode  string
	mergeFail bool
	routeFail bool
	rounds    [][]string
	nodeHook  func(context.Context, string) error
}

func newStepFixture(t *testing.T, loop bool) (*graph.Graph[string, string], *stepFixture) {
	t.Helper()
	f := &stepFixture{calls: map[string]int{}, entries: map[string][][]string{}}
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	g := graph.NewGraph[string, string]("rounds")
	must(g.SetStateFactory(func(context.Context, string) (*forkState, error) { f.base = &forkState{}; return f.base, nil }))
	must(g.SetStepMerger(func(_ context.Context, base *forkState, rs []graph.StepResult[*forkState]) error {
		n := len(base.Values)
		var nodes []string
		for _, r := range rs {
			nodes = append(nodes, r.NodeID)
			base.Values = append(base.Values, r.State.Values[n:]...)
			if r.GroupID != "" {
				f.groups = append(f.groups, r.GroupID)
			}
		}
		f.rounds = append(f.rounds, nodes)
		if f.mergeFail {
			return errors.New("merge failed")
		}
		return nil
	}))
	for _, id := range []string{"fork", "a", "b", "c", "join"} {
		name := id
		must(g.AddNode(name, runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
			f.mu.Lock()
			f.calls[name]++
			f.mu.Unlock()
			if f.nodeHook != nil {
				if e := f.nodeHook(ctx, name); e != nil {
					return "", e
				}
			}
			if f.failNode == name {
				return "", errors.New("node failed")
			}
			return in, nil
		}), graph.WithPreHandler(func(_ context.Context, s *forkState) (string, error) {
			f.mu.Lock()
			f.entries[name] = append(f.entries[name], append([]string(nil), s.Values...))
			f.mu.Unlock()
			return name, nil
		}), graph.WithPostHandler(func(_ context.Context, s *forkState, out string) error { s.Values = append(s.Values, out); return nil })))
	}
	must(g.AddEdge(graph.Start, "fork"))
	must(g.AddFork("fork", "a", "b"))
	must(g.AddJoin("fork", "join"))
	must(g.AddEdge("a", "join"))
	must(g.AddEdge("b", "c"))
	must(g.AddEdge("c", "join"))
	must(g.AddEdge("join", graph.End))
	must(g.AddBranch("b", "c", "route-check", func(context.Context, string) (bool, error) {
		if f.routeFail {
			return false, errors.New("route failed")
		}
		return false, nil
	}))
	if loop {
		must(g.AddBranch("join", "fork", "repeat", func(_ context.Context, s *forkState) (bool, error) { return len(s.Values) < 10, nil }))
	}
	return g, f
}

func readStep(t *testing.T, store state.Store, key string) (checkpointProgress, forkState) {
	t.Helper()
	record, found, e := store.Load(context.Background(), key)
	if e != nil || !found {
		t.Fatalf("load: %v %v", found, e)
	}
	c, e := decodeProgress(record.Execution)
	if e != nil {
		t.Fatal(e)
	}
	var s forkState
	if e = json.Unmarshal(c.State.Data, &s); e != nil {
		t.Fatal(e)
	}
	return c, s
}

func TestNativeStepIsolationOrderAndRepeatedFork(t *testing.T) {
	g, f := newStepFixture(t, true)
	bFinished := make(chan struct{}, 2)
	f.nodeHook = func(ctx context.Context, name string) error {
		if name == "b" {
			bFinished <- struct{}{}
		}
		if name == "a" {
			select {
			case <-bFinished:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	c := mustCompile(t, g)
	store := state.NewMemoryStore()
	out, e := c.Invoke(context.Background(), "", graph.WithCheckpoint(store, "loop"))
	if e != nil || out != "join" {
		t.Fatalf("%q %v", out, e)
	}
	want := []string{"fork", "a", "b", "c", "join", "fork", "a", "b", "c", "join"}
	if !reflect.DeepEqual(f.base.Values, want) {
		t.Fatalf("State order: %v", f.base.Values)
	}
	if !reflect.DeepEqual(f.entries["a"], f.entries["b"]) {
		t.Fatalf("same-round inputs differ: %v / %v", f.entries["a"], f.entries["b"])
	}
	// c 接续的是成员 b 自己的局部 State（a 的中间态不进入分支），
	// 与合并前主干隔离。
	if !reflect.DeepEqual(f.entries["c"][0], []string{"fork", "b"}) {
		t.Fatalf("next-round state: %v", f.entries["c"])
	}
	if f.calls["join"] != 2 {
		t.Fatalf("join calls %d", f.calls["join"])
	}
	// 合并只在全部成员到达后执行一次：每次扇出产生两条成员终态记录，
	// 两次扇出共四条，组实例按轮次区分。
	if len(f.groups) != 4 || f.groups[0] != f.groups[1] || f.groups[0] == f.groups[2] {
		t.Fatalf("group instances: %v", f.groups)
	}
	saved, _ := readStep(t, store, "loop")
	if saved.Step != 10 || saved.Schedule.Round != 8 {
		t.Fatalf("counters: %+v", saved)
	}
	before := len(f.rounds)
	if _, e = c.Resume(context.Background(), graph.WithCheckpoint(store, "loop")); e != nil || len(f.rounds) != before {
		t.Fatalf("completed run executed again: %v", e)
	}
}

func TestNativeStepFailureBoundaries(t *testing.T) {
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		for _, kind := range []string{"node", "merge", "route", "checkpoint"} {
			t.Run(string(mode)+"/"+kind, func(t *testing.T) {
				g, f := newStepFixture(t, false)
				store := &byteCheckpointStore{}
				if kind == "node" {
					f.failNode = "c"
				}
				if kind == "route" {
					f.routeFail = true
				}
				if kind == "merge" {
					f.nodeHook = func(_ context.Context, n string) error {
						if n == "fork" {
							f.mergeFail = true
						}
						return nil
					}
				}
				if kind == "checkpoint" {
					store.saveErr = errors.New("save failed")
					store.failAfter = 2
				}
				c := mustCompile(t, g)
				opts := []runnable.Option{graph.WithCheckpoint(store, "failure"), graph.WithDurability(mode)}
				if _, e := c.Invoke(context.Background(), "", opts...); e == nil {
					t.Fatal("expected failure")
				}
				saved, s := readStep(t, store, "failure")
				expected := []string{"fork"}
				expectedStep := 1
				if kind == "merge" {
					expected = nil
					expectedStep = 0
				}
				if kind == "node" {
					// 成员中间态不合并进主干：c 失败时提交的主干只有 fork，
					// a/b 的终态保存在组内成员快照中。
					expected = []string{"fork"}
					expectedStep = 3
				}
				if !reflect.DeepEqual(s.Values, expected) || saved.Step != expectedStep {
					t.Fatalf("partial commit: %v step=%d", s.Values, saved.Step)
				}
				if mode == graph.DurabilitySync && !reflect.DeepEqual(f.base.Values, expected) {
					t.Fatalf("failed round changed live State: %v", f.base.Values)
				}
				if kind == "node" {
					if !reflect.DeepEqual(saved.Schedule.Group.Arrived, []string{"a"}) || len(saved.Schedule.Tasks) != 1 || saved.Schedule.Tasks[0].Node != "c" {
						t.Fatalf("lost join waiting state: %+v", saved.Schedule)
					}
				}
				f.failNode = ""
				f.mergeFail = false
				f.routeFail = false
				f.nodeHook = nil
				store.saveErr = nil
				aBefore := f.calls["a"]
				if _, e := c.Resume(context.Background(), opts...); e != nil {
					t.Fatal(e)
				}
				if kind == "node" && f.calls["a"] != aBefore {
					t.Fatal("already-arrived branch replayed")
				}
				_, s = readStep(t, store, "failure")
				if !reflect.DeepEqual(s.Values, []string{"fork", "a", "b", "c", "join"}) {
					t.Fatalf("resume State: %v", s.Values)
				}
			})
		}
	}
}

func TestNativeStepBudgetAndCancellation(t *testing.T) {
	t.Run("whole-round-budget", func(t *testing.T) {
		g, f := newStepFixture(t, false)
		if e := g.SetMaxSteps(2); e != nil {
			t.Fatal(e)
		}
		_, e := mustCompile(t, g).Invoke(context.Background(), "")
		if e == nil || f.calls["a"]+f.calls["b"] != 0 {
			t.Fatalf("partial round started: %v %v", e, f.calls)
		}
	})
	t.Run("queued-cancel", func(t *testing.T) {
		g, f := newStepFixture(t, false)
		if e := g.SetMaxConcurrency(1); e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.nodeHook = func(ctx context.Context, n string) error {
			if n == "a" {
				cancel()
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		_, e := mustCompile(t, g).Invoke(ctx, "")
		if !errors.Is(e, context.Canceled) || f.calls["b"] != 0 || f.calls["join"] != 0 {
			t.Fatalf("cancel: %v calls=%v", e, f.calls)
		}
	})
	t.Run("limit-and-isolation", func(t *testing.T) {
		g, f := newStepFixture(t, false)
		if e := g.SetMaxConcurrency(1); e != nil {
			t.Fatal(e)
		}
		var active, peak atomic.Int32
		f.nodeHook = func(_ context.Context, n string) error {
			v := active.Add(1)
			if v > peak.Load() {
				peak.Store(v)
			}
			defer active.Add(-1)
			time.Sleep(time.Millisecond)
			return nil
		}
		if _, e := mustCompile(t, g).Invoke(context.Background(), ""); e != nil {
			t.Fatal(e)
		}
		if peak.Load() != 1 || !reflect.DeepEqual(f.entries["a"], f.entries["b"]) {
			t.Fatal("concurrency limit broke isolation")
		}
	})
}

func TestNativeStepInterruptAndUnsupportedInput(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		t.Run(map[bool]string{false: "pause", true: "input"}[dynamic], func(t *testing.T) {
			g, f := newStepFixture(t, false)
			store := state.NewMemoryStore()
			f.nodeHook = func(ctx context.Context, n string) error {
				if n != "c" {
					return nil
				}
				if dynamic {
					_, e := graph.Interrupt(ctx, "approve")
					return e
				}
				return control.RequestInterrupt("pause", nil)
			}
			c := mustCompile(t, g)
			opt := graph.WithCheckpoint(store, "pause")
			_, e := c.Invoke(context.Background(), "", opt)
			var interrupted *graph.InterruptedError
			if !errors.As(e, &interrupted) {
				t.Fatalf("dynamic=%v %v", dynamic, e)
			}
			saved, _ := readStep(t, store, "pause")
			if saved.Step != 3 || saved.Schedule.Tasks[0].Node != "c" {
				t.Fatalf("wrong boundary: %+v", saved)
			}
			if dynamic {
				// 任务级审批现场：分支任务内的 Interrupt 挂起后按问题 ID 恢复。
				pending := interrupted.PendingInterrupts()
				if len(pending) != 1 || pending[0].Node != "c" {
					t.Fatalf("pending=%v", pending)
				}
				if _, e = c.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, true)); e != nil {
					t.Fatal(e)
				}
				if f.calls["c"] != 2 {
					t.Fatalf("replayed c=%d", f.calls["c"])
				}
				return
			}
			f.nodeHook = nil
			if _, e = c.Resume(context.Background(), opt); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestNativeForkCompileRejectsUnsupportedStructures(t *testing.T) {
	for _, kind := range []string{"cycle", "cross", "bypass", "nested", "extra-edge", "outside-join", "direct-join"} {
		t.Run(kind, func(t *testing.T) {
			g, _ := newStepFixture(t, false)
			var e error
			switch kind {
			case "cycle":
				e = g.AddBranch("c", "b", "cycle", func(context.Context, string) (bool, error) { return false, nil })
			case "cross":
				e = g.AddBranch("a", "c", "cross", func(context.Context, string) (bool, error) { return false, nil })
			case "bypass":
				e = g.AddBranch("b", graph.End, "bypass", func(context.Context, string) (bool, error) { return false, nil })
			case "nested":
				e = g.AddFork("b", "a", "c")
			case "extra-edge":
				e = g.AddBranch("fork", "join", "extra", func(context.Context, string) (bool, error) { return false, nil })
			case "outside-join":
				e = g.AddBranch(graph.Start, "join", "outside", func(context.Context, string) (bool, error) { return false, nil })
			case "direct-join":
				g = graph.NewGraph[string, string]("invalid")
				_ = g.AddFork(graph.Start, "a", "join")
				_ = g.AddJoin(graph.Start, "join")
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, e = g.Compile(); e == nil || !strings.Contains(e.Error(), "fork") {
				t.Fatalf("expected fork validation, got %v", e)
			}
		})
	}
}

func TestNativeJoinStillSupportsHumanInput(t *testing.T) {
	g, f := newStepFixture(t, false)
	f.nodeHook = func(ctx context.Context, n string) error {
		if n == "join" {
			_, e := graph.Interrupt(ctx, "approve joined result")
			return e
		}
		return nil
	}
	c := mustCompile(t, g)
	store := state.NewMemoryStore()
	opt := graph.WithCheckpoint(store, "input")
	_, e := c.Invoke(context.Background(), "", opt)
	if !errors.Is(e, graph.ErrInterrupted) {
		t.Fatalf("%v", e)
	}
	saved, _ := readStep(t, store, "input")
	if saved.Schedule.Group != nil || len(saved.Interrupts) != 1 {
		t.Fatalf("join boundary: %+v", saved)
	}
	if _, e = c.Resume(context.Background(), opt, graph.WithResume(saved.Interrupts[0].ID, "approved")); e != nil {
		t.Fatal(e)
	}
	if f.calls["a"] != 1 || f.calls["b"] != 1 || f.calls["join"] != 2 {
		t.Fatalf("unexpected replay: %v", f.calls)
	}
}

func TestNativeResumeRejectsBrokenSchedule(t *testing.T) {
	for _, kind := range []string{"old-execution", "missing-branch", "wrong-node", "wrong-group"} {
		t.Run(kind, func(t *testing.T) {
			g, f := newStepFixture(t, false)
			f.failNode = "c"
			c := mustCompile(t, g)
			store := state.NewMemoryStore()
			opt := graph.WithCheckpoint(store, "broken")
			if _, e := c.Invoke(context.Background(), "", opt); e == nil {
				t.Fatal("expected branch failure")
			}
			record, _, e := store.Load(context.Background(), "broken")
			if e != nil {
				t.Fatal(e)
			}
			saved, e := decodeProgress(record.Execution)
			if e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "old-execution":
				// 将旧执行对象包装进新检查点也应拒绝。
			case "missing-branch":
				saved.Schedule.Group.Arrived = nil
			case "wrong-node":
				saved.Schedule.Tasks[0].Node = "a"
				saved.ResumeNode = "a"
			case "wrong-group":
				saved.Schedule.Tasks[0].GroupID = "different"
			}
			record.Execution, e = json.Marshal(saved)
			if e != nil {
				t.Fatal(e)
			}
			if kind == "old-execution" {
				var fields map[string]json.RawMessage
				if e = json.Unmarshal(record.Execution, &fields); e != nil {
					t.Fatal(e)
				}
				fields["schema_version"] = json.RawMessage("4")
				record.Execution, e = json.Marshal(fields)
				if e != nil {
					t.Fatal(e)
				}
			}
			if _, e = store.Save(context.Background(), record); e != nil {
				t.Fatal(e)
			}
			before := f.calls["c"]
			if _, e = c.Resume(context.Background(), opt); !errors.Is(e, state.ErrInvalidCheckpoint) {
				t.Fatalf("%v", e)
			}
			if before != f.calls["c"] {
				t.Fatal("invalid schedule ran a task")
			}
		})
	}
}

func TestStepMergerWaitsForStreamSuccess(t *testing.T) {
	g := graph.NewGraph[int, int]("stream-transaction")
	base := &forkState{}
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	must(g.SetStateFactory(func(context.Context, int) (*forkState, error) { return base, nil }))
	merges := 0
	must(g.SetStepMerger(func(_ context.Context, s *forkState, rs []graph.StepResult[*forkState]) error {
		merges++
		s.Values = rs[0].State.Values
		return nil
	}))
	must(g.AddNode("stream", errorStreamSource{err: errors.New("stream failed")}, graph.WithPreHandler(func(_ context.Context, s *forkState) (int, error) {
		s.Values = append(s.Values, "uncommitted")
		return 1, nil
	})))
	must(g.AddEdge(graph.Start, "stream"))
	must(g.AddEdge("stream", graph.End))
	reader, e := mustCompile(t, g).Stream(context.Background(), 0)
	must(e)
	if _, e = reader.Collect(context.Background()); e == nil {
		t.Fatal("expected stream failure")
	}
	if merges != 0 || len(base.Values) != 0 {
		t.Fatalf("stream failure committed: merges=%d State=%v", merges, base.Values)
	}
}

func TestNativeForkJoinAndRoundRecovery(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "invoke", true: "stream"}[stream], func(t *testing.T) {
			fail := true
			calls := map[string]int{}
			mu := &sync.Mutex{}
			var store *state.MemoryStore
			// b 只在 a 的完成记录真正落盘后才失败——并发下 a 未必先提交，若不在
			// 此等待，断言"a 不重跑"会依赖 goroutine 调度时序（偶发 a:2）。
			g := forkGraph(t, &fail, calls, mu, func() bool {
				if store == nil {
					return false
				}
				record, _, err := store.Load(context.Background(), "fork")
				if err != nil {
					return false
				}
				saved, err := decodeProgress(record.Execution)
				if err != nil {
					return false
				}
				for _, task := range saved.Tasks {
					if task.Node == "a" && task.Status == "completed" {
						return true
					}
				}
				return false
			})
			c := mustCompile(t, g)
			store = state.NewMemoryStore()
			opts := []runnable.Option{graph.WithCheckpoint(store, "fork"), graph.WithDurability(graph.DurabilitySync)}
			_, err := c.Invoke(context.Background(), "", opts...)
			if err == nil {
				t.Fatal("expected failure")
			}
			record, _, err := store.Load(context.Background(), "fork")
			if err != nil {
				t.Fatal(err)
			}
			saved, err := decodeProgress(record.Execution)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Step != 0 {
				t.Fatalf("failed round committed: %d", saved.Step)
			}
			beforeA := calls["a"]
			beforeB := calls["b"]
			fail = false
			var out string
			if stream {
				r, e := c.ResumeStream(context.Background(), opts...)
				if e != nil {
					t.Fatal(e)
				}
				out, err = r.Collect(context.Background())
			} else {
				out, err = c.Resume(context.Background(), opts...)
			}
			if err != nil || out != "joined" {
				t.Fatalf("%q %v", out, err)
			}
			// 已完成任务的结果保存在执行树上，恢复时直接复用，不再重复执行；
			// 失败任务按领取时的入口快照重放。
			if calls["a"] != beforeA || calls["b"] != beforeB+1 || calls["join"] != 1 || calls["c"] != 1 {
				t.Fatalf("calls: %v", calls)
			}
			record, _, err = store.Load(context.Background(), "fork")
			if err != nil {
				t.Fatal(err)
			}
			saved, err = decodeProgress(record.Execution)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Step != 4 {
				t.Fatalf("task count = %d", saved.Step)
			}
		})
	}
}
