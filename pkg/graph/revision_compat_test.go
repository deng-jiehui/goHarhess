package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

// revisionTestGraph 按版本变量构造同一张顺序图：可选的上游节点 pre
// 与下游节点 notify 用于制造恢复闭包内外的拓扑差异。
func revisionTestGraph(t *testing.T, extraPre, extraNotify bool, codec graph.StateCodec, counters map[string]*atomic.Int32) *graph.Graph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("revtest")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, input string) (*checkpointTestState, error) {
		return &checkpointTestState{Value: input}, nil
	}))
	if codec != nil {
		must(g.SetStateCodec(codec))
	}
	node := func(name string, fn func(string) string) {
		must(g.AddNode(name, runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			if counter := counters[name]; counter != nil {
				counter.Add(1)
			}
			return fn(input), nil
		}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
			graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil })))
	}
	if extraPre {
		node("pre", func(s string) string { return s + "-pre" })
	}
	node("a", func(s string) string { return s + "-a" })
	must(g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, "approve")
		if err != nil {
			return "", err
		}
		return input + "-" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
		graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil })))
	if extraNotify {
		node("notify", func(s string) string { return s + "-notify" })
	}
	node("b", func(s string) string { return s + "-b" })
	if extraPre {
		must(g.AddEdge(graph.Start, "pre"))
		must(g.AddEdge("pre", "a"))
	} else {
		must(g.AddEdge(graph.Start, "a"))
	}
	must(g.AddEdge("a", "ask"))
	if extraNotify {
		must(g.AddEdge("ask", "notify"))
		must(g.AddEdge("notify", "b"))
	} else {
		must(g.AddEdge("ask", "b"))
	}
	must(g.AddEdge("b", graph.End))
	return g
}

// revisionTestSuspend 编译并运行图至 ask 挂起，返回存储、问题 ID 与旧 revision。
func revisionTestSuspend(t *testing.T, g *graph.Graph[string, string]) (*byteCheckpointStore, string, string) {
	t.Helper()
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	store := &byteCheckpointStore{}
	_, err = compiled.Invoke(context.Background(), "in", graph.WithCheckpoint(store, "rev"))
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) || len(interrupted.PendingInterrupts()) != 1 {
		t.Fatalf("invoke = %v, want one pending interrupt", err)
	}
	record, found, loadErr := store.Load(context.Background(), "rev")
	if loadErr != nil || !found {
		t.Fatalf("load: %v %v", found, loadErr)
	}
	progress := inspectCheckpoint(t, record)
	return store, progress.Interrupts[0].ID, progress.GraphRevision
}

// 恢复闭包之外的拓扑改动（恢复点上游新增节点）不再使检查点失效。
func TestResumeAllowsChangesOutsideResumeClosure(t *testing.T) {
	counters := map[string]*atomic.Int32{"a": {}, "pre": {}}
	store, questionID, _ := revisionTestSuspend(t, revisionTestGraph(t, false, false, nil, counters))

	v2 := revisionTestGraph(t, true, false, nil, counters)
	compiled, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"))
	if err != nil {
		t.Fatalf("resume after upstream insert: %v", err)
	}
	if out != "in-a-yes-b" {
		t.Fatalf("out=%q", out)
	}
	if counters["a"].Load() != 1 || counters["pre"].Load() != 0 {
		t.Fatalf("a=%d pre=%d, upstream nodes must not re-execute", counters["a"].Load(), counters["pre"].Load())
	}
}

// 恢复闭包内的结构变化（恢复点后插入后继节点）仍被拒绝。
func TestResumeRejectsChangesInsideResumeClosure(t *testing.T) {
	counters := map[string]*atomic.Int32{"a": {}}
	store, questionID, _ := revisionTestSuspend(t, revisionTestGraph(t, false, false, nil, counters))

	v2 := revisionTestGraph(t, false, true, nil, counters)
	compiled, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume = %v, want ErrGraphRevisionMismatch", err)
	}
}

// 无契约条目的旧进度仍按整图 revision 严格比较。
func TestResumeWithoutContractStillRequiresExactRevision(t *testing.T) {
	counters := map[string]*atomic.Int32{"a": {}}
	store, questionID, _ := revisionTestSuspend(t, revisionTestGraph(t, false, false, nil, counters))
	record, found, err := store.Load(context.Background(), "rev")
	if err != nil || !found {
		t.Fatalf("load: %v %v", found, err)
	}
	// 剥掉契约字段模拟旧格式进度：上游插入同样应被拒绝。
	stripped := map[string]any{}
	if err := json.Unmarshal(record.Execution, &stripped); err != nil {
		t.Fatal(err)
	}
	delete(stripped, "contract")
	raw, err := json.Marshal(stripped)
	if err != nil {
		t.Fatal(err)
	}
	record.Execution = raw
	if _, err := store.Save(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	v2 := revisionTestGraph(t, true, false, nil, counters)
	compiled, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume without contract = %v, want ErrGraphRevisionMismatch", err)
	}
}

// WithMigration 把旧版本编码头表示的 State 改写为新版本格式后继续恢复；
// 未声明迁移或声明的来源版本不匹配时仍被拒绝。
func TestResumeMigrationRewritesStateCodec(t *testing.T) {
	codecV1, err := graph.NewStateCodec[*checkpointTestState]("test-state", 1,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) { return json.Marshal(s) },
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			var s checkpointTestState
			return &s, json.Unmarshal(data, &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	codecV2, err := graph.NewStateCodec[*checkpointTestState]("test-state", 2,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) {
			data, err := json.Marshal(s)
			return append([]byte("v2|"), data...), err
		},
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			text, ok := strings.CutPrefix(string(data), "v2|")
			if !ok {
				return nil, errors.New("not a v2 payload")
			}
			var s checkpointTestState
			return &s, json.Unmarshal([]byte(text), &s)
		})
	if err != nil {
		t.Fatal(err)
	}

	counters := map[string]*atomic.Int32{"a": {}}
	store, questionID, oldRev := revisionTestSuspend(t, revisionTestGraph(t, false, false, codecV1, counters))

	// 同一张图，仅 State 编解码升级：未迁移时拒绝。
	v2 := revisionTestGraph(t, false, false, codecV2, counters)
	compiled, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume without migration = %v, want ErrGraphRevisionMismatch", err)
	}
	// 声明的来源版本不匹配，迁移不生效。
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"),
		graph.WithMigration("deadbeef", func(old graph.GraphStateCheckpoint) (graph.GraphStateCheckpoint, error) {
			return old, nil
		}))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume with wrong migration source = %v, want ErrGraphRevisionMismatch", err)
	}
	// 匹配来源版本：v1 字节改写为 v2 格式后继续恢复。
	migrated := false
	out, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"),
		graph.WithMigration(oldRev, func(old graph.GraphStateCheckpoint) (graph.GraphStateCheckpoint, error) {
			migrated = true
			return graph.GraphStateCheckpoint{
				CodecID: old.CodecID, Version: 2,
				Data: append([]byte("v2|"), old.Data...),
			}, nil
		}))
	if err != nil {
		t.Fatalf("resume with migration: %v", err)
	}
	if !migrated {
		t.Fatal("migration function was not invoked")
	}
	if out != "in-a-yes-b" {
		t.Fatalf("out=%q", out)
	}
	if counters["a"].Load() != 1 {
		t.Fatalf("a re-ran: %d", counters["a"].Load())
	}
}

// sidePathGraph 构造 ask 节点存在额外下游路径的图：variant="cond" 时
// ask 有通往 notify→End 的条件分支；variant="end" 时 b 同时直连 End 与
// side→End；variant="" 只有直线 ask→b→End。用于复现"删除路径但未重新
// 接线"时旧校验漏检的问题。
func sidePathGraph(t *testing.T, variant string, bFails *atomic.Bool) *graph.Graph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("sidepath")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, input string) (*checkpointTestState, error) {
		return &checkpointTestState{Value: input}, nil
	}))
	node := func(name string) {
		must(g.AddNode(name, runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			if name == "b" && bFails != nil && bFails.Swap(false) {
				return "", errors.New("b flaky failure")
			}
			return input + "-" + name, nil
		}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
			graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil })))
	}
	node("a")
	must(g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, "approve")
		if err != nil {
			return "", err
		}
		return input + "-" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
		graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil })))
	node("b")
	must(g.AddEdge(graph.Start, "a"))
	must(g.AddEdge("a", "ask"))
	must(g.AddEdge("ask", "b"))
	must(g.AddEdge("b", graph.End))
	if variant == "cond" {
		// 审批后可能走的侧路：条件边经 notify 到 End。
		node("notify")
		must(graph.AddBranch(g, "ask", "notify", "notify", func(_ context.Context, v string) (bool, error) { return v == "", nil }))
		must(g.AddEdge("notify", graph.End))
	}
	return g
}

func suspendAt(t *testing.T, g *graph.Graph[string, string], key string) (*byteCheckpointStore, string, string) {
	t.Helper()
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	store := &byteCheckpointStore{}
	_, err = compiled.Invoke(context.Background(), "in", graph.WithCheckpoint(store, key))
	pending := interruptedPending(t, err)
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	record, found, loadErr := store.Load(context.Background(), key)
	if loadErr != nil || !found {
		t.Fatalf("load: %v %v", found, loadErr)
	}
	progress := inspectCheckpoint(t, record)
	return store, pending[0].ID, progress.GraphRevision
}

// P1：删除恢复点之后的路径必须拒绝恢复——旧图中以恢复节点为起点的完整
// 出边集合都参与比较，通往已删除节点或 End 的边不能被忽略。
func TestResumeRejectsDeletedDownstreamPath(t *testing.T) {
	// v1：审批后除直线 ask→b→End 外还有条件分支 ask→notify→End；
	// v2 删除该分支且不重新接线。旧校验因 notify 不在新图闭包内而漏检。
	store, questionID, _ := suspendAt(t, sidePathGraph(t, "cond", nil), "side")
	compiled, err := sidePathGraph(t, "", nil).Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "side"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume after deleting downstream path = %v, want ErrGraphRevisionMismatch", err)
	}
}

// P2：恢复现场的全部 State 快照（frame.State、任务 Entry/Local）都要经
// 迁移函数改写。A 已完成、B 待审批时升级 Codec：A 的结果继续复用，B 从
// 迁移后的入口快照重放。
func TestResumeMigrationCoversTaskSnapshots(t *testing.T) {
	codecV1, err := graph.NewStateCodec[*forkState]("fork-state", 1,
		func(_ context.Context, s *forkState) ([]byte, error) { return json.Marshal(s) },
		func(_ context.Context, data []byte) (*forkState, error) {
			var s forkState
			return &s, json.Unmarshal(data, &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	codecV2, err := graph.NewStateCodec[*forkState]("fork-state", 2,
		func(_ context.Context, s *forkState) ([]byte, error) {
			data, err := json.Marshal(s)
			return append([]byte("v2|"), data...), err
		},
		func(_ context.Context, data []byte) (*forkState, error) {
			text, ok := strings.CutPrefix(string(data), "v2|")
			if !ok {
				return nil, errors.New("not a v2 payload")
			}
			var s forkState
			return &s, json.Unmarshal([]byte(text), &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	var aRuns, bRuns atomic.Int32
	build := func(codec graph.StateCodec) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("migfork")
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
		must(g.SetStateCodec(codec))
		must(g.AddNode("a", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
			aRuns.Add(1)
			return "a", nil
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
		return g
	}

	v1 := build(codecV1)
	compiled, err := v1.Compile()
	if err != nil {
		t.Fatal(err)
	}
	store := &byteCheckpointStore{}
	_, err = compiled.Invoke(context.Background(), "go", graph.WithCheckpoint(store, "migfork"))
	pending := interruptedPending(t, err)
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	record, found, err := store.Load(context.Background(), "migfork")
	if err != nil || !found {
		t.Fatalf("load: %v %v", found, err)
	}
	oldRev := inspectCheckpoint(t, record).GraphRevision

	v2 := build(codecV2)
	compiled2, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opt := graph.WithCheckpoint(store, "migfork")
	out, err := compiled2.Resume(context.Background(), opt,
		graph.WithResume(pending[0].ID, "yes"),
		graph.WithMigration(oldRev, func(old graph.GraphStateCheckpoint) (graph.GraphStateCheckpoint, error) {
			return graph.GraphStateCheckpoint{
				CodecID: old.CodecID, Version: 2,
				Data: append([]byte("v2|"), old.Data...),
			}, nil
		}))
	if err != nil {
		t.Fatalf("resume with migration over task snapshots: %v", err)
	}
	if !strings.Contains(out, "a") || !strings.Contains(out, "b:yes") {
		t.Fatalf("merged=%q", out)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("completed branch re-executed: a=%d", aRuns.Load())
	}
}

// P1：迁移并回答后节点暂时失败，再次直接 Resume 必须读取已提交的新版
// 现场——回答保存时的图版本与已迁移 State 同属一份快照，不能重复执行旧版迁移。
func TestResumeMigrationPersistsNewIdentity(t *testing.T) {
	codecV1, err := graph.NewStateCodec[*checkpointTestState]("test-state", 1,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) { return json.Marshal(s) },
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			var s checkpointTestState
			return &s, json.Unmarshal(data, &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	codecV2, err := graph.NewStateCodec[*checkpointTestState]("test-state", 2,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) {
			data, err := json.Marshal(s)
			return append([]byte("v2|"), data...), err
		},
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			text, ok := strings.CutPrefix(string(data), "v2|")
			if !ok {
				return nil, errors.New("not a v2 payload")
			}
			var s checkpointTestState
			return &s, json.Unmarshal([]byte(text), &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	// 回答后的 ask 重放暂时失败一次：回答保存与节点提交之间的快照
	// 必须已经携带新版本标记，否则再次恢复会对 v2 数据重复迁移。
	var askFails atomic.Bool
	askFails.Store(true)
	build := func(codec graph.StateCodec) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("flaky-ask")
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(g.SetStateFactory(func(_ context.Context, input string) (*checkpointTestState, error) {
			return &checkpointTestState{Value: input}, nil
		}))
		must(g.SetStateCodec(codec))
		must(g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
			answer, err := graph.Interrupt(ctx, "approve")
			if err != nil {
				return "", err
			}
			if askFails.Swap(false) {
				return "", errors.New("ask flaky failure")
			}
			return input + "-" + fmt.Sprint(answer), nil
		}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
			graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil })))
		must(g.AddEdge(graph.Start, "ask"))
		must(g.AddEdge("ask", graph.End))
		return g
	}
	store, questionID, oldRev := suspendAt(t, build(codecV1), "flaky")

	compiled2, err := build(codecV2).Compile()
	if err != nil {
		t.Fatal(err)
	}
	var migrations atomic.Int32
	opt := graph.WithCheckpoint(store, "flaky")
	migration := graph.WithMigration(oldRev, func(old graph.GraphStateCheckpoint) (graph.GraphStateCheckpoint, error) {
		migrations.Add(1)
		return graph.GraphStateCheckpoint{
			CodecID: old.CodecID, Version: 2,
			Data: append([]byte("v2|"), old.Data...),
		}, nil
	})
	// 第一次恢复：迁移+回答，ask 重放暂时失败，现场停在回答保存的快照上。
	_, err = compiled2.Resume(context.Background(), opt, graph.WithResume(questionID, "yes"), migration)
	if err == nil || !strings.Contains(err.Error(), "flaky") {
		t.Fatalf("first resume = %v, want transient business failure", err)
	}
	// 第二次恢复：不带答案直接续跑，已提交现场必须是新版本——再进入
	// 旧版迁移会把 v2 数据二次前缀，迁移与解码都会失败。
	out, err := compiled2.Resume(context.Background(), opt, migration)
	if err != nil {
		t.Fatalf("second resume after transient failure: %v", err)
	}
	if !strings.HasSuffix(out, "-yes") {
		t.Fatalf("out=%q", out)
	}
	if migrations.Load() != 1 {
		t.Fatalf("migration ran %d times, want exactly once", migrations.Load())
	}
}

// 迁移函数返回当前 Codec 无法解码的快照：整个准备结果拒绝发布，
// 旧检查点与待答问题保持不变。
func TestResumeMigrationRejectsUndecodableSnapshot(t *testing.T) {
	codecV1, err := graph.NewStateCodec[*checkpointTestState]("test-state", 1,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) { return json.Marshal(s) },
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			var s checkpointTestState
			return &s, json.Unmarshal(data, &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	codecV2, err := graph.NewStateCodec[*checkpointTestState]("test-state", 2,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) {
			data, err := json.Marshal(s)
			return append([]byte("v2|"), data...), err
		},
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			text, ok := strings.CutPrefix(string(data), "v2|")
			if !ok {
				return nil, errors.New("not a v2 payload")
			}
			var s checkpointTestState
			return &s, json.Unmarshal([]byte(text), &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	store, questionID, oldRev := suspendAt(t, func() *graph.Graph[string, string] {
		g := sidePathGraph(t, "", nil)
		if err := g.SetStateCodec(codecV1); err != nil {
			t.Fatal(err)
		}
		return g
	}(), "badmig")

	v2 := sidePathGraph(t, "", nil)
	if err := v2.SetStateCodec(codecV2); err != nil {
		t.Fatal(err)
	}
	compiled2, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opt := graph.WithCheckpoint(store, "badmig")
	// 迁移函数报错：拒绝恢复。
	_, err = compiled2.Resume(context.Background(), opt, graph.WithResume(questionID, "yes"),
		graph.WithMigration(oldRev, func(graph.GraphStateCheckpoint) (graph.GraphStateCheckpoint, error) {
			return graph.GraphStateCheckpoint{}, errors.New("migration blew up")
		}))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume with failing migration = %v, want ErrGraphRevisionMismatch", err)
	}
	// 迁移结果无法由当前 Codec 解码：同样拒绝。
	_, err = compiled2.Resume(context.Background(), opt, graph.WithResume(questionID, "yes"),
		graph.WithMigration(oldRev, func(old graph.GraphStateCheckpoint) (graph.GraphStateCheckpoint, error) {
			return graph.GraphStateCheckpoint{CodecID: old.CodecID, Version: 2, Data: []byte("not-json")}, nil
		}))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume with undecodable migration = %v, want ErrGraphRevisionMismatch", err)
	}
	// 旧检查点原样保留：版本标记、State 版本与问题 ID 均未变。
	record, found, err := store.Load(context.Background(), "badmig")
	if err != nil || !found {
		t.Fatalf("load: %v %v", found, err)
	}
	saved := inspectCheckpoint(t, record)
	if saved.GraphRevision != oldRev || saved.State.Version != 1 || strings.HasPrefix(string(saved.State.Data), "v2|") {
		t.Fatalf("checkpoint mutated: rev=%q codec=%s/%d", saved.GraphRevision, saved.State.CodecID, saved.State.Version)
	}
	if len(saved.Interrupts) != 1 || saved.Interrupts[0].ID != questionID || saved.Interrupts[0].Answered {
		t.Fatalf("pending question lost: %+v", saved.Interrupts)
	}
}

// 嵌套子图使用自己的迁移规则：子图升级自身 Codec，父图 Codec 保持原版本；
// 子执行的新版 State 与新版本标记同一份落盘，父图其余分支不受影响。
func TestNestedChildMigrationKeepsOwnCodecBoundary(t *testing.T) {
	childCodec := func(version int) graph.StateCodec {
		encode := func(_ context.Context, s *inputState) ([]byte, error) { return json.Marshal(s) }
		decode := func(_ context.Context, data []byte) (*inputState, error) {
			var s inputState
			return &s, json.Unmarshal(data, &s)
		}
		if version == 2 {
			encode = func(_ context.Context, s *inputState) ([]byte, error) {
				data, err := json.Marshal(s)
				return append([]byte("v2|"), data...), err
			}
			decode = func(_ context.Context, data []byte) (*inputState, error) {
				text, ok := strings.CutPrefix(string(data), "v2|")
				if !ok {
					return nil, errors.New("not a v2 payload")
				}
				var s inputState
				return &s, json.Unmarshal([]byte(text), &s)
			}
		}
		codec, err := graph.NewStateCodec[*inputState]("child-state", version, encode, decode)
		if err != nil {
			t.Fatal(err)
		}
		return codec
	}
	buildChild := func(version int) *graph.CompiledGraph[string, string] {
		child := graph.NewGraph[string, string]("child-ask")
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(child.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }))
		must(child.SetStateCodec(childCodec(version)))
		must(child.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
			first, err := graph.Interrupt(ctx, "child approval")
			if err != nil {
				return "", err
			}
			second, err := graph.Interrupt(ctx, "child confirm")
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%v/%v", first, second), nil
		}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
		must(child.AddEdge(graph.Start, "ask"))
		must(child.AddEdge("ask", graph.End))
		return mustCompile(t, child)
	}

	var childCall *graph.CompiledGraph[string, string]
	var childOpts []runnable.Option
	parent := graph.NewGraph[string, string]("parent")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(parent.SetStateFactory(func(_ context.Context, in string) (*checkpointTestState, error) {
		return &checkpointTestState{Value: in}, nil
	}))
	must(parent.AddNode("call", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		return childCall.Invoke(ctx, in, childOpts...)
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil })))
	must(parent.AddEdge(graph.Start, "call"))
	must(parent.AddEdge("call", graph.End))
	compiledParent := mustCompile(t, parent)

	childCall = buildChild(1)
	store := &byteCheckpointStore{}
	opt := graph.WithCheckpoint(store, "nested-mig")
	_, err := compiledParent.Invoke(context.Background(), "in", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	tree := loadExecTree(t, store, "nested-mig")
	if len(tree.Children) != 1 || tree.Children[0].Frame.GraphRevision == "" {
		t.Fatalf("child frame missing: %+v", tree.Children)
	}
	childOldRev := tree.Children[0].Frame.GraphRevision

	// 子图升级自身 Codec v1→v2；父图结构、父图 Codec 均不变。回答第一个
	// 问题后子图在第二个问题上再次挂起——此刻的子 frame 仍在树中。
	childCall = buildChild(2)
	childOpts = []runnable.Option{graph.WithMigration(childOldRev, func(old graph.GraphStateCheckpoint) (graph.GraphStateCheckpoint, error) {
		return graph.GraphStateCheckpoint{
			CodecID: old.CodecID, Version: 2,
			Data: append([]byte("v2|"), old.Data...),
		}, nil
	})}
	firstID := pending[0].ID
	_, err = compiledParent.Resume(context.Background(), opt, graph.WithResume(firstID, "yes"))
	pending = interruptedPending(t, err)
	if len(pending) != 1 || pending[0].ID == firstID {
		t.Fatalf("pending=%v, want the child's second question", pending)
	}
	// 落盘的子 frame：State 已是 v2 编码、版本标记与当前子图一致。
	tree = loadExecTree(t, store, "nested-mig")
	if len(tree.Children) != 1 {
		t.Fatalf("children=%v", tree.Children)
	}
	frame := tree.Children[0].Frame
	if frame.State.CodecID != "child-state" || frame.State.Version != 2 || !strings.HasPrefix(string(frame.State.Data), "v2|") {
		t.Fatalf("child frame state not migrated: %+v", frame.State)
	}
	if frame.GraphRevision == childOldRev {
		t.Fatal("child frame kept old revision after migration")
	}
	// 回答第二个问题：父图汇合完成。
	out, err := compiledParent.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "confirmed"))
	if err != nil {
		t.Fatalf("final resume: %v", err)
	}
	if !strings.Contains(out, "yes/confirmed") {
		t.Fatalf("out=%q", out)
	}
}

// 1.13.1：Compile 只作用于克隆副本，Builder 在编译后仍可修改并再次编译。
func TestBuilderStaysMutableAfterCompile(t *testing.T) {
	counters := map[string]*atomic.Int32{"a": {}}
	g := revisionTestGraph(t, false, false, nil, counters)
	if _, err := g.Compile(); err != nil {
		t.Fatal(err)
	}
	// 编译后的每个修改接口都必须可用；built 只标记内部运行时快照。
	codec, err := graph.NewStateCodec[*checkpointTestState]("test-state", 1,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) { return json.Marshal(s) },
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			var s checkpointTestState
			return &s, json.Unmarshal(data, &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.SetStateCodec(codec); err != nil {
		t.Fatalf("SetStateCodec after compile: %v", err)
	}
	if err := g.AddNode("extra", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input + "-extra", nil
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
		graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil })); err != nil {
		t.Fatalf("AddNode after compile: %v", err)
	}
	// b 已有无条件出边到 End，新增出边用条件边，避免破坏既有拓扑。
	if err := graph.AddBranch(g, "b", "extra", "extra", func(_ context.Context, value string) (bool, error) {
		return value != "", nil
	}); err != nil {
		t.Fatalf("AddBranch after compile: %v", err)
	}
	if err := g.AddEdge("extra", graph.End); err != nil {
		t.Fatalf("AddEdge after compile: %v", err)
	}
	if err := g.SetMaxConcurrency(4); err != nil {
		t.Fatalf("SetMaxConcurrency after compile: %v", err)
	}
	if err := g.SetMaxSteps(64); err != nil {
		t.Fatalf("SetMaxSteps after compile: %v", err)
	}
	if err := g.SetStepMerger(func(_ context.Context, _ *checkpointTestState, _ []graph.StepResult[*checkpointTestState]) error {
		return nil
	}); err != nil {
		t.Fatalf("SetStepMerger after compile: %v", err)
	}
	v2, err := g.Compile()
	if err != nil {
		t.Fatalf("recompile: %v", err)
	}
	found := false
	for _, node := range v2.Snapshot().Nodes {
		if node.ID == "extra" {
			found = true
		}
	}
	if !found {
		t.Fatal("recompiled graph does not contain node added after first compile")
	}
}
