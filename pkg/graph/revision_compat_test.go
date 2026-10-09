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

// 整图指纹是唯一的兼容性判据：恢复点上游新增节点同样使指纹变化，
// 检查点不再区分闭包内外，一律拒绝恢复。
func TestResumeRejectsChangesOutsideResumeClosure(t *testing.T) {
	counters := map[string]*atomic.Int32{"a": {}, "pre": {}}
	store, questionID, _ := revisionTestSuspend(t, revisionTestGraph(t, false, false, nil, counters))

	v2 := revisionTestGraph(t, true, false, nil, counters)
	compiled, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume after upstream insert = %v, want ErrGraphRevisionMismatch", err)
	}
}

// 恢复点之后插入后继节点同样被拒绝。
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

// 篡改的图名同样被拒绝：指纹之外的身份字段仍逐项核对。
func TestResumeRejectsDifferentGraphName(t *testing.T) {
	counters := map[string]*atomic.Int32{"a": {}}
	store, questionID, _ := revisionTestSuspend(t, revisionTestGraph(t, false, false, nil, counters))
	record, found, err := store.Load(context.Background(), "rev")
	if err != nil || !found {
		t.Fatalf("load: %v %v", found, err)
	}
	stripped := map[string]any{}
	if err := json.Unmarshal(record.Execution, &stripped); err != nil {
		t.Fatal(err)
	}
	stripped["graph_name"] = "other-graph"
	raw, err := json.Marshal(stripped)
	if err != nil {
		t.Fatal(err)
	}
	record.Execution = raw
	if _, err := store.Save(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	v2 := revisionTestGraph(t, false, false, nil, counters)
	compiled, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume with renamed graph = %v, want ErrGraphRevisionMismatch", err)
	}
}

// Codec 升级使整图指纹变化：同名图、同拓扑下仅 State 编解码演进
// 也拒绝恢复，不再提供迁移通道。
func TestResumeRejectsCodecUpgrade(t *testing.T) {
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
	store, questionID, _ := revisionTestSuspend(t, revisionTestGraph(t, false, false, codecV1, counters))

	v2 := revisionTestGraph(t, false, false, codecV2, counters)
	compiled, err := v2.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Resume(context.Background(), graph.WithCheckpoint(store, "rev"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume after codec upgrade = %v, want ErrGraphRevisionMismatch", err)
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

// 嵌套子图指纹独立校验：子图升级自身 Codec 后子 frame 的整图指纹与
// 当前编译不一致，父图恢复按 ErrResumeIncompatible 拒绝——父子各有
// 自己的指纹边界，父图结构与 Codec 不变也不影响判定。
func TestNestedChildRejectsChangedFingerprint(t *testing.T) {
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
		return childCall.Invoke(ctx, in)
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil })))
	must(parent.AddEdge(graph.Start, "call"))
	must(parent.AddEdge("call", graph.End))
	compiledParent := mustCompile(t, parent)

	childCall = buildChild(1)
	store := &byteCheckpointStore{}
	opt := graph.WithCheckpoint(store, "nested-fp")
	_, err := compiledParent.Invoke(context.Background(), "in", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	tree := loadExecTree(t, store, "nested-fp")
	if len(tree.Children) != 1 || tree.Children[0].Key == "" {
		t.Fatalf("child record key missing: %+v", tree.Children)
	}
	childTree := loadExecTree(t, store, tree.Children[0].Key)
	if childTree.GraphRevision == "" {
		t.Fatalf("child record missing revision: %+v", childTree)
	}
	childOldRev := childTree.GraphRevision

	// 子图升级自身 Codec v1→v2：父图结构、父图 Codec 均不变，但子记录
	// 指纹与当前子图编译不一致，父图带答案恢复被拒。
	childCall = buildChild(2)
	_, err = compiledParent.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "yes"))
	if !errors.Is(err, graph.ErrResumeIncompatible) {
		t.Fatalf("resume after child codec change = %v, want ErrResumeIncompatible", err)
	}
	// 父子记录原样保留：子记录指纹未变，可再次以旧版子图恢复。
	savedTree := loadExecTree(t, store, "nested-fp")
	if len(savedTree.Children) != 1 || savedTree.Children[0].Key == "" {
		t.Fatalf("child key mutated: %+v", savedTree.Children)
	}
	savedChild := loadExecTree(t, store, savedTree.Children[0].Key)
	if savedChild.GraphRevision != childOldRev {
		t.Fatalf("child record mutated: %+v", savedChild)
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
