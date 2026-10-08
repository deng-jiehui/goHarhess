package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// 阶段 4：并行组编译期展开——分支末端接续连线后仍回到组汇合。
func TestParallelBranchContinuationEdges(t *testing.T) {
	type stateT struct {
		Text   string `json:"text"`
		Extras int    `json:"extras"`
	}
	g := graph.NewGraph[string, string]("expand")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s + ":a", nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil }},
		{Name: "b", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s + ":b", nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil }},
	}, graph.WithStateMerger(func(_ context.Context, s *stateT, branches []*stateT) error {
		var parts []string
		for _, b := range branches {
			parts = append(parts, b.Text)
		}
		s.Text = strings.Join(parts, "|")
		s.Extras += len(branches)
		return nil
	})))
	// 分支 a 增加接续节点；分支 b 直连汇合。
	must(g.AddNode("extra", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s + ":extra", nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, out string) error {
			s.Text = out
			s.Extras++
			return nil
		})))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "extra"))
	must(g.AddEdge("extra", "fan"))
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) {
			return fmt.Sprintf("%s#%d", s.Text, s.Extras), nil
		})))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	out, err := mustCompile(t, g).Invoke(context.Background(), "base")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	// 声明序合并：a 走 extra，b 直连；组出口到 sink。
	if out != "base:a:extra|base:b#2" {
		t.Fatalf("out=%q", out)
	}
}

// 分支接续路径带条件路由——成员后的普通节点走 Branch 裁决。
func TestParallelBranchContinuationConditional(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("expand-cond")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
		{Name: "b", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, s *stateT, _ []*stateT) error { return nil })))
	hop := runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil })
	for _, id := range []string{"hop", "amid", "mid"} {
		must(g.AddNode(id, hop, graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	}
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "hop"))
	must(g.AddEdge("fan/b", "mid"))
	must(g.AddBranch("hop", "amid", "skip", func(_ context.Context, s *stateT) (bool, error) {
		return s.Text == "skip", nil
	}))
	must(g.AddEdge("hop", "fan"))
	must(g.AddEdge("amid", "fan"))
	must(g.AddEdge("mid", "fan"))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "direct"); err != nil {
		t.Fatalf("invoke direct: %v", err)
	}
	if _, err := compiled.Invoke(context.Background(), "skip"); err != nil {
		t.Fatalf("invoke skip: %v", err)
	}
}

// 分支路径离开组边界（不接回组汇合）必须拒绝。
func TestParallelBranchPathEscapesGroupRejected(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("escape")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
			Pre:  func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil })))
	must(g.AddNode("leak", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "leak"))
	must(g.AddEdge("leak", graph.End))
	must(g.AddEdge("fan", graph.End))

	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "does not reach") {
		t.Fatalf("compile err=%v", err)
	}
}

// 分支路径成环必须拒绝。
func TestParallelBranchPathCycleRejected(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("cycle")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
			Pre:  func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "fan/a"))
	must(g.AddEdge("fan", graph.End))

	_, err := g.Compile()
	if err == nil {
		t.Fatal("cycle must be rejected")
	}
}

// 分支名与汇合身份冲突在登记期拒绝。
func TestParallelJoinNameCollisionRejected(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("collide")
	if err := g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }); err != nil {
		t.Fatal(err)
	}
	err := g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "__join__", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
			Pre:  func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err=%v", err)
	}
}

// 旧格式检查点（包装节点任务+分支记录）迁移到展开拓扑后完成恢复：
// 已完成分支不重跑，挂起分支沿用原任务记录与交互日志。
func TestParallelLegacyCheckpointMigrates(t *testing.T) {
	type stateT struct {
		Text  string   `json:"text"`
		Calls []string `json:"calls"`
	}
	var fastRuns, slowRuns atomic.Int32
	g := graph.NewGraph[string, string]("legacy")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) {
		return &stateT{Text: text, Calls: []string{}}, nil
	}))
	branch := func(name string, runs *atomic.Int32, interrupt bool) graph.ParallelBranch {
		return graph.ParallelBranch{
			Name: name,
			Node: runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
				runs.Add(1)
				if interrupt {
					answer, err := graph.Interrupt(ctx, "approve "+name)
					if err != nil {
						return "", err
					}
					return s + ":" + fmt.Sprint(answer), nil
				}
				return s + ":" + name, nil
			}),
			Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, out string) error {
				s.Text = out
				s.Calls = append(s.Calls, name)
				return nil
			},
		}
	}
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		branch("fast", &fastRuns, false),
		branch("slow", &slowRuns, true),
	}, graph.WithStateMerger(func(_ context.Context, s *stateT, branches []*stateT) error {
		var parts []string
		for _, b := range branches {
			parts = append(parts, b.Text)
		}
		s.Text = strings.Join(parts, "|")
		s.Calls = append(s.Calls, "merge")
		return nil
	})))
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	store := state.NewMemoryStore()
	compiled := mustCompile(t, g)
	opt := graph.WithCheckpoint(store, "legacy")
	_, err := compiled.Invoke(context.Background(), "base", opt)
	pending := interruptedPending(t, err)
	if len(pending) != 1 || pending[0].Node != "fan/slow" {
		t.Fatalf("pending=%v", pending)
	}
	questionID := pending[0].ID

	// 把新格式进度改写为旧版包装形态：
	// - 调度任务 = 单个包装任务（node=组名，无 group）
	// - 分支记录以「父任务/分支名」登记，Node=组名、问题 Node=分支名
	// - 契约去掉成员/汇合节点与组内边
	record, found, err := store.Load(context.Background(), "legacy")
	if err != nil || !found {
		t.Fatalf("load: found=%v err=%v", found, err)
	}
	var progress map[string]json.RawMessage
	if err := json.Unmarshal(record.Execution, &progress); err != nil {
		t.Fatal(err)
	}
	progress["graph_revision"] = mustJSON(t, "legacy-revision")
	progress["resume_node"] = mustJSON(t, "fan")

	var sched map[string]json.RawMessage
	if err := json.Unmarshal(progress["schedule"], &sched); err != nil {
		t.Fatal(err)
	}
	var schedTasks []map[string]json.RawMessage
	if err := json.Unmarshal(sched["tasks"], &schedTasks); err != nil {
		t.Fatal(err)
	}
	var outerID string
	for _, tk := range schedTasks {
		var node, id string
		if err := json.Unmarshal(tk["node"], &node); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(tk["id"], &id); err != nil {
			t.Fatal(err)
		}
		if node == "fan/slow" {
			outerID = id
		}
	}
	if outerID == "" {
		t.Fatal("slow member task not found in schedule")
	}
	delete(sched, "group")
	sched["tasks"] = mustJSON(t, []map[string]string{{"id": outerID, "node": "fan"}})
	progress["schedule"] = mustJSON(t, sched)
	progress["waiting"] = json.RawMessage(`{"kind":"group","tasks":[` + string(mustJSON(t, outerID+"/slow")) + `]}`)

	var recs []map[string]json.RawMessage
	if err := json.Unmarshal(progress["tasks"], &recs); err != nil {
		t.Fatal(err)
	}
	var legacy []map[string]json.RawMessage
	for _, rec := range recs {
		var node, branchName, id string
		if err := json.Unmarshal(rec["node"], &node); err != nil {
			t.Fatal(err)
		}
		_ = json.Unmarshal(rec["branch"], &branchName)
		_ = json.Unmarshal(rec["id"], &id)
		if branchName == "" {
			legacy = append(legacy, rec)
			continue
		}
		rec["id"] = mustJSON(t, outerID+"/"+branchName)
		rec["node"] = mustJSON(t, "fan")
		delete(rec, "group_id")
		// 旧日志以分支名作为问题归属节点。
		var journal []map[string]json.RawMessage
		if err := json.Unmarshal(rec["interrupts"], &journal); err == nil {
			for i := range journal {
				journal[i]["node"] = mustJSON(t, branchName)
			}
			rec["interrupts"] = mustJSON(t, journal)
		}
		legacy = append(legacy, rec)
	}
	progress["tasks"] = mustJSON(t, legacy)
	// 旧格式在 frame 级保存挂起会话的日志副本：归属节点与恢复点同为组名、
	// 步进与进度一致；任务日志里仍按分支名归属。
	for _, rec := range legacy {
		var branchName string
		if err := json.Unmarshal(rec["branch"], &branchName); err != nil || branchName != "slow" {
			continue
		}
		var journal []map[string]json.RawMessage
		if err := json.Unmarshal(rec["interrupts"], &journal); err != nil {
			t.Fatal(err)
		}
		for i := range journal {
			journal[i]["node"] = progress["resume_node"]
			journal[i]["step"] = progress["step"]
		}
		progress["interrupts"] = mustJSON(t, journal)
	}

	var contract map[string]json.RawMessage
	if err := json.Unmarshal(progress["contract"], &contract); err != nil {
		t.Fatal(err)
	}
	var nodes map[string]string
	if err := json.Unmarshal(contract["nodes"], &nodes); err != nil {
		t.Fatal(err)
	}
	delete(nodes, "fan/fast")
	delete(nodes, "fan/slow")
	delete(nodes, "fan/__join__")
	contract["nodes"] = mustJSON(t, nodes)
	var edges map[string]string
	if err := json.Unmarshal(contract["edges"], &edges); err != nil {
		t.Fatal(err)
	}
	for key := range edges {
		if strings.HasPrefix(key, "fan/fast>") || strings.HasPrefix(key, "fan/slow>") || strings.HasPrefix(key, "fan/__join__>") {
			delete(edges, key)
		}
	}
	contract["edges"] = mustJSON(t, edges)
	progress["contract"] = mustJSON(t, contract)
	record.Execution = mustJSON(t, progress)
	if _, err := store.Save(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if testing.Verbose() {
		t.Logf("progress=%s", progress)
	}

	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(questionID, "yes"))
	if err != nil {
		t.Fatalf("resume legacy: %v", err)
	}
	if out != "base:fast|base:yes" {
		t.Fatalf("out=%q", out)
	}
	if fastRuns.Load() != 1 || slowRuns.Load() != 2 {
		t.Fatalf("fast=%d slow=%d (completed branch must not re-run)", fastRuns.Load(), slowRuns.Load())
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
