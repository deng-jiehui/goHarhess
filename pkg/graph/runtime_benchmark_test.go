package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/internal/benchfixture"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

type benchSeqState struct {
	Sum     int
	Payload string
}

func mustBench(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

// sequentialGraph 构造 nodes 个节点的顺序链：节点执行固定整数运算，
// State 携带固定载荷。显式设置 MaxSteps 避免规模触及默认步数边界。
func sequentialGraph(tb testing.TB, nodes int, payloadBytes int) *graph.CompiledGraph[int, int] {
	tb.Helper()
	g := graph.NewGraph[int, int]("bench-sequential")
	mustBench(tb, g.SetStateFactory(func(_ context.Context, input int) (*benchSeqState, error) {
		return &benchSeqState{Sum: input, Payload: strings.Repeat("p", payloadBytes)}, nil
	}))
	for i := 0; i < nodes; i++ {
		name := fmt.Sprintf("n%d", i)
		mustBench(tb, g.AddNode(name, runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
			return input + 1, nil
		}),
			graph.WithPreHandler(func(_ context.Context, s *benchSeqState) (int, error) { return s.Sum, nil }),
			graph.WithPostHandler(func(_ context.Context, s *benchSeqState, out int) error { s.Sum = out; return nil }),
		))
	}
	for i := 0; i < nodes; i++ {
		from := fmt.Sprintf("n%d", i)
		to := graph.End
		if i+1 < nodes {
			to = fmt.Sprintf("n%d", i+1)
		}
		mustBench(tb, g.AddEdge(from, to))
	}
	mustBench(tb, g.AddEdge(graph.Start, "n0"))
	mustBench(tb, g.SetMaxSteps(nodes*2+8))
	compiled, err := g.Compile()
	mustBench(tb, err)
	return compiled
}

type benchForkState struct {
	Values  []string
	Payload string
}

// checkForkJoinValues 在汇合节点核对分支结果的数量、取值与顺序：
// 仅数分支数量无法发现分支值或顺序错误。
func checkForkJoinValues(s *benchForkState, branches int) error {
	if len(s.Values) != branches+1 || s.Values[0] != "base" {
		return fmt.Errorf("merged values = %v", s.Values)
	}
	for i := 0; i < branches; i++ {
		if s.Values[i+1] != fmt.Sprintf("b%d=%d", i, i) {
			return fmt.Errorf("merged value %d = %q", i+1, s.Values[i+1])
		}
	}
	return nil
}

// forkJoinGraph 构造原生 AddFork/AddJoin 并行图：branches 个分支从
// Start 扇出汇入 join，各分支写入独立结果，合并逻辑相同。
func forkJoinGraph(tb testing.TB, branches, payloadBytes, maxConcurrency int) *graph.CompiledGraph[int, int] {
	tb.Helper()
	g := graph.NewGraph[int, int]("bench-forkjoin")
	mustBench(tb, g.SetStateFactory(func(_ context.Context, input int) (*benchForkState, error) {
		return &benchForkState{Values: []string{"base"}, Payload: strings.Repeat("p", payloadBytes)}, nil
	}))
	mustBench(tb, g.SetStepMerger(func(_ context.Context, base *benchForkState, results []graph.StepResult[*benchForkState]) error {
		n := len(base.Values)
		for _, r := range results {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	names := make([]string, branches)
	for i := 0; i < branches; i++ {
		name := fmt.Sprintf("b%d", i)
		names[i] = name
		idx := i
		mustBench(tb, g.AddNode(name, runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
			return input + idx, nil
		}),
			graph.WithPreHandler(func(_ context.Context, s *benchForkState) (int, error) { return 0, nil }),
			graph.WithPostHandler(func(_ context.Context, s *benchForkState, out int) error {
				s.Values = append(s.Values, fmt.Sprintf("%s=%d", name, out))
				return nil
			}),
		))
		mustBench(tb, g.AddEdge(name, "join"))
	}
	mustBench(tb, g.AddNode("join", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
		return input + 1, nil
	}),
		graph.WithPreHandler(func(_ context.Context, s *benchForkState) (int, error) {
			if err := checkForkJoinValues(s, branches); err != nil {
				return 0, err
			}
			return len(s.Values), nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *benchForkState, _ int) error { return nil }),
	))
	mustBench(tb, g.AddFork(graph.Start, names...))
	mustBench(tb, g.AddJoin(graph.Start, "join"))
	mustBench(tb, g.AddEdge("join", graph.End))
	if maxConcurrency > 0 {
		mustBench(tb, g.SetMaxConcurrency(maxConcurrency))
	}
	mustBench(tb, g.SetMaxSteps(branches*2+8))
	compiled, err := g.Compile()
	mustBench(tb, err)
	return compiled
}

// parallelNodeGraph 保留原场景名，与 forkjoin 走同一条扇出/扇入路径。
func parallelNodeGraph(tb testing.TB, branches, payloadBytes, maxConcurrency int) *graph.CompiledGraph[int, int] {
	return forkJoinGraph(tb, branches, payloadBytes, maxConcurrency)
}

// benchGraphInvoke 测一次完整 CompiledGraph.Invoke。
func benchGraphInvoke(b *testing.B, name string, compiled *graph.CompiledGraph[int, int], want int, options ...runnable.Option) {
	b.Run(name, func(b *testing.B) {
		probe, err := compiled.Invoke(context.Background(), 0, options...)
		if err != nil || probe != want {
			b.Fatalf("probe = %d, %v", probe, err)
		}
		b.ReportAllocs()
		var out int
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			out, err = compiled.Invoke(context.Background(), 0, options...)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if out != want {
			b.Fatalf("output = %d", out)
		}
	})
}

func benchmarkGraphGroup(b *testing.B, profile string) {
	switch profile {
	case "smoke":
		benchGraphInvoke(b, "SequentialGraph/nodes=16", sequentialGraph(b, 16, 1024), 16)
		benchGraphInvoke(b, "ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024", forkJoinGraph(b, 8, 1024, 0), 10)
	case "stress":
		benchGraphInvoke(b, "ParallelGraph/entry=parallelnode/branches=8/state_bytes=1048576", parallelNodeGraph(b, 8, 1<<20, 0), 10)
	default: // full
		for _, nodes := range []int{1, 10, 100} {
			benchGraphInvoke(b, fmt.Sprintf("SequentialGraph/nodes=%d", nodes), sequentialGraph(b, nodes, 1024), nodes)
		}
		// 构建+编译与单独编译分别命名。
		for _, nodes := range []int{10, 100} {
			b.Run(fmt.Sprintf("SequentialGraph/build_compile/nodes=%d", nodes), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_ = sequentialGraph(b, nodes, 1024)
				}
			})
		}
		b.Run("SequentialGraph/compile_only/nodes=100", func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				g := graph.NewGraph[int, int]("bench-compile")
				mustBench(b, g.SetStateFactory(func(_ context.Context, input int) (*benchSeqState, error) {
					return &benchSeqState{Sum: input}, nil
				}))
				prev := graph.Start
				for i := 0; i < 100; i++ {
					name := fmt.Sprintf("n%d", i)
					mustBench(b, g.AddNode(name, runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
						return input + 1, nil
					}),
						graph.WithPreHandler(func(_ context.Context, s *benchSeqState) (int, error) { return s.Sum, nil }),
						graph.WithPostHandler(func(_ context.Context, s *benchSeqState, out int) error { s.Sum = out; return nil })))
					mustBench(b, g.AddEdge(prev, name))
					prev = name
				}
				mustBench(b, g.AddEdge(prev, graph.End))
				mustBench(b, g.SetMaxSteps(208))
				b.StartTimer()
				compiled, err := g.Compile()
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if compiled == nil {
					b.Fatal("compile returned nil")
				}
			}
		})
		// 两种并行入口：2 / 8 / 32 分支 1 KiB State；8 分支补 64 KiB 与并发上限。
		for _, branches := range []int{2, 8, 32} {
			benchGraphInvoke(b, fmt.Sprintf("ParallelGraph/entry=forkjoin/branches=%d/state_bytes=1024", branches),
				forkJoinGraph(b, branches, 1024, 0), branches+2)
			benchGraphInvoke(b, fmt.Sprintf("ParallelGraph/entry=parallelnode/branches=%d/state_bytes=1024", branches),
				parallelNodeGraph(b, branches, 1024, 0), branches+2)
		}
		benchGraphInvoke(b, "ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536", forkJoinGraph(b, 8, 64<<10, 0), 10)
		benchGraphInvoke(b, "ParallelGraph/entry=parallelnode/branches=8/state_bytes=65536", parallelNodeGraph(b, 8, 64<<10, 0), 10)
		for _, limit := range []int{1, 8} {
			benchGraphInvoke(b, fmt.Sprintf("ParallelGraph/entry=forkjoin/branches=8/max_concurrency=%d", limit),
				forkJoinGraph(b, 8, 1024, limit), 10)
			benchGraphInvoke(b, fmt.Sprintf("ParallelGraph/entry=parallelnode/branches=8/max_concurrency=%d", limit),
				parallelNodeGraph(b, 8, 1024, limit), 10)
		}
	}
}

// checkpointedSequential 测 16 节点、固定载荷图的完整运行；
// 持久化选项经 options 传入，Store 在计时外重置。
func benchmarkPersistedGraphGroup(b *testing.B, profile string) {
	type mode struct {
		label string
		setup func(b *testing.B) []runnable.Option
	}
	modes := []mode{
		{label: "off", setup: func(b *testing.B) []runnable.Option { return nil }},
		{label: "sync", setup: func(b *testing.B) []runnable.Option {
			return []runnable.Option{
				graph.WithCheckpoint(state.NewMemoryStore(), "bench-persist"),
				graph.WithDurability(graph.DurabilitySync),
			}
		}},
		{label: "async", setup: func(b *testing.B) []runnable.Option {
			return []runnable.Option{
				graph.WithCheckpoint(state.NewMemoryStore(), "bench-persist"),
				graph.WithDurability(graph.DurabilityAsync),
			}
		}},
	}
	payloads := []int{4 << 10, 64 << 10}
	if profile == "smoke" {
		payloads = []int{4 << 10}
		modes = modes[2:]
	}
	if profile == "stress" {
		return
	}
	for _, payload := range payloads {
		for _, m := range modes {
			b.Run(fmt.Sprintf("PersistedGraph/nodes=16/state_bytes=%d/checkpoint=%s", payload, m.label), func(b *testing.B) {
				compiled := sequentialGraph(b, 16, payload)
				b.ReportAllocs()
				// 构图与编译不进计时；观测包装也不在计时循环内。
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					b.StopTimer()
					options := m.setup(b)
					b.StartTimer()
					out, err := compiled.Invoke(context.Background(), 0, options...)
					if err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					if out != 16 {
						b.Fatalf("output = %d", out)
					}
				}
				if m.label == "off" {
					return
				}
				// 计时外观测运行：相同输入与持久化模式，经 StoreObserver
				// 上报保存次数与数据量。
				observer := benchfixture.NewStoreObserver(state.NewMemoryStore())
				observed := []runnable.Option{
					graph.WithCheckpoint(observer, "bench-persist"),
					graph.WithDurability(map[string]graph.Durability{
						"sync":  graph.DurabilitySync,
						"async": graph.DurabilityAsync,
					}[m.label]),
				}
				out, err := compiled.Invoke(context.Background(), 0, observed...)
				if err != nil {
					b.Fatal(err)
				}
				if out != 16 {
					b.Fatalf("observed run output = %d", out)
				}
				benchfixture.ReportStoreObservation(b, observer, "bench-persist")
			})
		}
	}
}

// suspendResumeGraph 构造 8 节点顺序图，n4 在中段发起审批：Interrupt 挂起时
// 返回错误，恢复重放时消费答案后继续——挂起与恢复共用同一编译图。
// 返回各节点真实执行次数，供恢复后增量核对。
func suspendResumeGraph(tb testing.TB) (*graph.CompiledGraph[int, int], *[8]atomic.Int32) {
	tb.Helper()
	var runs [8]atomic.Int32
	g := graph.NewGraph[int, int]("bench-resume")
	mustBench(tb, g.SetStateFactory(func(_ context.Context, input int) (*benchSeqState, error) {
		return &benchSeqState{Sum: input}, nil
	}))
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("n%d", i)
		idx := i
		mustBench(tb, g.AddNode(name, runnable.Func[int, int](func(ctx context.Context, input int, _ ...runnable.Option) (int, error) {
			runs[idx].Add(1)
			if idx == 4 {
				if _, err := graph.Interrupt(ctx, "approve-mid"); err != nil {
					return 0, err
				}
			}
			return input + 1, nil
		}),
			graph.WithPreHandler(func(_ context.Context, s *benchSeqState) (int, error) { return s.Sum, nil }),
			graph.WithPostHandler(func(_ context.Context, s *benchSeqState, out int) error { s.Sum = out; return nil })))
	}
	for i := 0; i < 8; i++ {
		from := fmt.Sprintf("n%d", i)
		to := graph.End
		if i+1 < 8 {
			to = fmt.Sprintf("n%d", i+1)
		}
		mustBench(tb, g.AddEdge(from, to))
	}
	mustBench(tb, g.AddEdge(graph.Start, "n0"))
	mustBench(tb, g.SetMaxSteps(24))
	compiled, err := g.Compile()
	mustBench(tb, err)
	return compiled, &runs
}

// suspendGraphFixture 生成固定暂停现场：返回编译图、暂停 JSON、待答
// 问题 ID 与各节点执行计数；恢复每次迭代把同一现场导入新 Store。
func suspendGraphFixture(tb testing.TB) (*graph.CompiledGraph[int, int], []byte, string, *[8]atomic.Int32) {
	tb.Helper()
	compiled, runs := suspendResumeGraph(tb)
	source := state.NewMemoryStore()
	_, err := compiled.Invoke(context.Background(), 0, graph.WithCheckpoint(source, "bench-resume"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		tb.Fatalf("expected interruption, got %v", err)
	}
	data, err := stopped.Checkpoint.JSON()
	mustBench(tb, err)
	questionID := ""
	for _, record := range stopped.PendingInterrupts() {
		if !record.Answered {
			questionID = record.ID
		}
	}
	if questionID == "" {
		tb.Fatal("no pending question recorded")
	}
	return compiled, data, questionID, runs
}

// suspendForkFixture 生成 8 分支并行暂停现场：b0..b6 完成并持久化，
// b7 在分支内发起审批等待恢复。返回分支与汇合节点的执行计数。
func suspendForkFixture(tb testing.TB) (*graph.CompiledGraph[int, int], []byte, string, *[9]atomic.Int32) {
	tb.Helper()
	var runs [9]atomic.Int32 // 0..7 为分支，8 为汇合节点
	g := graph.NewGraph[int, int]("bench-resume-fork")
	mustBench(tb, g.SetStateFactory(func(_ context.Context, input int) (*benchForkState, error) {
		return &benchForkState{Values: []string{"base"}}, nil
	}))
	mustBench(tb, g.SetStepMerger(func(_ context.Context, base *benchForkState, results []graph.StepResult[*benchForkState]) error {
		n := len(base.Values)
		for _, r := range results {
			base.Values = append(base.Values, r.State.Values[n:]...)
		}
		return nil
	}))
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("b%d", i)
		idx := i
		mustBench(tb, g.AddNode(name, runnable.Func[int, int](func(ctx context.Context, input int, _ ...runnable.Option) (int, error) {
			runs[idx].Add(1)
			if idx == 7 {
				if _, err := graph.Interrupt(ctx, "approve-branch"); err != nil {
					return 0, err
				}
			}
			return input + idx, nil
		}),
			graph.WithPreHandler(func(_ context.Context, _ *benchForkState) (int, error) { return 0, nil }),
			graph.WithPostHandler(func(_ context.Context, s *benchForkState, out int) error {
				s.Values = append(s.Values, fmt.Sprintf("b%d=%d", idx, out))
				return nil
			})))
		mustBench(tb, g.AddEdge(name, "join"))
	}
	mustBench(tb, g.AddNode("join", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
		runs[8].Add(1)
		return input + 1, nil
	}),
		graph.WithPreHandler(func(_ context.Context, s *benchForkState) (int, error) {
			if err := checkForkJoinValues(s, 8); err != nil {
				return 0, err
			}
			return len(s.Values), nil
		}),
		graph.WithPostHandler(func(_ context.Context, _ *benchForkState, _ int) error { return nil })))
	names := make([]string, 8)
	for i := range names {
		names[i] = fmt.Sprintf("b%d", i)
	}
	mustBench(tb, g.AddFork(graph.Start, names...))
	mustBench(tb, g.AddJoin(graph.Start, "join"))
	mustBench(tb, g.AddEdge("join", graph.End))
	mustBench(tb, g.SetMaxSteps(24))
	compiled, err := g.Compile()
	mustBench(tb, err)
	source := state.NewMemoryStore()
	_, err = compiled.Invoke(context.Background(), 0, graph.WithCheckpoint(source, "bench-resume-fork"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		tb.Fatalf("expected interruption, got %v", err)
	}
	data, err := stopped.Checkpoint.JSON()
	mustBench(tb, err)
	questionID := ""
	for _, record := range stopped.PendingInterrupts() {
		if !record.Answered {
			questionID = record.ID
		}
	}
	if questionID == "" {
		tb.Fatal("no pending question recorded")
	}
	return compiled, data, questionID, &runs
}

// completedRunFixture 生成一次已完成的持久化运行：返回编译图、含最终
// 检查点的 Store 与节点执行计数；恢复已完成运行应直接读取结果。
func completedRunFixture(tb testing.TB) (*graph.CompiledGraph[int, int], state.Store, *atomic.Int32) {
	tb.Helper()
	var runs atomic.Int32
	g := graph.NewGraph[int, int]("bench-completed")
	mustBench(tb, g.SetStateFactory(func(_ context.Context, input int) (*benchSeqState, error) {
		return &benchSeqState{Sum: input}, nil
	}))
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("n%d", i)
		mustBench(tb, g.AddNode(name, runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
			runs.Add(1)
			return input + 1, nil
		}),
			graph.WithPreHandler(func(_ context.Context, s *benchSeqState) (int, error) { return s.Sum, nil }),
			graph.WithPostHandler(func(_ context.Context, s *benchSeqState, out int) error { s.Sum = out; return nil })))
	}
	prev := graph.Start
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("n%d", i)
		to := graph.End
		if i+1 < 4 {
			to = fmt.Sprintf("n%d", i+1)
		}
		mustBench(tb, g.AddEdge(prev, name))
		prev = name
		if i+1 == 4 {
			mustBench(tb, g.AddEdge(name, to))
		}
	}
	mustBench(tb, g.SetMaxSteps(12))
	compiled, err := g.Compile()
	mustBench(tb, err)
	store := state.NewMemoryStore()
	out, err := compiled.Invoke(context.Background(), 0, graph.WithCheckpoint(store, "bench-completed"))
	if err != nil || out != 4 || runs.Load() != 4 {
		tb.Fatalf("setup run: out=%d runs=%d err=%v", out, runs.Load(), err)
	}
	return compiled, store, &runs
}

func benchmarkResumeGroup(b *testing.B) {
	compiled, data, questionID, seqRuns := suspendGraphFixture(b)
	forkCompiled, forkData, forkQuestionID, forkRuns := suspendForkFixture(b)
	completedCompiled, completedStore, completedRuns := completedRunFixture(b)
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		b.Run(fmt.Sprintf("ResumeGraph/sequential_suspend/checkpoint=%s", mode), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				store := state.NewMemoryStore()
				if _, err := benchfixture.ImportSuspended(context.Background(), data, store); err != nil {
					b.Fatal(err)
				}
				options := []runnable.Option{
					graph.WithCheckpoint(store, "bench-resume"),
					graph.WithDurability(mode),
					graph.WithResume(questionID, 0),
				}
				var before [8]int32
				for i := range before {
					before[i] = seqRuns[i].Load()
				}
				b.StartTimer()
				out, err := compiled.Resume(context.Background(), options...)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if out != 8 {
					b.Fatalf("resumed output = %d", out)
				}
				// n0..n3 已提交不得重跑，n4..n7 恰好多执行一次。
				for i := 0; i < 4; i++ {
					if got := seqRuns[i].Load() - before[i]; got != 0 {
						b.Fatalf("committed node n%d re-executed %d times", i, got)
					}
				}
				for i := 4; i < 8; i++ {
					if got := seqRuns[i].Load() - before[i]; got != 1 {
						b.Fatalf("node n%d executions this resume = %d, want 1", i, got)
					}
				}
			}
		})
		// 8 分支中 7 个完成、1 个等待审批：恢复只补跑等待分支与汇合。
		b.Run(fmt.Sprintf("ResumeGraph/parallel_suspend/checkpoint=%s", mode), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				store := state.NewMemoryStore()
				if _, err := benchfixture.ImportSuspended(context.Background(), forkData, store); err != nil {
					b.Fatal(err)
				}
				options := []runnable.Option{
					graph.WithCheckpoint(store, "bench-resume-fork"),
					graph.WithDurability(mode),
					graph.WithResume(forkQuestionID, "yes"),
				}
				var before [9]int32
				for i := range before {
					before[i] = forkRuns[i].Load()
				}
				b.StartTimer()
				out, err := forkCompiled.Resume(context.Background(), options...)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if out != 10 {
					b.Fatalf("resumed output = %d", out)
				}
				for i := 0; i < 7; i++ {
					if got := forkRuns[i].Load() - before[i]; got != 0 {
						b.Fatalf("completed branch b%d re-executed %d times", i, got)
					}
				}
				if got := forkRuns[7].Load() - before[7]; got != 1 {
					b.Fatalf("waiting branch executions this resume = %d, want 1", got)
				}
				if got := forkRuns[8].Load() - before[8]; got != 1 {
					b.Fatalf("join executions this resume = %d, want 1", got)
				}
			}
		})
		// 已完成运行的结果读取：Resume 直接返回保存的结果，不执行节点。
		b.Run(fmt.Sprintf("ResumeGraph/completed_result/checkpoint=%s", mode), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				before := completedRuns.Load()
				out, err := completedCompiled.Resume(context.Background(),
					graph.WithCheckpoint(completedStore, "bench-completed"),
					graph.WithDurability(mode))
				if err != nil {
					b.Fatal(err)
				}
				if out != 4 {
					b.Fatalf("completed result = %d", out)
				}
				if got := completedRuns.Load() - before; got != 0 {
					b.Fatalf("completed run re-executed %d nodes", got)
				}
			}
		})
	}
}

func BenchmarkGHSmoke(b *testing.B) {
	benchmarkGraphGroup(b, "smoke")
	benchmarkPersistedGraphGroup(b, "smoke")
}

func BenchmarkGHFull(b *testing.B) {
	benchmarkGraphGroup(b, "full")
	benchmarkPersistedGraphGroup(b, "full")
	benchmarkResumeGroup(b)
}

func BenchmarkGHStress(b *testing.B) {
	benchmarkGraphGroup(b, "stress")
}
