package tool_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/internal/benchfixture"
	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// benchTools 构造固定数量的同规格工具：needsMessages 声明历史注入，
// 返回文本固定 1 KiB；runs 记录每个工具的真实执行次数。
func benchTools(count int, needsMessages []bool) ([]tool.Tool, []atomic.Int32) {
	tools := make([]tool.Tool, 0, count)
	runs := make([]atomic.Int32, count)
	for i := 0; i < count; i++ {
		idx := i
		spec := tool.Spec{
			Name:          fmt.Sprintf("bench-tool-%d", i),
			Concurrency:   tool.ConcurrencyPolicy{Group: "benchmark", MaxParallel: 8},
			NeedsMessages: needsMessages[i],
		}
		tools = append(tools, tool.NewFunctionTool(spec, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
			runs[idx].Add(1)
			if spec.NeedsMessages && input.Messages == nil {
				return tool.Result{}, fmt.Errorf("tool %d declared history but got nil", idx)
			}
			return tool.TextResult(strings.Repeat("r", 1024)), nil
		}))
	}
	return tools, runs
}

func benchToolNames(tools []tool.Tool) []string {
	names := make([]string, len(tools))
	for i, t := range tools {
		names[i] = t.Spec().Name
	}
	return names
}

// checkToolResults 完整核对一批工具结果：条数、顺序、调用 ID 配对、
// 成功标记与正文——业务失败被转成 IsError 结果时必须失败，不能只数条数。
func checkToolResults(b *testing.B, output []schema.Message, tools []tool.Tool) {
	b.Helper()
	if len(output) != len(tools) {
		b.Fatalf("expected %d results, got %d", len(tools), len(output))
	}
	for i, m := range output {
		results := m.FunctionToolResults()
		if len(results) != 1 {
			b.Fatalf("result %d: %d result blocks", i, len(results))
		}
		r := results[0]
		if r.IsError {
			b.Fatalf("result %d (%s) is an error result", i, r.Name)
		}
		wantID := fmt.Sprintf("fixture-batch-%d", i)
		if r.CallID != wantID {
			b.Fatalf("result %d call_id = %q, want %q", i, r.CallID, wantID)
		}
		if r.Name != tools[i].Spec().Name {
			b.Fatalf("result %d name = %q, want %q", i, r.Name, tools[i].Spec().Name)
		}
		if len(r.ContentBlocks) != 1 || r.ContentBlocks[0].Text == nil ||
			len(r.ContentBlocks[0].Text.Text) != 1024 {
			b.Fatalf("result %d body malformed", i)
		}
	}
}

// benchToolBatch 测预建 ToolsNode 的一次完整批次调用：输入为
// turns 轮历史加末尾 Assistant 的 calls 个调用，构造在计时外。
func benchToolBatch(b *testing.B, name string, tools []tool.Tool, runs []atomic.Int32, turns int, ctx context.Context) {
	b.Run(name, func(b *testing.B) {
		node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: tools, MaxConcurrency: 8})
		if err != nil {
			b.Fatal(err)
		}
		messages := append(benchfixture.ToolHistory(turns, 1024),
			benchfixture.ToolCallTail(len(tools), benchToolNames(tools)))
		// 正确性预检查：同一调用路径核对成功标记、正文、ID、顺序与真实执行数。
		// b.N 校准会多次进入本子函数，计数器按每次调用前的清零值为基准。
		for i := range runs {
			runs[i].Store(0)
		}
		output, err := node.Invoke(ctx, messages)
		if err != nil {
			b.Fatal(err)
		}
		checkToolResults(b, output, tools)
		for i := range runs {
			if got := runs[i].Load(); got != 1 {
				b.Fatalf("tool %d executed %d times in one batch", i, got)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			output, err = node.Invoke(ctx, messages)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		checkToolResults(b, output, tools)
		b.ReportMetric(float64(len(tools)), "calls/op")
	})
}

func flagSet(count int, all bool) []bool {
	flags := make([]bool, count)
	for i := range flags {
		flags[i] = all
	}
	return flags
}

// mixedFlags 半数工具声明历史需求。
func mixedFlags(count int) []bool {
	flags := make([]bool, count)
	for i := range flags {
		flags[i] = i%2 == 0
	}
	return flags
}

func benchmarkToolGroup(b *testing.B, profile string) {
	ctx := context.Background()
	switch profile {
	case "smoke":
		tools, runs := benchTools(8, flagSet(8, false))
		benchToolBatch(b, "ToolBatch/calls=8/turns=100/history=off", tools, runs, 100, ctx)
	case "stress":
		// 32 个调用分批调度：重历史与历史注入叠加。
		tools, runs := benchTools(32, flagSet(32, true))
		benchToolBatch(b, "ToolBatch/calls=32/turns=1000/history=on", tools, runs, 1000, ctx)
	default: // full
		for _, calls := range []int{1, 8, 32} {
			for _, need := range []bool{false, true} {
				label := "off"
				if need {
					label = "on"
				}
				tools, runs := benchTools(calls, flagSet(calls, need))
				benchToolBatch(b, fmt.Sprintf("ToolBatch/calls=%d/turns=100/history=%s", calls, label),
					tools, runs, 100, ctx)
			}
		}
		// 8 工具半数需要历史。
		tools, runs := benchTools(8, mixedFlags(8))
		benchToolBatch(b, "ToolBatch/calls=8/turns=100/history=mixed", tools, runs, 100, ctx)
		// 8 个不需要历史的工具比较历史长度影响；turns=100 已由上面
		// calls=8 的场景覆盖，不重复运行。
		for _, turns := range []int{10, 1000} {
			tools, runs := benchTools(8, flagSet(8, false))
			benchToolBatch(b, fmt.Sprintf("ToolBatch/calls=8/turns=%d/history=off", turns),
				tools, runs, turns, ctx)
		}
		// 回调组：0 / 1 / 4 个空回调 × 历史开关。
		for _, callbacks := range []int{0, 1, 4} {
			for _, need := range []bool{false, true} {
				label := "off"
				if need {
					label = "on"
				}
				manager, err := callback.NewManager(callback.Config{}, benchCallbacks(callbacks))
				if err != nil {
					b.Fatal(err)
				}
				tools, runs := benchTools(8, flagSet(8, need))
				benchToolBatch(b, fmt.Sprintf("ToolCallbacks/calls=8/callbacks=%d/history=%s", callbacks, label),
					tools, runs, 100, callback.WithManager(ctx, manager))
			}
		}
	}
}

// benchCallbacks 生成 count 个不打印、不编码的空回调。
func benchCallbacks(count int) []callback.Handler {
	handlers := make([]callback.Handler, 0, count)
	for i := 0; i < count; i++ {
		handlers = append(handlers, tool.CallbackFuncs{CallbackName: fmt.Sprintf("bench-cb-%d", i)})
	}
	return handlers
}

func BenchmarkGHSmoke(b *testing.B)  { benchmarkToolGroup(b, "smoke") }
func BenchmarkGHFull(b *testing.B)   { benchmarkToolGroup(b, "full") }
func BenchmarkGHStress(b *testing.B) { benchmarkToolGroup(b, "stress") }
