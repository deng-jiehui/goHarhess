package agent_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/internal/benchfixture"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/stream"
	"github.com/goharness/goharness/pkg/tool"
)

// benchReActAgent 构造固定 Agent：确定性模型、一个回显工具；
// 返回工具真实执行次数供运行后核对。
func benchReActAgent(tb testing.TB, modelCalls int, needsHistory bool) (agent.Agent, *atomic.Int32) {
	tb.Helper()
	var calls atomic.Int32
	echo := tool.NewFunctionTool(tool.Spec{
		Name:          "bench-tool",
		NeedsMessages: needsHistory,
		Concurrency:   tool.ConcurrencyPolicy{Group: "benchmark", MaxParallel: 8},
	}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		calls.Add(1)
		return tool.TextResult(strings.Repeat("r", 1024)), nil
	})
	value, err := agent.New(benchfixture.NewStepModel(modelCalls-1, "bench-tool", strings.Repeat("a", 1024)), agent.WithName("bench-react"), agent.WithTools(echo), agent.WithMaxLLMCalls(modelCalls))
	if err != nil {
		tb.Fatal(err)
	}
	return value, &calls
}

// benchReAct 测一次完整 ReAct 运行：modelCalls 次模型调用（最后一次
// 为最终回答），消息增量数量为 2*modelCalls-1 经调用方核对。
func benchReAct(b *testing.B, name string, modelCalls int, needsHistory bool, options ...runnable.Option) {
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			value, toolRuns := benchReActAgent(b, modelCalls, needsHistory)
			input := []schema.Message{schema.UserMessage("bench")}
			b.StartTimer()
			output, err := value.Invoke(context.Background(), input, options...)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			if got := toolRuns.Load(); got != int32(modelCalls-1) {
				b.Fatalf("tool executions = %d, want %d", got, modelCalls-1)
			}
			// 2*modelCalls-1 条增量消息：工具调用与结果交替，末条为最终答案。
			if len(output) != 2*modelCalls-1 {
				b.Fatalf("messages = %d, want %d", len(output), 2*modelCalls-1)
			}
		}
	})
}

func checkpointOption(store state.Store, durability graph.Durability) []runnable.Option {
	if store == nil {
		return nil
	}
	return []runnable.Option{
		agent.WithConversationID("bench-conversation"),
		agent.WithCheckpoint(store, "bench-conversation"),
		agent.WithDurability(durability),
	}
}

func benchmarkAgentGroup(b *testing.B, profile string) {
	switch profile {
	case "smoke":
		benchReAct(b, "ReAct/model_calls=10/checkpoint=off", 10, false)
		benchmarkToolSuspendResume(b, "smoke")
	case "stress":
		// 同会话 1000 次连续请求的历史增长压力组。
		benchSessionGrowth(b, "SessionGrowth/requests=1000", 1000)
		// 64 KiB 文本、1024 个片段的分段流。
		benchAgentStream(b, "AgentStream/mode=events/text_bytes=65536", "events", strings.Repeat("s", 64*1024), 1024)
	default: // full
		for _, calls := range []int{10, 50, 100} {
			benchReAct(b, fmt.Sprintf("ReAct/model_calls=%d/checkpoint=off", calls), calls, false)
		}
		for _, need := range []bool{false, true} {
			label := "off"
			if need {
				label = "on"
			}
			benchReAct(b, fmt.Sprintf("ReAct/model_calls=50/history=%s", label), 50, need)
		}
		// Agent 持久化：10 / 50 次模型调用，off/sync/async。
		for _, calls := range []int{10, 50} {
			for _, mode := range []struct {
				label      string
				durability graph.Durability
				store      bool
			}{{"off", "", false}, {"sync", graph.DurabilitySync, true}, {"async", graph.DurabilityAsync, true}} {
				b.Run(fmt.Sprintf("AgentPersist/model_calls=%d/checkpoint=%s", calls, mode.label), func(b *testing.B) {
					b.ReportAllocs()
					var toolRuns *atomic.Int32
					newAgent := func(store state.Store) agent.Agent {
						var runs atomic.Int32
						toolRuns = &runs
						echo := tool.NewFunctionTool(tool.Spec{Name: "bench-tool"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
							runs.Add(1)
							return tool.TextResult(strings.Repeat("r", 1024)), nil
						})
						value, err := agent.New(benchfixture.NewStepModel(calls-1, "bench-tool", "done"), agent.WithName("bench-persist"), agent.WithTools(echo), agent.WithCheckpointStore(store), agent.WithMaxLLMCalls(calls))
						if err != nil {
							b.Fatal(err)
						}
						return value
					}
					for n := 0; n < b.N; n++ {
						b.StopTimer()
						// 计时内只用原生 MemoryStore：观测包装的开销不计入。
						var store state.Store
						if mode.store {
							store = state.NewMemoryStore()
						}
						value := newAgent(store)
						options := checkpointOption(store, mode.durability)
						b.StartTimer()
						output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("bench")}, options...)
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						if len(output) != 2*calls-1 {
							b.Fatalf("messages = %d", len(output))
						}
						if got := toolRuns.Load(); got != int32(calls-1) {
							b.Fatalf("tool executions = %d, want %d", got, calls-1)
						}
					}
					if mode.store {
						// 计时外观测运行：相同输入与持久化模式，经 StoreObserver
						// 上报保存次数与数据量，不影响上面的计时口径。
						observer := benchfixture.NewStoreObserver(state.NewMemoryStore())
						value := newAgent(observer)
						output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("bench")},
							checkpointOption(observer, mode.durability)...)
						if err != nil {
							b.Fatal(err)
						}
						if len(output) != 2*calls-1 {
							b.Fatalf("observed run messages = %d", len(output))
						}
						benchfixture.ReportStoreObservation(b, observer, "bench-conversation")
					}
				})
			}
		}
		// 会话增长：同会话 10 / 100 次连续请求，每次一工具调用加最终回答。
		benchSessionGrowth(b, "SessionGrowth/requests=10", 10)
		benchSessionGrowth(b, "SessionGrowth/requests=100", 100)
		// 固定现场恢复：同批 8 工具 7 完成 1 等待审批。
		benchmarkToolSuspendResume(b, "full")
		// 消息与公共事件流。
		benchmarkAgentStreams(b)
	}
}

// benchSessionGrowth 测同会话连续 requests 次请求为一次 op：
// 每次请求为一次工具调用加一次最终回答；Store 每次 op 从空开始。
func benchSessionGrowth(b *testing.B, name string, requests int) {
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			store := state.NewMemoryStore()
			value, err := agent.New(benchfixture.NewTurnModel("bench-tool", "done"), agent.WithName("bench-session"), agent.WithTools(tool.NewFunctionTool(tool.Spec{Name: "bench-tool"}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
				return tool.TextResult("r"), nil
			})), agent.WithCheckpointStore(store), agent.WithMaxLLMCalls(2))
			if err != nil {
				b.Fatal(err)
			}
			options := []runnable.Option{
				agent.WithConversationID("bench-growth"),
				agent.WithCheckpoint(store, "bench-growth"),
			}
			b.StartTimer()
			for i := 0; i < requests; i++ {
				output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("next")}, options...)
				if err != nil {
					b.Fatal(err)
				}
				// 每次请求固定一次工具调用加一次最终回答 = 3 条增量消息。
				if len(output) != 3 {
					b.Fatalf("messages = %d, want 3", len(output))
				}
			}
			b.StopTimer()
			if _, ok, err := store.Load(context.Background(), "bench-growth"); err != nil || !ok {
				b.Fatalf("session state missing: %v %v", ok, err)
			}
			b.ReportMetric(float64(requests), "requests/op")
		}
	})
}

// benchmarkToolSuspendResume 测固定现场恢复：同批 8 个工具 7 个已
// 完成、1 个等待审批，恢复只执行剩余调用。
func benchmarkToolSuspendResume(b *testing.B, profile string) {
	_ = profile
	// 构造暂停现场：模型一次发出 8 个调用，7 个直接完成，ask 挂起。
	var askRuns atomic.Int32
	var doneRuns [7]atomic.Int32
	tools := make([]tool.Tool, 0, 8)
	for i := 0; i < 7; i++ {
		idx := i
		tools = append(tools, tool.NewFunctionTool(tool.Spec{Name: fmt.Sprintf("done-%d", i)}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
			doneRuns[idx].Add(1)
			return tool.TextResult("ok"), nil
		}))
	}
	tools = append(tools, tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		askRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "approve-tool")
		if err != nil {
			return tool.Result{}, err
		}
		return tool.TextResult(fmt.Sprintf("approved:%v", answer)), nil
	}))
	names := make([]string, 8)
	for i := range tools {
		names[i] = tools[i].Spec().Name
	}
	newAgent := func(store state.Store) agent.Agent {
		value, err := agent.New(benchfixture.NewScriptedStepModel(
			benchfixture.ModelStep{Calls: names},
			benchfixture.ModelStep{Text: "done"},
		), agent.WithName("bench-tool-resume"), agent.WithTools(tools...), agent.WithCheckpointStore(store), agent.WithMaxLLMCalls(8))
		if err != nil {
			b.Fatal(err)
		}
		return value
	}
	source := state.NewMemoryStore()
	first := newAgent(source)
	_, err := first.Invoke(context.Background(), []schema.Message{schema.UserMessage("bench")},
		agent.WithConversationID("bench-tool-resume"),
		agent.WithCheckpoint(source, "bench-tool-resume"))
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		b.Fatalf("expected interruption, got %v", err)
	}
	if len(suspended.PendingInterrupts()) != 1 {
		b.Fatalf("pending interrupts = %d", len(suspended.PendingInterrupts()))
	}
	questionID := suspended.PendingInterrupts()[0].ID
	stopped, ok, err := source.Load(context.Background(), "bench-tool-resume")
	if err != nil || !ok {
		b.Fatalf("suspended checkpoint missing: %v %v", ok, err)
	}
	data, err := stopped.JSON()
	if err != nil {
		b.Fatal(err)
	}
	totalDone := func() int32 {
		var sum int32
		for i := range doneRuns {
			sum += doneRuns[i].Load()
		}
		return sum
	}
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		b.Run(fmt.Sprintf("ResumeAgent/tool_suspend/checkpoint=%s", mode), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				store := state.NewMemoryStore()
				if _, err := benchfixture.ImportSuspended(context.Background(), data, store); err != nil {
					b.Fatal(err)
				}
				resumed := newAgent(store)
				options := []runnable.Option{
					agent.WithConversationID("bench-tool-resume"),
					agent.WithCheckpoint(store, "bench-tool-resume"),
					agent.WithDurability(mode),
					agent.WithResume(questionID, "yes"),
				}
				doneBefore, askBefore := totalDone(), askRuns.Load()
				b.StartTimer()
				output, err := agent.Resume(context.Background(), resumed, options...)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if len(output) == 0 || output[len(output)-1].JoinedText("") != "done" {
					b.Fatalf("resumed output = %#v", output)
				}
				// 已完成工具不得重跑；等待审批的工具恰好多执行一次。
				if got := totalDone() - doneBefore; got != 0 {
					b.Fatalf("completed tools re-executed: %d", got)
				}
				if got := askRuns.Load() - askBefore; got != 1 {
					b.Fatalf("waiting tool executions this run = %d, want 1", got)
				}
			}
		})
	}
}

// benchAgentStream 测一条 text 分 segments 帧发送的流式消费路径：
// mode 为 invoke / stream / events / events_lifecycle，同步写进子基准名。
func benchAgentStream(b *testing.B, name, mode, text string, segments int) {
	b.Run(name, func(b *testing.B) {
		switch mode {
		case "invoke", "stream", "events", "events_lifecycle":
		default:
			b.Fatalf("unknown stream mode %q", mode)
		}
		b.StopTimer()
		textBytes := len(text)
		// 计时外核对：公共事件流逐片收到文本增量（起止事件不计入
		// 片段）、消息起止完整、恰好一次最终结果；分段发布退化为
		// 结束时单次快照时必须失败。完成后 ResetTimer 丢弃预检查
		// 计入的时间与分配，且保持计时停止进入正式循环。
		probe, err := agent.New(benchfixture.NewSegmentModel(text, segments), agent.WithName("bench-stream-probe"))
		if err != nil {
			b.Fatal(err)
		}
		probeEvents, err := agent.StreamEvents(context.Background(), probe,
			[]schema.Message{schema.UserMessage("go")}, stream.Config{Buffer: 64})
		if err != nil {
			b.Fatal(err)
		}
		appends, starts, ends, results := 0, 0, 0, 0
		var rebuilt strings.Builder
		for {
			event, err := probeEvents.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			switch event.Kind {
			case stream.KindMessage:
				if event.Message.Delta != nil {
					switch event.Message.Delta.Op {
					case schema.MessageStart:
						starts++
					case schema.BlockAppend:
						appends++
						rebuilt.WriteString(event.Message.Delta.Text)
					case schema.MessageEnd:
						ends++
					}
				}
			case stream.KindResult:
				if event.Result != nil {
					results++
				}
			}
		}
		wantAppends := benchfixture.NewSegmentModel(text, segments).SegmentCount()
		if starts != 1 || ends != 1 || results != 1 || appends != wantAppends || rebuilt.String() != text {
			b.Fatalf("public stream probe: starts=%d ends=%d appends=%d/%d results=%d rebuilt=%dB",
				starts, ends, appends, wantAppends, results, rebuilt.Len())
		}
		b.ReportAllocs()
		b.ResetTimer()
		var lastLen int
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			value, err := agent.New(benchfixture.NewSegmentModel(text, segments), agent.WithName("bench-stream"))
			if err != nil {
				b.Fatal(err)
			}
			input := []schema.Message{schema.UserMessage("go")}
			b.StartTimer()
			switch mode {
			case "invoke":
				output, err := value.Invoke(context.Background(), input)
				if err != nil {
					b.Fatal(err)
				}
				lastLen = len(output[len(output)-1].JoinedText(""))
			case "stream":
				reader, err := value.Stream(context.Background(), input)
				if err != nil {
					b.Fatal(err)
				}
				output, err := reader.Collect(reader.Context())
				if err != nil {
					b.Fatal(err)
				}
				lastLen = len(output[len(output)-1].JoinedText(""))
			case "events", "events_lifecycle":
				config := stream.Config{Buffer: 64, Lifecycle: mode == "events_lifecycle"}
				events, err := agent.StreamEvents(context.Background(), value, input, config)
				if err != nil {
					b.Fatal(err)
				}
				var results int
				for {
					event, err := events.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					if event.Kind == stream.KindResult && event.Result != nil {
						results++
						lastLen = len((*event.Result)[len(*event.Result)-1].JoinedText(""))
					}
				}
				if results != 1 {
					b.Fatalf("stream results = %d", results)
				}
			}
			b.StopTimer()
			if lastLen != textBytes {
				b.Fatalf("final text = %d", lastLen)
			}
		}
	})
}

// benchmarkAgentStreams 比较 Invoke、Stream 消费与公共事件流消费：
// 分段模型真正把文本分帧发送，每种方式得到相同业务结果。
func benchmarkAgentStreams(b *testing.B) {
	const textBytes = 16 * 1024
	for _, mode := range []string{"invoke", "stream", "events", "events_lifecycle"} {
		benchAgentStream(b, fmt.Sprintf("AgentStream/mode=%s/text_bytes=%d", mode, textBytes),
			mode, strings.Repeat("s", textBytes), 256)
	}
}

func BenchmarkGHSmoke(b *testing.B)  { benchmarkAgentGroup(b, "smoke") }
func BenchmarkGHFull(b *testing.B)   { benchmarkAgentGroup(b, "full") }
func BenchmarkGHStress(b *testing.B) { benchmarkAgentGroup(b, "stress") }
