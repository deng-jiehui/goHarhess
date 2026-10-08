package contextengine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/internal/benchfixture"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/schema"
)

// benchContextBuild 测一次 ContextBuilder.Invoke；输入生成与构建在计时外。
func benchContextBuild(b *testing.B, name string, request contextengine.Request, check func(b *testing.B, result contextengine.Result)) {
	b.Run(name, func(b *testing.B) {
		builder, err := contextengine.NewContextBuilder(contextengine.Config{
			MaxContextTokens:     request.MaxContextTokens,
			ReservedOutputTokens: request.ReservedOutputTokens,
		})
		if err != nil {
			b.Fatal(err)
		}
		// 正确性预检查：计时的同一条调用路径先核对语义。
		probe, err := builder.Invoke(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		check(b, probe)
		b.ReportAllocs()
		var result contextengine.Result
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			result, err = builder.Invoke(context.Background(), request)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		check(b, result)
	})
}

func pinnedRequest(turns int, toolHistory bool, maxTokens, reserved int) contextengine.Request {
	var history []schema.Message
	if toolHistory {
		history = benchfixture.ToolHistory(turns, 256)
	} else {
		history = benchfixture.TextHistory(turns, 256)
	}
	return contextengine.Request{
		Pinned:               []schema.Message{schema.UserMessage(strings.Repeat("p", 256))},
		History:              history,
		Input:                []schema.Message{schema.UserMessage(strings.Repeat("i", 256))},
		MaxContextTokens:     maxTokens,
		ReservedOutputTokens: reserved,
	}
}

func benchmarkContextGroup(b *testing.B, profile string) {
	switch profile {
	case "smoke":
		// 100 轮工具历史丢弃裁剪。
		benchContextBuild(b, "ContextTrim/DropOld/turns=100/kind=tool",
			pinnedRequest(100, true, 8192, 1024),
			func(b *testing.B, r contextengine.Result) {
				if r.Stats.DroppedTurns == 0 {
					b.Fatal("DropOld did not drop turns")
				}
			})
	case "stress":
		// 1000 轮工具历史重型裁剪，固定迭代运行。
		benchContextBuild(b, "ContextTrim/DropOld/turns=1000/kind=tool",
			pinnedRequest(1000, true, 8192, 1024),
			func(b *testing.B, r contextengine.Result) {
				if r.Stats.DroppedTurns == 0 {
					b.Fatal("DropOld did not drop turns")
				}
			})
	default: // full
		for _, turns := range []int{10, 100, 1000} {
			benchContextBuild(b, fmt.Sprintf("ContextTrim/NoTrim/turns=%d", turns),
				pinnedRequest(turns, false, 2*1024*1024, 1024),
				func(b *testing.B, r contextengine.Result) {
					if r.Stats.DroppedTurns != 0 || r.Stats.DroppedMessages != 0 {
						b.Fatalf("NoTrim dropped %d turns", r.Stats.DroppedTurns)
					}
				})
		}
		for _, turns := range []int{100, 1000} {
			for _, kind := range []struct {
				label string
				tool  bool
			}{{"text", false}, {"tool", true}} {
				benchContextBuild(b, fmt.Sprintf("ContextTrim/DropOld/turns=%d/kind=%s", turns, kind.label),
					pinnedRequest(turns, kind.tool, 8192, 1024),
					func(b *testing.B, r contextengine.Result) {
						if r.Stats.DroppedTurns == 0 {
							b.Fatal("DropOld did not drop turns")
						}
						if r.Stats.OutputTokens > r.Stats.BudgetTokens {
							b.Fatalf("output %d exceeds budget %d", r.Stats.OutputTokens, r.Stats.BudgetTokens)
						}
					})
			}
		}
		// 100 轮中文输入补充：字节预算下非 ASCII 文本的计数与裁剪。
		cjk := pinnedRequest(100, false, 8192, 1024)
		cjk.History = make([]schema.Message, 0, 200)
		for i := 0; i < 100; i++ {
			cjk.History = append(cjk.History,
				schema.UserMessage(strings.Repeat("中", 85)),
				schema.AssistantMessage(strings.Repeat("文", 85)))
		}
		benchContextBuild(b, "ContextTrim/DropOld/turns=100/kind=cjk", cjk,
			func(b *testing.B, r contextengine.Result) {
				if r.Stats.DroppedTurns == 0 {
					b.Fatal("CJK DropOld did not drop turns")
				}
			})
		// 64 KiB 最新输入超出预算时的截断路径。
		oversized := pinnedRequest(10, false, 8192, 1024)
		oversized.Input = []schema.Message{schema.UserMessage(strings.Repeat("i", 64*1024))}
		benchContextBuild(b, "ContextTrim/OversizedLatest/input_bytes=65536", oversized,
			func(b *testing.B, r contextengine.Result) {
				if r.Stats.TruncatedMessages == 0 {
					b.Fatal("OversizedLatest did not truncate")
				}
			})
	}
}

func BenchmarkGHSmoke(b *testing.B)  { benchmarkContextGroup(b, "smoke") }
func BenchmarkGHFull(b *testing.B)   { benchmarkContextGroup(b, "full") }
func BenchmarkGHStress(b *testing.B) { benchmarkContextGroup(b, "stress") }
