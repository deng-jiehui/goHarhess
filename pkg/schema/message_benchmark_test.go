package schema_test

import (
	"fmt"
	"testing"

	"github.com/goharness/goharness/internal/benchfixture"
	"github.com/goharness/goharness/pkg/schema"
)

// benchMessageClone 测一次固定历史的 CloneMessagesStrict；规模写进子
// 基准名。富载荷含工具调用/结果配对与三层嵌套元数据。
func benchMessageClone(b *testing.B, name string, input []schema.Message) {
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		var output []schema.Message
		var err error
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			output, err = schema.CloneMessagesStrict(input)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if len(output) != len(input) {
			b.Fatalf("clone result incomplete: %d/%d", len(output), len(input))
		}
		b.ReportMetric(float64(len(input)), "messages/op")
	})
}

func benchmarkMessageCloneGroup(b *testing.B, sizes []int) {
	for _, size := range sizes {
		// 名为消息条数：TextHistory 参数是轮数（每轮 2 条），折半传入。
		benchMessageClone(b, fmt.Sprintf("messages=%d/kind=text", size), benchfixture.TextHistory(size/2, 1024))
		benchMessageClone(b, fmt.Sprintf("messages=%d/kind=toolmeta", size), benchfixture.MetadataHistory(size, 1024))
	}
}

func BenchmarkGHSmoke(b *testing.B) {
	b.Run("MessageClone", func(b *testing.B) {
		benchmarkMessageCloneGroup(b, []int{100})
	})
}

func BenchmarkGHFull(b *testing.B) {
	b.Run("MessageClone", func(b *testing.B) {
		benchmarkMessageCloneGroup(b, []int{10, 100, 1000})
	})
}

func BenchmarkGHStress(b *testing.B) {
	b.Run("MessageClone", func(b *testing.B) {
		// 1 MiB 内嵌媒体载荷：拷贝成本随数据规模线性放大。
		benchMessageClone(b, "messages=1/media_bytes=1048576", benchfixture.MediaHistory(1<<20))
	})
}
