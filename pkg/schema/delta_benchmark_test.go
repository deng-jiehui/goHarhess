package schema

import (
	"encoding/json"
	"strings"
	"testing"
)

// Both cases build and encode the same 16 KiB text in 256 updates.
func BenchmarkMessageTransport(b *testing.B) {
	chunk := strings.Repeat("x", 64)
	want := strings.Repeat("x", 256*64)
	b.Run("delta", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			a := NewMessageAccumulator()
			if err := a.Apply(MessageDelta{MessageID: "m", Op: MessageStart, Role: RoleAssistant}); err != nil {
				b.Fatal(err)
			}
			block := NewTextBlock("")
			if err := a.Apply(MessageDelta{MessageID: "m", Op: BlockStart, Block: &block}); err != nil {
				b.Fatal(err)
			}
			var bytes int
			for i := 0; i < 256; i++ {
				d := MessageDelta{MessageID: "m", Op: BlockAppend, Text: chunk}
				if err := a.Apply(d); err != nil {
					b.Fatal(err)
				}
				v, err := json.Marshal(d)
				if err != nil {
					b.Fatal(err)
				}
				bytes += len(v)
			}
			if err := a.Apply(MessageDelta{MessageID: "m", Op: MessageEnd}); err != nil {
				b.Fatal(err)
			}
			// 重建全文核对：应用失败或被静默丢弃的增量必须暴露。
			msg, ok := a.Message("m")
			if !ok || msg.JoinedText("") != want {
				b.Fatalf("rebuilt message incomplete: ok=%v len=%d", ok, len(msg.JoinedText("")))
			}
			b.ReportMetric(float64(bytes), "wire_bytes/op")
		}
	})
	b.Run("snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			var text strings.Builder
			var bytes int
			for i := 0; i < 256; i++ {
				text.WriteString(chunk)
				v, err := json.Marshal(AssistantMessage(text.String()))
				if err != nil {
					b.Fatal(err)
				}
				bytes += len(v)
			}
			if text.String() != want {
				b.Fatalf("snapshot text len = %d", text.Len())
			}
			b.ReportMetric(float64(bytes), "wire_bytes/op")
		}
	})
}
