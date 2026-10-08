package state_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/goharness/goharness/internal/benchfixture"
	"github.com/goharness/goharness/pkg/state"
)

// seededStore 预填 count 条历史记录；返回 Store、会话 ID 与最旧记录 ID。
// Save 走乐观锁：候选记录沿用最新记录的 ID 与 Version。
func seededStore(tb testing.TB, ctx context.Context, count int) (*state.MemoryStore, string, string) {
	store := state.NewMemoryStore()
	conversation := "bench-conversation"
	var oldest string
	var latest state.Checkpoint
	for i := 0; i < count; i++ {
		checkpoint := benchfixture.NewCheckpoint(64)
		checkpoint.ConversationID = conversation
		checkpoint.ID = latest.ID
		checkpoint.ParentID = latest.ID
		checkpoint.Version = latest.Version
		saved, err := store.Save(ctx, checkpoint)
		if err != nil {
			tb.Fatalf("seed %d: %v", i, err)
		}
		if i == 0 {
			oldest = saved.ID
		}
		latest = saved
	}
	return store, conversation, oldest
}

func benchmarkStateGroup(b *testing.B, profile string) {
	if profile == "stress" {
		// 方案 Stress 清单不含 state 场景。
		return
	}
	ctx := context.Background()
	// 编解码场景：固定有效载荷 4 / 64 / 512 KiB。
	sizes := []int{4 << 10, 64 << 10, 512 << 10}
	if profile == "smoke" {
		sizes = []int{64 << 10}
	}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("CheckpointCodec/payload_bytes=%d", size), func(b *testing.B) {
			checkpoint := benchfixture.NewCheckpoint(size)
			data, err := checkpoint.JSON()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(data)), "json_bytes")
			b.Run("clone", func(b *testing.B) {
				b.ReportAllocs()
				var out state.Checkpoint
				for n := 0; n < b.N; n++ {
					out = checkpoint.Clone()
				}
				if out.Execution == nil {
					b.Fatal("clone lost execution")
				}
			})
			b.Run("json", func(b *testing.B) {
				b.ReportAllocs()
				var out []byte
				for n := 0; n < b.N; n++ {
					out, err = checkpoint.JSON()
					if err != nil {
						b.Fatal(err)
					}
				}
				if len(out) == 0 {
					b.Fatal("empty encoding")
				}
			})
			b.Run("decode", func(b *testing.B) {
				b.ReportAllocs()
				var out state.Checkpoint
				for n := 0; n < b.N; n++ {
					out, err = state.DecodeCheckpoint(data)
					if err != nil {
						b.Fatal(err)
					}
				}
				if out.RunID != checkpoint.RunID {
					b.Fatal("decode lost identity")
				}
			})
		})
	}
	if profile == "smoke" {
		// Smoke 只保留 64 KiB 编码与 MemoryStore Load。
		b.Run("MemoryStore/load_history=100", func(b *testing.B) {
			store, conversation, _ := seededStore(b, ctx, 100)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				checkpoint, ok, err := store.Load(ctx, conversation)
				if err != nil || !ok {
					b.Fatalf("%v %v", ok, err)
				}
				if checkpoint.Version != 100 {
					b.Fatalf("version = %d", checkpoint.Version)
				}
			}
		})
		return
	}
	// MemoryStore 固定规模操作。
	b.Run("MemoryStore/save_empty", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			store := state.NewMemoryStore()
			checkpoint := benchfixture.NewCheckpoint(4 << 10)
			checkpoint.ConversationID = "save-empty"
			b.StartTimer()
			saved, err := store.Save(ctx, checkpoint)
			if err != nil {
				b.Fatal(err)
			}
			if saved.Version != 1 {
				b.Fatalf("version = %d", saved.Version)
			}
		}
	})
	b.Run("MemoryStore/save_history=100", func(b *testing.B) {
		// 已有 100 条历史、版本 100 时保存第 101 条。
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			store, conversation, _ := seededStore(b, ctx, 100)
			latest, ok, err := store.Load(ctx, conversation)
			if err != nil || !ok {
				b.Fatalf("seed load: %v %v", ok, err)
			}
			checkpoint := benchfixture.NewCheckpoint(4 << 10)
			checkpoint.ConversationID = conversation
			checkpoint.ID = latest.ID
			checkpoint.ParentID = latest.ID
			checkpoint.Version = latest.Version
			b.StartTimer()
			saved, err := store.Save(ctx, checkpoint)
			if err != nil {
				b.Fatal(err)
			}
			if saved.Version != 101 {
				b.Fatalf("version = %d", saved.Version)
			}
		}
	})
	b.Run("MemoryStore/load_history=100", func(b *testing.B) {
		store, conversation, _ := seededStore(b, ctx, 100)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			checkpoint, ok, err := store.Load(ctx, conversation)
			if err != nil || !ok {
				b.Fatalf("%v %v", ok, err)
			}
			if checkpoint.Version != 100 {
				b.Fatalf("version = %d", checkpoint.Version)
			}
		}
	})
	for _, history := range []int{100, 1000} {
		b.Run(fmt.Sprintf("MemoryStore/get_oldest/history=%d", history), func(b *testing.B) {
			store, conversation, oldest := seededStore(b, ctx, history)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				checkpoint, ok, err := store.Get(ctx, conversation, oldest)
				if err != nil || !ok || checkpoint.ID != oldest {
					b.Fatalf("%v %v", ok, err)
				}
			}
		})
		for _, limit := range []int{10, 100} {
			if limit > history {
				continue
			}
			b.Run(fmt.Sprintf("MemoryStore/list_latest/history=%d/limit=%d", history, limit), func(b *testing.B) {
				store, conversation, _ := seededStore(b, ctx, history)
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					page, err := store.List(ctx, conversation, state.ListOptions{Limit: limit})
					if err != nil || len(page) != limit {
						b.Fatalf("%d %v", len(page), err)
					}
				}
			})
		}
	}
}

func BenchmarkGHSmoke(b *testing.B)  { benchmarkStateGroup(b, "smoke") }
func BenchmarkGHFull(b *testing.B)   { benchmarkStateGroup(b, "full") }
func BenchmarkGHStress(b *testing.B) { benchmarkStateGroup(b, "stress") }
