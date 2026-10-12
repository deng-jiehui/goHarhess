package redisstore_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// redisBenchEnv 显式开启真实 Redis 基准：只有提供地址时才运行。
const redisBenchEnv = "GOHARNESS_BENCH_REDIS_ADDR"

type benchRedisFixture struct {
	client *redis.Client
	store  *redisstore.RedisStore
	prefix string
}

// newBenchRedis 连接真实 Redis：专用 key 前缀、连接与 Lua 脚本预热在
// 计时外；清理只删除本前缀下的 key。未提供地址时跳过。
func newBenchRedis(b *testing.B) benchRedisFixture {
	b.Helper()
	addr := strings.TrimSpace(os.Getenv(redisBenchEnv))
	if addr == "" {
		b.Skipf("%s not set; skipping real Redis benchmarks", redisBenchEnv)
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		b.Skipf("redis %s unreachable: %v", addr, err)
	}
	prefix := fmt.Sprintf("goharness:bench:%d", time.Now().UnixNano())
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: prefix, TTL: 10 * time.Minute})
	if err != nil {
		_ = client.Close()
		b.Fatal(err)
	}
	// 预热：建立连接、加载 Lua 脚本、初始化内部状态。
	warm := benchRedisCheckpoint(4 << 10)
	warm.ConversationID = "warmup"
	if _, err := store.Save(ctx, warm); err != nil {
		b.Fatalf("warmup save: %v", err)
	}
	if _, ok, err := store.Load(ctx, "warmup"); err != nil || !ok {
		b.Fatalf("warmup load: %v %v", ok, err)
	}
	if err := store.Delete(ctx, "warmup"); err != nil {
		b.Fatalf("warmup delete: %v", err)
	}
	// 报告服务端版本等部署信息，供结果记录。
	if info, err := client.Info(ctx, "server").Result(); err == nil {
		for _, line := range strings.Split(info, "\n") {
			if strings.HasPrefix(line, "redis_version:") {
				b.Logf("redis %s (prefix %s)", strings.TrimSpace(line), prefix)
			}
		}
	}
	fixture := benchRedisFixture{client: client, store: store, prefix: prefix}
	b.Cleanup(func() {
		// 只删除本次基准拥有的 key。
		iter := client.Scan(ctx, 0, prefix+":*", 1000).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		_ = client.Close()
	})
	return fixture
}

// benchRedisCheckpoint 生成固定有效载荷的记录：与核心基准同一形状，
// 起始记录数、TTL 与版本固定。
func benchRedisCheckpoint(payloadBytes int) state.Checkpoint {
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	now := time.Now()
	return state.Checkpoint{
		RunID:          "fixture-run",
		Status:         "running",
		Execution:      []byte(fmt.Sprintf(`{"payload":%q}`, string(payload))),
		SchemaVersion:  state.CurrentCheckpointSchemaVersion,
		ConversationID: "fixture-redis",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// seedRedisHistory 预填 count 条历史记录；连接创建与数据准备在计时外。
func seedRedisHistory(b *testing.B, store *redisstore.RedisStore, conversation string, count int) {
	b.Helper()
	ctx := context.Background()
	var latest state.Checkpoint
	for i := 0; i < count; i++ {
		checkpoint := benchRedisCheckpoint(4 << 10)
		checkpoint.ConversationID = conversation
		checkpoint.ID = latest.ID
		checkpoint.ParentID = latest.ID
		checkpoint.Version = latest.Version
		saved, err := store.Save(ctx, checkpoint)
		if err != nil {
			b.Fatalf("seed %d: %v", i, err)
		}
		latest = saved
	}
}

func BenchmarkGHRedis(b *testing.B) {
	fixture := newBenchRedis(b)
	ctx := context.Background()

	// 固定大小记录 Save / Load：编码、网络与提交等待计入时间。
	// Go 校准会重跑同一子函数，迭代内使用固定会话名并在计时外
	// 删除旧记录，保证每次保存都从相同的起点开始。
	for _, size := range []int{4 << 10, 64 << 10, 512 << 10} {
		b.Run(fmt.Sprintf("RedisStore/save_payload_bytes=%d", size), func(b *testing.B) {
			conversation := fmt.Sprintf("save-%d", size)
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				if err := fixture.store.Delete(ctx, conversation); err != nil {
					b.Fatalf("reset save target: %v", err)
				}
				checkpoint := benchRedisCheckpoint(size)
				checkpoint.ConversationID = conversation
				b.StartTimer()
				saved, err := fixture.store.Save(ctx, checkpoint)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if saved.Version != 1 {
					b.Fatalf("version = %d", saved.Version)
				}
			}
		})
		b.Run(fmt.Sprintf("RedisStore/load_payload_bytes=%d", size), func(b *testing.B) {
			// 子函数重入时先清理再准备相同记录。
			conversation := fmt.Sprintf("load-%d", size)
			if err := fixture.store.Delete(ctx, conversation); err != nil {
				b.Fatalf("reset load target: %v", err)
			}
			checkpoint := benchRedisCheckpoint(size)
			checkpoint.ConversationID = conversation
			if _, err := fixture.store.Save(ctx, checkpoint); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				loaded, ok, err := fixture.store.Load(ctx, conversation)
				if err != nil || !ok {
					b.Fatalf("%v %v", ok, err)
				}
				if len(loaded.Execution) == 0 {
					b.Fatal("empty execution")
				}
			}
		})
	}

	// 100 / 1000 条历史 List。子函数重入时先清空再重填，
	// 保证起始历史规模相同。
	for _, history := range []int{100, 1000} {
		conversation := fmt.Sprintf("history-%d", history)
		if err := fixture.store.Delete(ctx, conversation); err != nil {
			b.Fatalf("reset history target: %v", err)
		}
		seedRedisHistory(b, fixture.store, conversation, history)
		for _, limit := range []int{10, 100} {
			if limit > history {
				continue
			}
			b.Run(fmt.Sprintf("RedisStore/list/history=%d/limit=%d", history, limit), func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					page, err := fixture.store.List(ctx, conversation, state.ListOptions{Limit: limit})
					if err != nil || len(page) != limit {
						b.Fatalf("%d %v", len(page), err)
					}
				}
			})
		}
	}

	// 固定长度日志的重复保存：日志不变，逐条比对全部哈希后只写记录。
	// 量的是会话历史下每次保存的客户端编码与脚本前缀比对成本。
	for _, history := range []int{100, 1000} {
		b.Run(fmt.Sprintf("RedisStore/save_log_compare/history=%d", history), func(b *testing.B) {
			conversation := fmt.Sprintf("compare-%d", history)
			if err := fixture.store.Delete(ctx, conversation); err != nil {
				b.Fatalf("reset compare target: %v", err)
			}
			seed := benchRedisCheckpoint(4 << 10)
			seed.ConversationID = conversation
			for i := 0; i < history; i++ {
				seed.Messages = append(seed.Messages, schema.UserMessage(fmt.Sprintf("history message %04d", i)))
			}
			latest, err := fixture.store.Save(ctx, seed)
			if err != nil {
				b.Fatalf("seed compare history: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				latest, err = fixture.store.Save(ctx, latest)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// 同样 16 节点图的 sync / async 完整执行。固定会话名 +
	// 迭代计时外删除，保证重跑同一子函数不产生版本冲突。
	compiled := benchRedisGraph(b, 16)
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		b.Run(fmt.Sprintf("RedisGraphRun/nodes=16/checkpoint=%s", mode), func(b *testing.B) {
			conversation := fmt.Sprintf("run-%s", mode)
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				if err := fixture.store.Delete(ctx, conversation); err != nil {
					b.Fatalf("reset run target: %v", err)
				}
				options := []runnable.Option{
					graph.WithCheckpoint(fixture.store, conversation),
					graph.WithDurability(mode),
				}
				b.StartTimer()
				out, err := compiled.Invoke(ctx, 0, options...)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if out != 16 {
					b.Fatalf("output = %d", out)
				}
			}
		})
	}
}

// benchRedisGraph 构造与核心基准同形的 16 节点顺序图。
func benchRedisGraph(tb testing.TB, nodes int) *graph.CompiledGraph[int, int] {
	tb.Helper()
	type seqState struct {
		Sum     int
		Payload string
	}
	g := graph.NewGraph[int, int]("bench-redis")
	if err := g.SetStateFactory(func(_ context.Context, input int) (*seqState, error) {
		return &seqState{Sum: input, Payload: strings.Repeat("p", 4<<10)}, nil
	}); err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < nodes; i++ {
		name := fmt.Sprintf("n%d", i)
		if err := g.AddNode(name, runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
			return input + 1, nil
		}),
			graph.WithPreHandler(func(_ context.Context, s *seqState) (int, error) { return s.Sum, nil }),
			graph.WithPostHandler(func(_ context.Context, s *seqState, out int) error { s.Sum = out; return nil })); err != nil {
			tb.Fatal(err)
		}
	}
	for i := 0; i < nodes; i++ {
		from := fmt.Sprintf("n%d", i)
		to := graph.End
		if i+1 < nodes {
			to = fmt.Sprintf("n%d", i+1)
		}
		if err := g.AddEdge(from, to); err != nil {
			tb.Fatal(err)
		}
	}
	if err := g.AddEdge(graph.Start, "n0"); err != nil {
		tb.Fatal(err)
	}
	if err := g.SetMaxSteps(nodes*2 + 8); err != nil {
		tb.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		tb.Fatal(err)
	}
	return compiled
}
