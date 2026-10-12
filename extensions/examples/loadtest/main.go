// Command loadtest 是阶段六第零波（W0-2）的并发压测工具：可配置延迟的
// 假模型驱动同一批 Agent 在多会话并发下运行，输出各阶段耗时分位、错误
// 计数、协程数与内存，并支持 pprof。第零波用它取基线，后续每一波用它
// 做加入各组件前后的对照。
//
// 用法（在 extensions/ 目录下）：
//
//	go run ./examples/loadtest -sessions 64 -concurrency 16 -requests 256
//	go run ./examples/loadtest -store redis -redisaddr 127.0.0.1:6379 -history 200 -probe
//	go run ./examples/loadtest -mode seed -sessions 5000 -history 100 -store redis   # 量 logs 缓存
//	go run ./examples/loadtest -pprofaddr 127.0.0.1:6060 -cpuprofile cpu.pb.gz
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// ---------- 指标收集 ----------

// latency 记录一组耗时样本，输出分位数；并发安全。
type latency struct {
	mu      sync.Mutex
	samples []float64 // 毫秒
}

func (l *latency) add(d time.Duration) {
	l.mu.Lock()
	l.samples = append(l.samples, float64(d)/float64(time.Millisecond))
	l.mu.Unlock()
}

type summary struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

func (l *latency) summary() summary {
	l.mu.Lock()
	s := make([]float64, len(l.samples))
	copy(s, l.samples)
	l.mu.Unlock()
	sort.Float64s(s)
	out := summary{Count: len(s)}
	if len(s) == 0 {
		return out
	}
	quantile := func(q float64) float64 {
		idx := int(math.Ceil(q*float64(len(s)))) - 1
		if idx < 0 {
			idx = 0
		}
		return s[idx]
	}
	out.P50 = quantile(0.50)
	out.P90 = quantile(0.90)
	out.P99 = quantile(0.99)
	out.Max = s[len(s)-1]
	return out
}

// ---------- 计时 Store 包装器 ----------

// timedStore 记录 Save/Load 等阶段的耗时分布；RollbackBasisReader 原样透传，
// 语义与底层 Store 一致。
type timedStore struct {
	inner            state.Store
	save, load       *latency
	get, list        *latency
	del              *latency
	saveErr, loadErr atomic.Int64
}

func newTimedStore(inner state.Store) *timedStore {
	return &timedStore{inner: inner, save: &latency{}, load: &latency{}, get: &latency{}, list: &latency{}, del: &latency{}}
}

func (s *timedStore) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	start := time.Now()
	out, err := s.inner.Save(ctx, c)
	s.save.add(time.Since(start))
	if err != nil {
		s.saveErr.Add(1)
	}
	return out, err
}

func (s *timedStore) Load(ctx context.Context, id string) (state.Checkpoint, bool, error) {
	start := time.Now()
	out, ok, err := s.inner.Load(ctx, id)
	s.load.add(time.Since(start))
	if err != nil {
		s.loadErr.Add(1)
	}
	return out, ok, err
}

func (s *timedStore) Get(ctx context.Context, id, checkpointID string) (state.Checkpoint, bool, error) {
	start := time.Now()
	out, ok, err := s.inner.Get(ctx, id, checkpointID)
	s.get.add(time.Since(start))
	return out, ok, err
}

func (s *timedStore) List(ctx context.Context, id string, options state.ListOptions) ([]state.Checkpoint, error) {
	start := time.Now()
	out, err := s.inner.List(ctx, id, options)
	s.list.add(time.Since(start))
	return out, err
}

func (s *timedStore) Delete(ctx context.Context, id string) error {
	start := time.Now()
	err := s.inner.Delete(ctx, id)
	s.del.add(time.Since(start))
	return err
}

func (s *timedStore) RollbackBasis(ctx context.Context, id string) (state.Checkpoint, bool, error) {
	if r, ok := s.inner.(state.RollbackBasisReader); ok {
		return r.RollbackBasis(ctx, id)
	}
	return state.Checkpoint{}, false, errors.New("inner store does not implement RollbackBasisReader")
}

// ---------- 假模型与工具 ----------

var callSeq atomic.Int64

// delayModel 每次调用先按配置延迟睡眠（可被 ctx 取消），再按脚本响应：
// 末尾消息不是工具结果时发出 toolCalls 个工具调用，否则给最终答案。
// 每次调用的真实耗时写入 lats。
func delayModel(delay time.Duration, toolNames []string, lats *latency) llm.ChatModel {
	return llm.NewHandlerModel(func(ctx context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		start := time.Now()
		defer func() { lats.add(time.Since(start)) }()
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		if n := len(req.Messages); len(toolNames) > 0 && n > 0 && req.Messages[n-1].Role != schema.RoleTool {
			blocks := make([]schema.ContentBlock, 0, len(toolNames))
			for _, name := range toolNames {
				id := fmt.Sprintf("lt-call-%d", callSeq.Add(1))
				blocks = append(blocks, schema.NewFunctionToolCallBlock(id, name, `{}`))
			}
			return []schema.Message{schema.NewMessage(schema.RoleAssistant, blocks...)}, nil
		}
		return []schema.Message{schema.AssistantMessage("loadtest answer")}, nil
	})
}

func sleepTool(name string, delay time.Duration) tool.Tool {
	return tool.NewFunctionTool(tool.Spec{Name: name, IsReadOnly: true}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return tool.Result{}, ctx.Err()
			case <-timer.C:
			}
		}
		return tool.TextResult(name + " ok"), nil
	})
}

// ---------- 配置与结果 ----------

type config struct {
	mode        string
	sessions    int
	concurrency int
	requests    int
	contention  float64
	tools       int
	history     int
	delay       time.Duration
	toolDelay   time.Duration
	store       string
	redisAddr   string
	prefix      string
	keep        bool
	durability  string
	timeout     time.Duration
	pprofAddr   string
	cpuProfile  string
	memProfile  string
	probe       bool
	probeEvery  time.Duration
	jsonOut     string
}

type result struct {
	Config      config             `json:"config"`
	StartedAt   string             `json:"started_at"`
	WallSeconds float64            `json:"wall_seconds"`
	Seeded      int                `json:"seeded"`
	Requests    map[string]int64   `json:"requests"`
	Throughput  float64            `json:"requests_per_second"`
	HotOK       int64              `json:"hot_ok"`
	Latency     map[string]summary `json:"latency_ms"`
	Goroutines  map[string]int     `json:"goroutines"`
	Memory      map[string]any     `json:"memory"`
	Errors      map[string]string  `json:"errors,omitempty"`
}

func main() {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", "run", "run：压测；seed：只播种会话历史（量 logs 缓存内存）")
	flag.IntVar(&cfg.sessions, "sessions", 16, "会话数")
	flag.IntVar(&cfg.concurrency, "concurrency", 8, "并发 worker 数")
	flag.IntVar(&cfg.requests, "requests", 0, "测量的 Invoke 总次数；0 表示等于会话数")
	flag.Float64Var(&cfg.contention, "contention", 0, "同会话争用比例：该比例的请求集中打向会话 0")
	flag.IntVar(&cfg.tools, "tools", 2, "每轮并行工具调用数；0 表示单次模型调用直接回答")
	flag.IntVar(&cfg.history, "history", 0, "每会话预置消息数")
	flag.DurationVar(&cfg.delay, "delay", 2*time.Millisecond, "模型每次调用延迟")
	flag.DurationVar(&cfg.toolDelay, "tooldelay", time.Millisecond, "工具每次执行延迟")
	flag.StringVar(&cfg.store, "store", "memory", "检查点存储：none|memory|redis")
	flag.StringVar(&cfg.redisAddr, "redisaddr", "", "Redis 地址；默认取 GOHARNESS_BENCH_REDIS_ADDR，再退 localhost:6379")
	flag.StringVar(&cfg.prefix, "prefix", "", "Redis 键前缀；默认 goharness:loadtest:<pid>，结束清理")
	flag.BoolVar(&cfg.keep, "keep", false, "保留 Redis 键不清理")
	flag.StringVar(&cfg.durability, "durability", "async", "检查点持久化模式：async|sync")
	flag.DurationVar(&cfg.timeout, "timeout", 3*time.Minute, "整体超时")
	flag.StringVar(&cfg.pprofAddr, "pprofaddr", "", "pprof 监听地址，如 127.0.0.1:6060")
	flag.StringVar(&cfg.cpuProfile, "cpuprofile", "", "把测量窗口的 CPU profile 写到文件")
	flag.StringVar(&cfg.memProfile, "memprofile", "", "结束后把堆 profile 写到文件")
	flag.BoolVar(&cfg.probe, "probe", false, "运行期间每 probeevery 探测一次 Redis PING 延迟（头端阻塞证据）")
	flag.DurationVar(&cfg.probeEvery, "probeevery", 10*time.Millisecond, "Redis 探测间隔")
	flag.StringVar(&cfg.jsonOut, "json", "", "结果 JSON 输出路径；空则只打文本")
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	if cfg.sessions < 1 || cfg.concurrency < 1 || cfg.history < 0 {
		return errors.New("sessions/concurrency 必须为正，history 不能为负")
	}
	if cfg.requests == 0 {
		cfg.requests = cfg.sessions
	}
	if cfg.requests < 1 || cfg.contention < 0 || cfg.contention > 1 {
		return errors.New("requests 必须为正，contention 取值 0 到 1")
	}
	if cfg.mode != "run" && cfg.mode != "seed" {
		return fmt.Errorf("未知 mode %q", cfg.mode)
	}
	durability := graph.DurabilityAsync
	if cfg.durability == "sync" {
		durability = graph.DurabilitySync
	} else if cfg.durability != "async" {
		return fmt.Errorf("未知 durability %q", cfg.durability)
	}

	// 存储层：none 不带检查点；memory 进程内；redis 真实实例。
	var store state.Store
	var client *redis.Client
	redisAddr := cfg.redisAddr
	if redisAddr == "" {
		redisAddr = strings.TrimSpace(os.Getenv("GOHARNESS_BENCH_REDIS_ADDR"))
	}
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	prefix := cfg.prefix
	if prefix == "" {
		prefix = fmt.Sprintf("goharness:loadtest:%d", os.Getpid())
	}
	switch cfg.store {
	case "none":
	case "memory":
		store = state.NewMemoryStore()
	case "redis":
		client = redis.NewClient(&redis.Options{Addr: redisAddr})
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := client.Ping(pingCtx).Err(); err != nil {
			cancel()
			return fmt.Errorf("redis %s 不可达: %w", redisAddr, err)
		}
		cancel()
		redisStore, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client, Prefix: prefix, TTL: 10 * time.Minute})
		if err != nil {
			return err
		}
		store = redisStore
	default:
		return fmt.Errorf("未知 store %q", cfg.store)
	}
	if client != nil {
		defer client.Close()
		if !cfg.keep {
			defer cleanupKeys(client, prefix)
		}
	}
	timed := newTimedStore(store)

	// pprof 在线端点。
	if cfg.pprofAddr != "" {
		go func() { _ = http.ListenAndServe(cfg.pprofAddr, nil) }()
		fmt.Fprintf(os.Stderr, "pprof: http://%s/debug/pprof/\n", cfg.pprofAddr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	conversations := make([]string, cfg.sessions)
	for i := range conversations {
		conversations[i] = fmt.Sprintf("lt-%d-%d", os.Getpid(), i)
	}

	res := &result{
		Config:     cfg,
		StartedAt:  time.Now().Format(time.RFC3339),
		Requests:   map[string]int64{},
		Latency:    map[string]summary{},
		Goroutines: map[string]int{},
		Memory:     map[string]any{},
	}
	e2e, modelLat := &latency{}, &latency{}

	// 协程与内存基线：GC 后取样，让对比只含本次运行保留的部分。
	runtime.GC()
	var mem0 runtime.MemStats
	runtime.ReadMemStats(&mem0)
	res.Goroutines["start"] = runtime.NumGoroutine()

	// ---- 播种历史 ----
	seedLat := &latency{}
	if cfg.history > 0 {
		jobs := make(chan int, cfg.sessions)
		var seedWG sync.WaitGroup
		seedWorkers := cfg.concurrency
		for w := 0; w < seedWorkers; w++ {
			seedWG.Add(1)
			go func() {
				defer seedWG.Done()
				for i := range jobs {
					if timed.inner == nil {
						continue
					}
					seedLat.add(seedConversation(ctx, timed, conversations[i], cfg.history))
				}
			}()
		}
		for i := range conversations {
			jobs <- i
		}
		close(jobs)
		seedWG.Wait()
		res.Seeded = len(conversations) - int(seedErr.Load())
		res.Latency["seed.save"] = seedLat.summary()
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("播种被中止: %w", err)
		}
	}
	if cfg.mode == "seed" {
		// 只播种：报告内存增量后即结束，不跑请求。
		finishMemory(res, mem0, cfg)
		res.Latency["store.save"] = timed.save.summary()
		report(res, cfg)
		return writeJSON(res, cfg)
	}

	// ---- 请求计划：同会话争用的请求排在最前，保证并发交叠 ----
	hot := int(math.Round(float64(cfg.requests) * cfg.contention))
	plans := make([]string, cfg.requests)
	for i := 0; i < cfg.requests; i++ {
		if i < hot || cfg.sessions == 1 {
			plans[i] = conversations[0]
		} else {
			plans[i] = conversations[1+i%(cfg.sessions-1)]
		}
	}

	// ---- Agent ----
	var tools []tool.Tool
	var toolNames []string
	for i := 0; i < cfg.tools; i++ {
		name := fmt.Sprintf("lt_tool_%d", i)
		tools = append(tools, sleepTool(name, cfg.toolDelay))
		toolNames = append(toolNames, name)
	}
	model := delayModel(cfg.delay, toolNames, modelLat)
	options := []agent.Option{agent.WithName("loadtest"), agent.WithMaxLLMCalls(int32Max(4, cfg.tools+2))}
	if timed.inner != nil {
		options = append(options, agent.WithCheckpointStore(timed), agent.WithCheckpointDurability(durability))
	}
	assistant, err := agent.New(model, append(options, agent.WithTools(tools...))...)
	if err != nil {
		return err
	}

	// ---- 测量窗口 ----
	if cfg.cpuProfile != "" {
		f, err := os.Create(cfg.cpuProfile)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer f.Close()
		defer pprof.StopCPUProfile()
	}

	var probeLat *latency
	stopProbe := make(chan struct{})
	if cfg.probe && client != nil {
		probeLat = &latency{}
		go redisProbe(client, probeLat, cfg.probeEvery, stopProbe)
		defer close(stopProbe)
	}

	jobs := make(chan int, cfg.requests)
	var okCount, conflictCount, unfinishedCount, canceledCount, otherCount atomic.Int64
	var hotOK atomic.Int64
	errSamples := sync.Map{}
	var wg sync.WaitGroup
	startWall := time.Now()
	for w := 0; w < cfg.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				start := time.Now()
				_, err := assistant.Invoke(ctx, []schema.Message{schema.UserMessage(fmt.Sprintf("loadtest request %d", i))},
					agent.WithConversationID(plans[i]))
				e2e.add(time.Since(start))
				switch {
				case err == nil:
					okCount.Add(1)
					if i < hot {
						hotOK.Add(1)
					}
				case errors.Is(err, state.ErrConflict):
					conflictCount.Add(1)
				case errors.Is(err, state.ErrUnfinishedRun):
					unfinishedCount.Add(1)
				case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
					canceledCount.Add(1)
				default:
					otherCount.Add(1)
					errSamples.LoadOrStore(fmt.Sprintf("%T: %v", err, err), true)
				}
			}
		}()
	}
	for i := 0; i < cfg.requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	wall := time.Since(startWall)

	res.WallSeconds = wall.Seconds()
	res.Requests["ok"] = okCount.Load()
	res.Requests["conflict"] = conflictCount.Load()
	res.Requests["unfinished"] = unfinishedCount.Load()
	res.Requests["canceled"] = canceledCount.Load()
	res.Requests["other"] = otherCount.Load()
	res.HotOK = hotOK.Load()
	res.Throughput = float64(okCount.Load()) / wall.Seconds()
	res.Latency["e2e"] = e2e.summary()
	res.Latency["model"] = modelLat.summary()
	if timed.inner != nil {
		res.Latency["store.save"] = timed.save.summary()
		res.Latency["store.load"] = timed.load.summary()
	}
	if probeLat != nil {
		res.Latency["redis.ping"] = probeLat.summary()
	}
	errSamples.Range(func(k, _ any) bool {
		if res.Errors == nil {
			res.Errors = map[string]string{}
		}
		res.Errors[fmt.Sprint(k)] = "seen"
		return true
	})

	// 协程回落与内存收尾：稍等异步持久化与流收尾退出。
	time.Sleep(300 * time.Millisecond)
	res.Goroutines["end"] = runtime.NumGoroutine()
	finishMemory(res, mem0, cfg)
	if cfg.memProfile != "" {
		f, err := os.Create(cfg.memProfile)
		if err != nil {
			return err
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			return err
		}
		f.Close()
	}

	report(res, cfg)
	return writeJSON(res, cfg)
}

var seedErr atomic.Int64

// seedConversation 直接向 Store 写一份已完成会话，消息为 user/assistant
// 交替的短文本；返回本次保存耗时。
func seedConversation(ctx context.Context, store *timedStore, conversation string, history int) time.Duration {
	messages := make([]schema.Message, 0, history)
	for i := 0; i < history; i++ {
		if i%2 == 0 {
			messages = append(messages, schema.UserMessage(fmt.Sprintf("seed question %d for %s", i, conversation)))
		} else {
			messages = append(messages, schema.AssistantMessage(fmt.Sprintf("seed answer %d", i)))
		}
	}
	checkpoint := state.Checkpoint{
		ConversationID: conversation,
		Status:         state.StatusCompleted,
		SchemaVersion:  state.CurrentCheckpointSchemaVersion,
		Messages:       messages,
		Values:         map[string]any{},
	}
	start := time.Now()
	_, err := store.Save(ctx, checkpoint)
	if err != nil {
		seedErr.Add(1)
	}
	return time.Since(start)
}

// redisProbe 在测量窗口内周期性 PING：保存脚本若在服务端串行点阻塞，
// PING 的尾延时会直接表现出来。
func redisProbe(client *redis.Client, lats *latency, every time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			start := time.Now()
			_ = client.Ping(ctx).Err()
			lats.add(time.Since(start))
		}
	}
}

// cleanupKeys 删除本次运行前缀下的全部键。
func cleanupKeys(client *redis.Client, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	iter := client.Scan(ctx, 0, prefix+":*", 500).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= 1000 {
			_ = client.Del(ctx, keys...).Err()
			keys = keys[:0]
		}
	}
	if len(keys) > 0 {
		_ = client.Del(ctx, keys...).Err()
	}
}

func finishMemory(res *result, mem0 runtime.MemStats, cfg config) {
	debug.FreeOSMemory()
	var mem1 runtime.MemStats
	runtime.ReadMemStats(&mem1)
	res.Memory["heap_alloc_start_bytes"] = mem0.HeapAlloc
	res.Memory["heap_alloc_end_bytes"] = mem1.HeapAlloc
	res.Memory["heap_alloc_delta_bytes"] = int64(mem1.HeapAlloc) - int64(mem0.HeapAlloc)
	res.Memory["heap_inuse_end_bytes"] = mem1.HeapInuse
	res.Memory["total_alloc_delta_bytes"] = int64(mem1.TotalAlloc) - int64(mem0.TotalAlloc)
	if cfg.sessions > 0 {
		res.Memory["retained_bytes_per_session"] = (int64(mem1.HeapAlloc) - int64(mem0.HeapAlloc)) / int64(cfg.sessions)
	}
}

func report(res *result, cfg config) {
	fmt.Println("=== loadtest ===")
	c := res.Config
	fmt.Printf("mode=%s sessions=%d concurrency=%d requests=%d contention=%.2f tools=%d history=%d delay=%s tooldelay=%s store=%s durability=%s\n",
		c.mode, c.sessions, c.concurrency, c.requests, c.contention, c.tools, c.history, c.delay, c.toolDelay, c.store, c.durability)
	if res.Seeded > 0 {
		fmt.Printf("seeded=%d save %s\n", res.Seeded, formatSummary(res.Latency["seed.save"]))
	}
	if res.WallSeconds > 0 {
		fmt.Printf("wall=%.2fs throughput=%.1f req/s  ok=%d conflict=%d unfinished=%d canceled=%d other=%d hot_ok=%d\n",
			res.WallSeconds, res.Throughput, res.Requests["ok"], res.Requests["conflict"], res.Requests["unfinished"], res.Requests["canceled"], res.Requests["other"], res.HotOK)
	}
	names := make([]string, 0, len(res.Latency))
	for name := range res.Latency {
		if name != "seed.save" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf("%-12s %s\n", name, formatSummary(res.Latency[name]))
	}
	fmt.Printf("goroutines start=%d end=%d\n", res.Goroutines["start"], res.Goroutines["end"])
	fmt.Printf("memory heap_alloc_delta=%d bytes total_alloc_delta=%d bytes retained/session=%d bytes\n",
		res.Memory["heap_alloc_delta_bytes"], res.Memory["total_alloc_delta_bytes"], res.Memory["retained_bytes_per_session"])
	for sample := range res.Errors {
		fmt.Println("other error sample:", sample)
	}
}

func formatSummary(s summary) string {
	if s.Count == 0 {
		return "n=0"
	}
	return fmt.Sprintf("n=%d p50=%.2fms p90=%.2fms p99=%.2fms max=%.2fms", s.Count, s.P50, s.P90, s.P99, s.Max)
}

func writeJSON(res *result, cfg config) error {
	if cfg.jsonOut == "" {
		return nil
	}
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cfg.jsonOut, append(data, '\n'), 0o644)
}

// int32Max 避免引入 math 的整型转换噪音。
func int32Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
