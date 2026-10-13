// 参考服务：标准库 net/http + SSE，把 goHarness 组件按推荐顺序组装。
// 只做演示，不是官方服务端。组装顺序（具体方案 7.2）：
//
//	认证（示例 token）→ tenant.With → tenant.NewStore 装饰 → agent →
//	limits.Bulkhead → SSE 输出（SendTimeout 保护）
//
//	POST /v1/sessions/{sid}/runs    {"message": "..."} 运行；Accept: text/event-stream 时 SSE
//	POST /v1/sessions/{sid}/resume  {"question_id": "...", "answer": "..."} 恢复挂起
//	DELETE /v1/sessions/{sid}       删除会话
//	GET  /healthz /readyz           存活与就绪（排空中 readyz 失败）
//
// 模型是本地脚本模型，无需外部服务：回显输入；输入含 "approve" 时先经
// ask_approval 工具发起审批挂起，演示 /resume 路径。默认内存存储；
// 设 GOHARNESS_REDIS_ADDR=127.0.0.1:6379 切到真实 Redis。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goharness/goharness/extensions/limits"
	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/stream"
	"github.com/goharness/goharness/pkg/tool"
)

// demoModel 回显输入；输入含 "approve" 时先调用 ask_approval 发起审批。
func demoModel() llm.ChatModel {
	return llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		last := req.Messages[len(req.Messages)-1]
		for _, r := range last.FunctionToolResults() {
			for _, b := range r.ContentBlocks {
				if b.Text != nil {
					return []schema.Message{schema.AssistantMessage("approved -> " + b.Text.Text)}, nil
				}
			}
		}
		if strings.Contains(last.JoinedText(""), "approve") {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-1", "ask_approval", `{"question":"允许执行吗？"}`))}, nil
		}
		return []schema.Message{schema.AssistantMessage("echo: " + last.JoinedText(""))}, nil
	})
}

// askApproval 发起审批挂起：answer 恢复后原样返回。
func askApproval() tool.Tool {
	return tool.NewFunctionTool(tool.Spec{Name: "ask_approval"}, func(ctx context.Context, in tool.CallInput) (tool.Result, error) {
		question := "允许执行吗？"
		if q, ok := in.Args["question"].(string); ok && q != "" {
			question = q
		}
		answer, err := graph.Interrupt(ctx, question)
		if err != nil {
			return tool.Result{}, err
		}
		return tool.TextResult(fmt.Sprintf("%v", answer)), nil
	})
}

// resumeKey 标记本次舱壁调用走恢复路径（见 next 分派）。
type resumeKey struct{}

// server 持有组装好的依赖。
type server struct {
	store   state.Store // 已是租户装饰后的 Store
	bulk    *limits.Bulkhead[[]schema.Message, []schema.Message]
	drain   atomic.Bool
	started time.Time
}

// subjectOf 解析示例认证头：Bearer <tenant>/<user>。
// 真实部署换成 JWT 校验后提取同样的字段。
func subjectOf(r *http.Request) (tool.Subject, error) {
	auth := r.Header.Get("Authorization")
	parts := strings.SplitN(strings.TrimPrefix(auth, "Bearer "), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return tool.Subject{}, errors.New("expected Authorization: Bearer <tenant>/<user>")
	}
	return tool.Subject{ID: parts[1], TenantID: parts[0]}, nil
}

// scoped 把请求 ctx 限定到租户作用域；失败返回是否已拒绝。
func scoped(w http.ResponseWriter, r *http.Request) (context.Context, bool) {
	subject, err := subjectOf(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return nil, false
	}
	ctx, err := tenant.With(r.Context(), subject)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return nil, false
	}
	return ctx, true
}

func (s *server) sessionKey(ctx context.Context, r *http.Request) (string, bool) {
	// r.PathValue 的 sid 由 mux 保证非空。
	key, err := tenant.SessionKey(ctx, r.PathValue("sid"))
	if err != nil {
		return "", false
	}
	return key, true
}

func (s *server) run(w http.ResponseWriter, r *http.Request) {
	ctx, ok := scoped(w, r)
	if !ok {
		return
	}
	if s.drain.Load() {
		writeError(w, http.StatusServiceUnavailable, "server is draining")
		return
	}
	var body struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Message) == "" {
		writeError(w, http.StatusBadRequest, `expected {"message": "..."}`)
		return
	}
	key, ok := s.sessionKey(ctx, r)
	if !ok {
		writeError(w, http.StatusForbidden, "session key outside tenant scope")
		return
	}
	input := []schema.Message{schema.UserMessage(body.Message)}
	opts := []runnable.Option{agent.WithCheckpoint(s.store, key)}

	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		s.runSSE(w, r, ctx, input, opts)
		return
	}
	output, err := s.bulk.Invoke(ctx, input, opts...)
	writeRunResult(w, output, err)
}

// runSSE 用 stream.Run 开公共流作用域：舱壁名额在整个运行期间持有，
// 事件逐条写出；SendTimeout 保护慢消费者不拖垮协程。
func (s *server) runSSE(w http.ResponseWriter, r *http.Request, ctx context.Context, input []schema.Message, opts []runnable.Option) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	reader, err := stream.Run(ctx, func(run context.Context) ([]schema.Message, error) {
		return s.bulk.Invoke(run, input, opts...)
	}, stream.Config{Buffer: 128, SendTimeout: 3 * time.Second})
	if err != nil {
		fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
		if flusher != nil {
			flusher.Flush()
		}
		return
	}
	defer reader.Close()
	for {
		event, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			if errors.Is(err, stream.ErrSlowConsumer) {
				log.Printf("slow consumer disconnected: %v", err)
			}
			fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		data, _ := json.Marshal(event)
		fmt.Fprintf(w, "data: %s\n\n", data)
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (s *server) resume(w http.ResponseWriter, r *http.Request) {
	ctx, ok := scoped(w, r)
	if !ok {
		return
	}
	var body struct {
		QuestionID string `json:"question_id"`
		Answer     string `json:"answer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.QuestionID == "" {
		writeError(w, http.StatusBadRequest, `expected {"question_id": "...", "answer": "..."}`)
		return
	}
	key, ok := s.sessionKey(ctx, r)
	if !ok {
		writeError(w, http.StatusForbidden, "session key outside tenant scope")
		return
	}
	// 恢复同样过舱壁：Resume 是一次完整运行。ctx 标记让舱壁内的分派
	// Runnable 走 agent.Resume 而不是新运行。
	out, err := s.bulk.Invoke(context.WithValue(ctx, resumeKey{}, true), nil,
		agent.WithCheckpoint(s.store, key),
		agent.WithResume(body.QuestionID, body.Answer))
	writeRunResult(w, out, err)
}

func (s *server) deleteSession(w http.ResponseWriter, r *http.Request) {
	ctx, ok := scoped(w, r)
	if !ok {
		return
	}
	key, ok := s.sessionKey(ctx, r)
	if !ok {
		writeError(w, http.StatusForbidden, "session key outside tenant scope")
		return
	}
	if err := s.store.Delete(ctx, key); err != nil {
		writeRunResult(w, nil, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeRunResult 把框架错误映射为 HTTP 状态码（具体方案 7.3）。
func writeRunResult(w http.ResponseWriter, output []schema.Message, err error) {
	var interrupted *graph.InterruptedError
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"output": lastText(output)})
	case errors.As(err, &interrupted):
		pending := interrupted.PendingInterrupts()
		questions := make([]map[string]any, 0, len(pending))
		for _, p := range pending {
			questions = append(questions, map[string]any{
				"question_id": p.ID, "question": json.RawMessage(p.Payload),
			})
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "suspended", "questions": questions})
	case errors.Is(err, tenant.ErrNoScope), errors.Is(err, tenant.ErrInvalidID):
		writeError(w, http.StatusUnauthorized, err.Error())
	case errors.Is(err, tenant.ErrTenantMismatch):
		writeError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, state.ErrConflict):
		writeError(w, http.StatusConflict, "checkpoint version conflict; retry")
	case errors.Is(err, state.ErrUnfinishedRun):
		writeError(w, http.StatusConflict, "session has an unfinished run; resume it or abandon")
	case errors.Is(err, graph.ErrRunAbandoned):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, limits.ErrOverloaded):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, limits.ErrDraining):
		writeError(w, http.StatusServiceUnavailable, "server is draining")
	case errors.Is(err, graph.ErrCheckpointRequired):
		writeError(w, http.StatusInternalServerError, "run requires checkpoint storage")
	case errors.Is(err, context.Canceled):
		// 客户端已断开，无需响应。
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func lastText(output []schema.Message) string {
	for i := len(output) - 1; i >= 0; i-- {
		if text := output[i].JoinedText(""); text != "" {
			return text
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

func buildStore() (state.Store, func(), error) {
	addr := os.Getenv("GOHARNESS_REDIS_ADDR")
	if addr == "" {
		return state.NewMemoryStore(), func() {}, nil
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, nil, fmt.Errorf("redis at %s: %w", addr, err)
	}
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	if err != nil {
		return nil, nil, err
	}
	return store, func() { _ = client.Close() }, nil
}

func main() {
	inner, cleanup, err := buildStore()
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()
	// 租户装饰器：Store 只接受本租户作用域内的会话 key。
	store := tenant.NewStore(inner, tenant.StrictUser())

	assistant, err := agent.New(demoModel(),
		agent.WithName("assistant"),
		agent.WithTools(askApproval()),
		agent.WithCheckpointStore(store),
	)
	if err != nil {
		log.Fatal(err)
	}
	// 舱壁只认识 Runnable：分派包装按 ctx 标记把请求路由到
	// Invoke（新运行）或 agent.Resume（恢复），两个入口共享同一组名额。
	next := runnable.Func[[]schema.Message, []schema.Message](
		func(ctx context.Context, input []schema.Message, opts ...runnable.Option) ([]schema.Message, error) {
			if resume, _ := ctx.Value(resumeKey{}).(bool); resume {
				return agent.Resume(ctx, assistant, opts...)
			}
			return assistant.Invoke(ctx, input, opts...)
		})
	bulk, err := limits.NewBulkhead[[]schema.Message, []schema.Message](next,
		limits.BulkheadConfig{MaxConcurrent: 16, MaxQueue: 64, QueueTimeout: 10 * time.Second, PerTenant: 4})
	if err != nil {
		log.Fatal(err)
	}

	srv := &server{store: store, bulk: bulk, started: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions/{sid}/runs", srv.run)
	mux.HandleFunc("POST /v1/sessions/{sid}/resume", srv.resume)
	mux.HandleFunc("DELETE /v1/sessions/{sid}", srv.deleteSession)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if srv.drain.Load() {
			writeError(w, http.StatusServiceUnavailable, "draining")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
	})

	// 观测：Slog 调用树打到 stderr；接入 OTel 时在请求 ctx 上再挂
	// otel.Observer 与 otel.Metrics（见 docs/运维指标与告警.md）。
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: observeSlog(mux),
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		srv.drain.Store(true) // readyz 先失败，负载均衡摘流
		log.Println("draining: interrupting in-flight runs (progress is checkpointed)")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Drain 用 RequestInterrupt 取消在途运行并等其落盘；进度可由
		// 另一实例 Resume。
		if err := bulk.Drain(ctx, "shutdown"); err != nil {
			log.Printf("drain: %v", err)
		}
		_ = httpServer.Shutdown(ctx)
	}()

	log.Printf("listening on %s (demo auth: -H 'Authorization: Bearer <tenant>/<user>')", httpServer.Addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// observeSlog 给每个请求挂一次 Slog 调用树观察者；真实部署换成
// otel.With + otel.Metrics 的组合，或两者并列。
func observeSlog(next http.Handler) http.Handler {
	logger := observe.Slog(nil)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			r = r.WithContext(observe.With(r.Context(), logger))
		}
		next.ServeHTTP(w, r)
	})
}
