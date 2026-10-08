package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// --- 第四轮复核：收尾提交必须保存末节点 State 与会话 ---

// tailCommitState 检查点解码：执行树根 frame 的 State.Data 中是外部化后的
// convState 线格式（history 被 history_len 引用代替）。
func tailCommitState(t *testing.T, execution json.RawMessage) (out string, historyLen int) {
	t.Helper()
	var root struct {
		State graph.GraphStateCheckpoint `json:"state"`
	}
	if err := json.Unmarshal(execution, &root); err != nil {
		t.Fatalf("decode execution: %v", err)
	}
	var wire struct {
		Out        string           `json:"out"`
		HistoryLen int              `json:"history_len"`
		History    []schema.Message `json:"history"`
	}
	if err := json.Unmarshal(root.State.Data, &wire); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if wire.HistoryLen == 0 {
		historyLen = len(wire.History)
	} else {
		historyLen = wire.HistoryLen
	}
	return wire.Out, historyLen
}

func mustLoadCheckpoint(t *testing.T, store *state.MemoryStore, key string) state.Checkpoint {
	t.Helper()
	rec, found, err := store.Load(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("load checkpoint %q: found=%v err=%v", key, found, err)
	}
	return rec
}

func assertTailCheckpoint(t *testing.T, store *state.MemoryStore, key string) {
	t.Helper()
	rec := mustLoadCheckpoint(t, store, key)
	if rec.Status != state.StatusCompleted {
		t.Fatalf("status=%q", rec.Status)
	}
	var result string
	if err := json.Unmarshal(rec.Result, &result); err != nil || result != "final-answer" {
		t.Fatalf("result=%s err=%v", rec.Result, err)
	}
	// 会话：根检查点 Messages 必须含末节点 PostHandler 追加的 assistant 消息。
	if len(rec.Messages) != 2 || rec.Messages[1].Role != schema.RoleAssistant {
		t.Fatalf("messages=%+v", rec.Messages)
	}
	// 业务 State：执行树里的快照必须是 PostHandler 提交后的值。
	out, historyLen := tailCommitState(t, rec.Execution)
	if out != "updated" || historyLen != 2 {
		t.Fatalf("checkpoint state out=%q historyLen=%d", out, historyLen)
	}
}

// 复现：末节点 PostHandler 更新业务 State 并追加最终会话消息；图到 End 时
// 受管子任务尚未收尾，根等待其收敛后直接完成。检查点必须保存最新 State、
// 会话与结果，不能停留在末节点提交前的旧现场。sync/async 各验一次。
func TestReviewManagedTailCommitCapturesFinalState(t *testing.T) {
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			store := state.NewMemoryStore()
			release := make(chan struct{})
			var postCalls atomic.Int32

			g := graph.NewGraph[string, string]("tail-commit")
			if err := g.SetStateFactory(func(_ context.Context, input string) (*convState, error) {
				return &convState{History: []schema.Message{schema.UserMessage(input)}}, nil
			}); err != nil {
				t.Fatal(err)
			}
			mainNode := runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) {
				if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{
					Builder: "child",
					Version: "v1",
					Input:   json.RawMessage(`"go"`),
				}); err != nil {
					return "", err
				}
				return "final-answer", nil
			})
			if err := g.AddNode("main", mainNode,
				graph.WithPreHandler(func(_ context.Context, s *convState) (string, error) {
					return "hi", nil
				}),
				graph.WithPostHandler(func(_ context.Context, s *convState, output string) error {
					s.Out = "updated"
					s.History = append(s.History, schema.AssistantMessage(output))
					postCalls.Add(1)
					return nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			if err := g.AddEdge(graph.Start, "main"); err != nil {
				t.Fatal(err)
			}
			if err := g.AddEdge("main", graph.End); err != nil {
				t.Fatal(err)
			}
			compiled := mustCompile(t, g)
			builder := func(_ context.Context, _ graph.ManagedSpec) (graph.ManagedRunner, error) {
				return func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return json.Marshal("child-done")
				}, nil
			}
			opts := func() []runnable.Option {
				return []runnable.Option{
					graph.WithCheckpoint(store, "tail"),
					graph.WithDurability(mode),
					graph.WithManagedBuilder("child", "v1", builder),
				}
			}
			done := make(chan error, 1)
			var out string
			go func() {
				var err error
				out, err = compiled.Invoke(context.Background(), "q", opts()...)
				done <- err
			}()
			// 子任务登记为 running 且根已进入 End 收敛等待时放行。
			deadline := time.Now().Add(5 * time.Second)
			for {
				if rec, found, _ := store.Load(context.Background(), "tail"); found && rec.Execution != nil {
					statuses := managedTreeStatuses(t, rec)
					running := false
					for _, s := range statuses {
						if len(s) >= 7 && s[:7] == "running" {
							running = true
						}
					}
					if running {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("managed task never reached running")
				}
				time.Sleep(10 * time.Millisecond)
			}
			close(release)
			if invokeErr := <-done; invokeErr != nil {
				t.Fatalf("invoke: %v", invokeErr)
			}
			if out != "final-answer" {
				t.Fatalf("output=%q", out)
			}
			assertTailCheckpoint(t, store, "tail")
			if postCalls.Load() != 1 {
				t.Fatalf("post calls=%d", postCalls.Load())
			}
		})
	}
}

// 审批挂起后恢复的收尾路径：子任务处于审批挂起，根在 End 等待；回答审批
// 并 Resume 完成时，检查点同样必须含末节点提交后的 State 与会话，且末节点
// 不重跑。
func TestReviewManagedTailCommitAfterApprovalResume(t *testing.T) {
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			store := state.NewMemoryStore()
			var postCalls atomic.Int32

			childGraph := graph.NewGraph[string, string]("tail-child")
			if err := childGraph.SetStateFactory(func(_ context.Context, input string) (*managedState, error) {
				return &managedState{In: input}, nil
			}); err != nil {
				t.Fatal(err)
			}
			ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
				if _, err := graph.Interrupt(ctx, "approve?"); err != nil {
					return "", err
				}
				return "done", nil
			})
			if err := childGraph.AddNode("ask", ask,
				graph.WithPreHandler(func(_ context.Context, s *managedState) (string, error) { return s.In, nil }),
			); err != nil {
				t.Fatal(err)
			}
			if err := childGraph.AddEdge(graph.Start, "ask"); err != nil {
				t.Fatal(err)
			}
			if err := childGraph.AddEdge("ask", graph.End); err != nil {
				t.Fatal(err)
			}
			childCompiled := mustCompile(t, childGraph)

			g := graph.NewGraph[string, string]("tail-approval")
			if err := g.SetStateFactory(func(_ context.Context, input string) (*convState, error) {
				return &convState{History: []schema.Message{schema.UserMessage(input)}}, nil
			}); err != nil {
				t.Fatal(err)
			}
			mainNode := runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) {
				if _, err := graph.SpawnManaged(ctx, graph.ManagedSpec{
					Builder: "child",
					Version: "v1",
					Input:   json.RawMessage(`"go"`),
				}); err != nil {
					return "", err
				}
				return "final-answer", nil
			})
			if err := g.AddNode("main", mainNode,
				graph.WithPreHandler(func(_ context.Context, s *convState) (string, error) {
					return "hi", nil
				}),
				graph.WithPostHandler(func(_ context.Context, s *convState, output string) error {
					s.Out = "updated"
					s.History = append(s.History, schema.AssistantMessage(output))
					postCalls.Add(1)
					return nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			if err := g.AddEdge(graph.Start, "main"); err != nil {
				t.Fatal(err)
			}
			if err := g.AddEdge("main", graph.End); err != nil {
				t.Fatal(err)
			}
			compiled := mustCompile(t, g)
			builder := func(_ context.Context, _ graph.ManagedSpec) (graph.ManagedRunner, error) {
				return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					out, err := childCompiled.Invoke(ctx, "task")
					if err != nil {
						return nil, err
					}
					return json.Marshal(out)
				}, nil
			}
			opts := func() []runnable.Option {
				return []runnable.Option{
					graph.WithCheckpoint(store, "tail"),
					graph.WithDurability(mode),
					graph.WithManagedBuilder("child", "v1", builder),
				}
			}
			_, err := compiled.Invoke(context.Background(), "q", opts()...)
			var interrupted *graph.InterruptedError
			if !errors.As(err, &interrupted) {
				t.Fatalf("want InterruptedError, got %v", err)
			}
			pending := interrupted.PendingInterrupts()
			if len(pending) != 1 {
				t.Fatalf("pending=%+v", pending)
			}
			out, err := compiled.Resume(context.Background(), append(opts(),
				graph.WithResume(pending[0].ID, "ok"))...)
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			if out != "final-answer" {
				t.Fatalf("output=%q", out)
			}
			assertTailCheckpoint(t, store, "tail")
			if postCalls.Load() != 1 {
				t.Fatalf("post calls=%d", postCalls.Load())
			}
		})
	}
}
