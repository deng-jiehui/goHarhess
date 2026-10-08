package subagent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/subagent"
)

// --- 阶段 3 第二轮复核：构建失败的错误类别与脱敏 ---

// TestReviewMixedBuilderErrors（32.2）：构建器把配置错误与包装后的框架错误
// 用 errors.Join 合并返回——两种合并顺序下框架错误都必须整体上交，不能被
// 配置错误分支提前转成普通工具结果；根 Invoke 返回错误且保留原错误链，
// 检查点不得标为成功完成。
func TestReviewMixedBuilderErrors(t *testing.T) {
	for _, mode := range durabilityModes {
		t.Run(string(mode), func(t *testing.T) {
			joined := []struct {
				name string
				err  error
			}{
				{"config first", errors.Join(
					&subagent.ConfigError{Message: "role requires another tool"},
					fmt.Errorf("sub graph check: %w", graph.ErrResumeIncompatible),
				)},
				{"framework first", errors.Join(
					fmt.Errorf("sub graph check: %w", graph.ErrResumeIncompatible),
					&subagent.ConfigError{Message: "role requires another tool"},
				)},
			}
			for _, tc := range joined {
				t.Run(tc.name, func(t *testing.T) {
					store := state.NewMemoryStore()
					cfg := customConfig(func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
						return nil, tc.err
					})
					m := newManager(t, cfg)
					_, err := newParent(t, m, &parentRun{}, spawnStep(map[string]any{"task": "t"})).
						Invoke(context.Background(), []schema.Message{schema.UserMessage("go")},
							runOpts(m, store, "conv", agent.WithDurability(mode))...)
					if err == nil || !errors.Is(err, graph.ErrResumeIncompatible) {
						t.Fatalf("err=%v, want the framework error preserved", err)
					}
					if record, _, _ := store.Load(context.Background(), "conv"); record.Status == state.StatusCompleted {
						t.Fatal("root run was recorded as completed")
					}
					if got := agentRecordsOrNone(t, store, "conv"); len(got) != 0 {
						t.Fatalf("rejected creation left sub-agents: %+v", got)
					}
				})
			}
		})
	}
}

// TestReviewConfigFeedbackSanitized（32.3）：配置错误被外层包装或与其他错误
// 合并时，模型可见文本只能是 ConfigError 自己声明的 Message——外层诊断里的
// 内部标记不外发；完整原错误留在错误链中。根运行继续，不留下子 Agent。
func TestReviewConfigFeedbackSanitized(t *testing.T) {
	for _, mode := range durabilityModes {
		t.Run(string(mode), func(t *testing.T) {
			wrapped := []struct {
				name string
				err  error
			}{
				{"wrapped config", fmt.Errorf(
					"loading private config private-role-secret: %w",
					&subagent.ConfigError{Message: "role requires another tool"},
				)},
				{"joined config", errors.Join(
					errors.New("private-role-secret diagnostic"),
					&subagent.ConfigError{Message: "role requires another tool"},
				)},
			}
			for _, tc := range wrapped {
				t.Run(tc.name, func(t *testing.T) {
					store := state.NewMemoryStore()
					cfg := customConfig(func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) {
						return nil, tc.err
					})
					m := newManager(t, cfg)
					run := &parentRun{}
					_, err := newParent(t, m, run, spawnStep(map[string]any{"task": "t"})).
						Invoke(context.Background(), []schema.Message{schema.UserMessage("go")},
							runOpts(m, store, "conv", agent.WithDurability(mode))...)
					if err != nil {
						t.Fatalf("invoke: %v", err)
					}
					seen := run.seen()
					if len(seen) != 1 {
						t.Fatalf("results=%+v", seen)
					}
					if !strings.Contains(seen[0].Text, "role requires another tool") {
						t.Fatalf("model did not receive config hint: %q", seen[0].Text)
					}
					if strings.Contains(seen[0].Text, "private-role-secret") {
						t.Fatalf("internal diagnostics leaked to model: %q", seen[0].Text)
					}
					if got := agentRecordsOrNone(t, store, "conv"); len(got) != 0 {
						t.Fatalf("rejected creation left sub-agents: %+v", got)
					}
				})
			}
		})
	}
}
