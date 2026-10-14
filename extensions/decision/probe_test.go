package decision_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// TestProbeInterruptInToolMiddleware 是 W4-9 的先行验证：graph.Interrupt
// 能否在 ReAct Agent 的 tool.Middleware 里使用、Resume 后拿到答案、
// 同批已完成兄弟调用不重跑。
func TestProbeInterruptInToolMiddleware(t *testing.T) {
	var fastDone, slowDone atomic.Int32

	fast := tool.NewFunctionTool(tool.Spec{Name: "fast", IsReadOnly: true},
		func(context.Context, tool.CallInput) (tool.Result, error) {
			fastDone.Add(1)
			return tool.TextResult("fast ok"), nil
		})
	slow := tool.NewFunctionTool(tool.Spec{Name: "slow"},
		func(context.Context, tool.CallInput) (tool.Result, error) {
			slowDone.Add(1)
			return tool.TextResult("slow ok"), nil
		})

	guard := tool.MiddlewareFunc(func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
		if spec.Name != "slow" {
			return next(ctx, input)
		}
		answer, err := graph.Interrupt(ctx, map[string]any{"tool": spec.Name})
		if err != nil {
			return tool.Result{}, err
		}
		if answer != true {
			return tool.ErrorResult(schema.NewTextBlock("rejected")), nil
		}
		return next(ctx, input)
	})

	value, err := agent.New(llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("c1", "fast", `{}`),
			schema.NewFunctionToolCallBlock("c2", "slow", `{}`))},
		[]schema.Message{schema.AssistantMessage("done")}),
		agent.WithName("probe"), agent.WithTools(fast, slow), agent.WithMiddleware(guard))
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithCheckpoint(state.NewMemoryStore(), "probe")

	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("expected InterruptedError, got %v", err)
	}
	pending := suspended.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("pending=%d", len(pending))
	}
	// 中断发生在 slow 的筛查里；fast 已完成，slow 未执行。
	if fastDone.Load() != 1 || slowDone.Load() != 0 {
		t.Fatalf("fast=%d slow=%d", fastDone.Load(), slowDone.Load())
	}

	out, err := agent.Resume(context.Background(), value, option, agent.WithResume(pending[0].ID, true))
	if err != nil {
		t.Fatal(err)
	}
	if last := out[len(out)-1]; last.JoinedText("") != "done" {
		t.Fatalf("unexpected output: %v", last.JoinedText(""))
	}
	// 恢复后 slow 恰好执行一次；已完成的 fast 不重跑。
	if fastDone.Load() != 1 || slowDone.Load() != 1 {
		t.Fatalf("fast=%d slow=%d", fastDone.Load(), slowDone.Load())
	}
}
