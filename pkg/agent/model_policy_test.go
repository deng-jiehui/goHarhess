package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// statusError 带 HTTP 状态码，用于触发默认的可重试判定。
type statusError struct{ code int }

func (e statusError) Error() string   { return fmt.Sprintf("status %d", e.code) }
func (e statusError) StatusCode() int { return e.code }

func TestAgentModelRetryRetriesTransientFailures(t *testing.T) {
	var calls atomic.Int32
	model := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		if calls.Add(1) <= 2 {
			return nil, statusError{code: 503}
		}
		return []schema.Message{schema.AssistantMessage("recovered")}, nil
	})
	wrapped, err := llm.NewRetryingModel(model, llm.RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	value, err := agent.New(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("model calls = %d, want 3", calls.Load())
	}
	if len(messages) == 0 || messages[len(messages)-1].JoinedText("") != "recovered" {
		t.Fatalf("final messages = %#v", messages)
	}
}

func TestAgentModelRetryDoesNotRetryPermanentFailures(t *testing.T) {
	var calls atomic.Int32
	model := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		calls.Add(1)
		return nil, statusError{code: 400}
	})
	wrapped, err := llm.NewRetryingModel(model, llm.RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	value, err := agent.New(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err == nil {
		t.Fatal("expected model error")
	}
	if calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1 (non-retryable must not retry)", calls.Load())
	}
}

func TestAgentModelRateLimitSpacesModelCalls(t *testing.T) {
	const interval = 60 * time.Millisecond
	var starts []time.Time
	model := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		starts = append(starts, time.Now())
		if len(starts) == 1 {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "echo", `{"text":"x"}`))}, nil
		}
		return []schema.Message{schema.AssistantMessage("done")}, nil
	})
	limited, err := llm.NewRateLimitedModel(model, llm.RateLimitConfig{MinInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	value, err := agent.New(limited, agent.WithTools(echo))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 2 {
		t.Fatalf("model calls = %d, want 2", len(starts))
	}
	// 最小间隔约束的是请求启动时刻；给计时器粒度留少量误差。
	if gap := starts[1].Sub(starts[0]); gap < interval-10*time.Millisecond {
		t.Fatalf("model call gap = %s, want >= %s", gap, interval)
	}
}

func TestAgentRateLimitInsideManualRetrySpacing(t *testing.T) {
	// 手工组合「重试外层、限流内层」：第二次真实请求必须重新经过最小间隔。
	const interval = 80 * time.Millisecond
	var starts []time.Time
	inner := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		starts = append(starts, time.Now())
		if len(starts) == 1 {
			return nil, statusError{code: 503}
		}
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	limited, err := llm.NewRateLimitedModel(inner, llm.RateLimitConfig{MinInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := llm.NewRetryingModel(limited, llm.RetryConfig{MaxAttempts: 2, InitialDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	value, err := agent.New(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 2 {
		t.Fatalf("model calls = %d, want 2", len(starts))
	}
	if gap := starts[1].Sub(starts[0]); gap < interval-10*time.Millisecond {
		t.Fatalf("retry request gap = %s, want >= %s (each real request must pass the limiter)", gap, interval)
	}
}

func TestAgentExplicitZeroToolRetriesDisablesRetries(t *testing.T) {
	// 显式零值：明确关闭重试，不得回填默认值。
	var attempts atomic.Int32
	flaky := tool.NewFunctionTool(tool.Spec{Name: "flaky", IsReadOnly: true}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		attempts.Add(1)
		return tool.Result{}, errors.New("always fails")
	})
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "flaky", `{}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.New(model, agent.WithTools(flaky), agent.WithToolRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("tool attempts = %d, want 1 (explicit zero disables retries)", attempts.Load())
	}
}

// backoffScenario 度量一次运行中只读工具两次实际启动的间隔。
func backoffScenario(t *testing.T, opts ...agent.Option) time.Duration {
	t.Helper()
	var starts []time.Time
	flaky := tool.NewFunctionTool(tool.Spec{Name: "flaky", IsReadOnly: true}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		starts = append(starts, time.Now())
		if len(starts) == 1 {
			return tool.Result{}, errors.New("transient")
		}
		return tool.TextResult("ok"), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "flaky", `{}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.New(model, append([]agent.Option{agent.WithTools(flaky)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 2 {
		t.Fatalf("tool attempts = %d, want 2", len(starts))
	}
	return starts[1].Sub(starts[0])
}

func TestAgentExplicitZeroRetryBackoffHonoredEndToEnd(t *testing.T) {
	// 显式零值：重试间隔必须真正为零，不得在工具节点处回填默认值。
	if gap := backoffScenario(t, agent.WithToolRetries(1), agent.WithToolRetryBackoff(0)); gap > 30*time.Millisecond {
		t.Fatalf("explicit zero backoff gap = %s, want ~0", gap)
	}
}

func TestAgentRetryBackoffDefaultAndPositiveValue(t *testing.T) {
	// 未提供间隔时沿用默认 100ms。
	if gap := backoffScenario(t, agent.WithToolRetries(1)); gap < 80*time.Millisecond {
		t.Fatalf("default backoff gap = %s, want ~100ms", gap)
	}
	// 正值按给定间隔生效。
	if gap := backoffScenario(t, agent.WithToolRetries(1), agent.WithToolRetryBackoff(60*time.Millisecond)); gap < 45*time.Millisecond {
		t.Fatalf("positive backoff gap = %s, want ~60ms", gap)
	}
}
