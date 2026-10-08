package agent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	value, err := agent.NewBasicAgent(
		agent.BaseConfig{Model: model},
		agent.WithModelRetry(llm.RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond}),
	)
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
	value, err := agent.NewBasicAgent(
		agent.BaseConfig{Model: model},
		agent.WithModelRetry(llm.RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond}),
	)
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
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{Model: model, Tools: []tool.Tool{echo}},
	}, agent.WithModelRateLimit(llm.RateLimitConfig{MinInterval: interval}))
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

func TestAgentToolRetriesDeprecatedAliasStillApplies(t *testing.T) {
	var attempts atomic.Int32
	flaky := tool.NewFunctionTool(tool.Spec{Name: "flaky", IsReadOnly: true}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		if attempts.Add(1) == 1 {
			return tool.Result{}, errors.New("transient")
		}
		return tool.TextResult("ok"), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "flaky", `{}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{Model: model, Tools: []tool.Tool{flaky}},
	}, agent.WithMaxRetries(2), agent.WithRetryBackoff(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("tool attempts = %d, want 2 (deprecated alias must still configure retries)", attempts.Load())
	}
}

func TestAgentRejectsConflictingRetryAliases(t *testing.T) {
	model := llm.NewScriptedModel()
	tests := []struct {
		name string
		opts []agent.Option
	}{
		{name: "retries", opts: []agent.Option{agent.WithMaxRetries(1), agent.WithToolRetries(2)}},
		{name: "backoff", opts: []agent.Option{agent.WithRetryBackoff(time.Second), agent.WithToolRetryBackoff(2 * time.Second)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := agent.NewBasicAgent(agent.BaseConfig{Model: model}, test.opts...)
			if err == nil || !strings.Contains(err.Error(), "configured twice") {
				t.Fatalf("NewBasicAgent error = %v, want alias conflict", err)
			}
		})
	}
	// 值相同视为同一意图，不报错。
	if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: model},
		agent.WithMaxRetries(2), agent.WithToolRetries(2)); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRejectsInvalidModelPolicy(t *testing.T) {
	model := llm.NewScriptedModel()
	tests := []struct {
		name string
		opts []agent.Option
	}{
		{name: "retry without attempts", opts: []agent.Option{agent.WithModelRetry(llm.RetryConfig{InitialDelay: time.Second})}},
		{name: "negative attempts", opts: []agent.Option{agent.WithModelRetry(llm.RetryConfig{MaxAttempts: -1})}},
		{name: "too many attempts", opts: []agent.Option{agent.WithModelRetry(llm.RetryConfig{MaxAttempts: 100})}},
		{name: "negative max delay", opts: []agent.Option{agent.WithModelRetry(llm.RetryConfig{MaxAttempts: 2, MaxDelay: -time.Second})}},
		{name: "negative concurrency", opts: []agent.Option{agent.WithModelRateLimit(llm.RateLimitConfig{MaxConcurrent: -1})}},
		{name: "interval too large", opts: []agent.Option{agent.WithModelRateLimit(llm.RateLimitConfig{MinInterval: time.Hour})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: model}, test.opts...); err == nil {
				t.Fatal("expected config error")
			}
		})
	}
}

func TestAgentRejectsDuplicateModelPoliciesAtAnyDepth(t *testing.T) {
	inner := llm.NewScriptedModel()
	retryWrapped, err := llm.NewRetryingModel(inner, llm.RetryConfig{MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	// 手工重试 + Agent 重试 + Agent 限流：新增限流不得遮住已有重试层。
	if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: retryWrapped},
		agent.WithModelRetry(llm.RetryConfig{MaxAttempts: 2}),
		agent.WithModelRateLimit(llm.RateLimitConfig{MaxConcurrent: 1}),
	); err == nil || !strings.Contains(err.Error(), "retry layer") {
		t.Fatalf("manual retry + both configs error = %v", err)
	}
	// 手工嵌套 Retrying(RateLimited(m))：内层限流也必须被识别。
	limited, err := llm.NewRateLimitedModel(inner, llm.RateLimitConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	nested, err := llm.NewRetryingModel(limited, llm.RetryConfig{MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: nested},
		agent.WithModelRateLimit(llm.RateLimitConfig{MaxConcurrent: 2})); err == nil || !strings.Contains(err.Error(), "rate-limit layer") {
		t.Fatalf("nested rate limit not detected: %v", err)
	}
	if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: nested},
		agent.WithModelRetry(llm.RetryConfig{MaxAttempts: 3})); err == nil {
		t.Fatal("nested retry layer not detected")
	}
	if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: limited},
		agent.WithModelRateLimit(llm.RateLimitConfig{MaxConcurrent: 2})); err == nil {
		t.Fatal("outer rate limit layer not detected")
	}
}

func TestAgentRateLimitInsideManualRetrySpacing(t *testing.T) {
	// 手工重试 + Agent 限流：框架重组为「重试外层、限流内层」，
	// 第二次真实请求必须重新经过最小间隔。
	const interval = 80 * time.Millisecond
	var starts []time.Time
	inner := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		starts = append(starts, time.Now())
		if len(starts) == 1 {
			return nil, statusError{code: 503}
		}
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	retryWrapped, err := llm.NewRetryingModel(inner, llm.RetryConfig{MaxAttempts: 2, InitialDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: retryWrapped},
		agent.WithModelRateLimit(llm.RateLimitConfig{MinInterval: interval}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 2 {
		t.Fatalf("model calls = %d, want 2 (manual retry count preserved)", len(starts))
	}
	if gap := starts[1].Sub(starts[0]); gap < interval-10*time.Millisecond {
		t.Fatalf("retry request gap = %s, want >= %s (each real request must pass the limiter)", gap, interval)
	}
}

func TestAgentExplicitZeroToolRetriesDisablesLegacyValue(t *testing.T) {
	model := llm.NewScriptedModel()
	// 旧字段配新选项显式零值：必须报告冲突，不得静默恢复旧值。
	if _, err := agent.NewBasicAgent(
		agent.BaseConfig{Model: model, MaxRetries: 2},
		agent.WithToolRetries(0),
	); err == nil || !strings.Contains(err.Error(), "configured twice") {
		t.Fatalf("legacy MaxRetries + explicit zero error = %v", err)
	}
	// 反向混用：显式旧零值选项与新字段非零同样冲突。
	if _, err := agent.NewBasicAgent(
		agent.BaseConfig{Model: model, ToolRetries: 2},
		agent.WithMaxRetries(0),
	); err == nil || !strings.Contains(err.Error(), "configured twice") {
		t.Fatalf("new field + explicit legacy zero error = %v", err)
	}
	// 单独显式零值：明确关闭重试，不得回填旧字段或默认值。
	var attempts atomic.Int32
	flaky := tool.NewFunctionTool(tool.Spec{Name: "flaky", IsReadOnly: true}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		attempts.Add(1)
		return tool.Result{}, errors.New("always fails")
	})
	scripted := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "flaky", `{}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{Model: scripted, Tools: []tool.Tool{flaky}},
	}, agent.WithToolRetries(0))
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
	value, err := agent.NewReactAgent(agent.ReactAgentConfig{
		BaseConfig: agent.BaseConfig{Model: model, Tools: []tool.Tool{flaky}},
	}, opts...)
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
	// 新选项显式零值：重试间隔必须真正为零，不得在工具节点处回填默认值。
	if gap := backoffScenario(t, agent.WithToolRetries(1), agent.WithToolRetryBackoff(0)); gap > 30*time.Millisecond {
		t.Fatalf("explicit zero backoff gap = %s, want ~0", gap)
	}
	// 旧别名单独设零同样生效。
	if gap := backoffScenario(t, agent.WithToolRetries(1), agent.WithRetryBackoff(0)); gap > 30*time.Millisecond {
		t.Fatalf("legacy explicit zero backoff gap = %s, want ~0", gap)
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

func TestAgentRejectsDoubleWrappedModel(t *testing.T) {
	inner := llm.NewScriptedModel()
	retryWrapped, err := llm.NewRetryingModel(inner, llm.RetryConfig{MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.NewBasicAgent(agent.BaseConfig{Model: retryWrapped},
		agent.WithModelRetry(llm.RetryConfig{MaxAttempts: 2})); err == nil || !strings.Contains(err.Error(), "retry layer") {
		t.Fatalf("retry double wrap error = %v", err)
	}
}
