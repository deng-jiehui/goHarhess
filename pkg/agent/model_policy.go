package agent

import (
	"errors"
	"fmt"
	"time"

	"github.com/goharness/goharness/pkg/llm"
)

// 模型策略的取值上限与工具侧保持同量级；更大的配额或间隔应直接在
// llm 装饰器上手工配置，而不是放进 Agent 初始化配置。
const (
	hardMaxModelAttempts    = hardMaxRetries + 1
	hardMaxModelRetryDelay  = hardMaxRetryBackoff
	hardMaxModelConcurrency = 128
	hardMaxModelInterval    = time.Minute
)

// validateModelPolicy 在使用默认值前校验模型重试与限流配置；零值表示不启用，
// 只设置延迟/谓词而未给出 MaxAttempts 视为配置错误。
func validateModelPolicy(retry llm.RetryConfig, limit llm.RateLimitConfig) error {
	if retry.MaxAttempts < 0 || retry.MaxAttempts > hardMaxModelAttempts {
		return fmt.Errorf("agent model retry max attempts must be between 0 and %d: %d", hardMaxModelAttempts, retry.MaxAttempts)
	}
	if retry.MaxAttempts < 2 {
		if retry.InitialDelay != 0 || retry.MaxDelay != 0 || retry.Retryable != nil {
			return fmt.Errorf("agent model retry requires max attempts of at least 2: %d", retry.MaxAttempts)
		}
	} else {
		if retry.InitialDelay < 0 || retry.InitialDelay > hardMaxModelRetryDelay {
			return fmt.Errorf("agent model retry initial delay must be between zero and %s: %s", hardMaxModelRetryDelay, retry.InitialDelay)
		}
		if retry.MaxDelay < 0 || retry.MaxDelay > hardMaxModelRetryDelay {
			return fmt.Errorf("agent model retry max delay must be between zero and %s: %s", hardMaxModelRetryDelay, retry.MaxDelay)
		}
	}
	if limit.MaxConcurrent < 0 || limit.MaxConcurrent > hardMaxModelConcurrency {
		return fmt.Errorf("agent model max concurrency must be between 0 and %d: %d", hardMaxModelConcurrency, limit.MaxConcurrent)
	}
	if limit.MinInterval < 0 || limit.MinInterval > hardMaxModelInterval {
		return fmt.Errorf("agent model min interval must be between 0 and %s: %s", hardMaxModelInterval, limit.MinInterval)
	}
	return nil
}

// wrapModelPolicy 按配置叠加模型装饰器，保证已公开的组合约定「重试外层、
// 限流内层」：每次真实请求都重新占用配额。先沿模型外层连续的内置策略链
// 检查已有的重试/限流层——同类策略重复配置时拒绝，而不是再叠一层让请求
// 次数相乘或配额悄悄叠加。
//
// 模型已有手工重试层且本轮只配置限流时，仅当该重试层正是最外层才可安全
// 重组：剥下它、在其内侧接入限流、再以原配置包回（保留 MaxAttempts、延迟
// 与自定义 Retryable）。重试层不在最外层（中间隔着其他包装）时无法安全
// 插入限流，构造期拒绝。隔着非内置包装的策略层无法识别，属于调用方责任。
func wrapModelPolicy(model llm.ChatModel, retry llm.RetryConfig, limit llm.RateLimitConfig) (llm.ChatModel, error) {
	policies := llm.InspectModelPolicies(model)
	retryEnabled := retry.MaxAttempts >= 2
	limitEnabled := limit.MaxConcurrent > 0 || limit.MinInterval > 0
	if retryEnabled && policies.Retry != nil {
		return nil, errors.New("model already has a retry layer; set ModelRetry or wrap manually, not both")
	}
	if limitEnabled && policies.RateLimit != nil {
		return nil, errors.New("model already has a rate-limit layer; set ModelRateLimit or wrap manually, not both")
	}
	// 手工重试层在最外层：剥下待限流接入后按原配置包回。
	var pendingRetry *llm.RetryConfig
	if limitEnabled && policies.Retry != nil {
		outer, ok := model.(*llm.RetryingModel)
		if !ok {
			return nil, errors.New("model has a retry layer that is not outermost; cannot place ModelRateLimit inside it — wrap as llm.NewRetryingModel(llm.NewRateLimitedModel(m, ...), ...) or configure both via ModelRetry/ModelRateLimit")
		}
		config := outer.Policy()
		pendingRetry = &config
		model = outer.UnwrapModel()
	}
	if limitEnabled {
		wrapped, err := llm.NewRateLimitedModel(model, limit)
		if err != nil {
			return nil, err
		}
		model = wrapped
	}
	if pendingRetry != nil {
		wrapped, err := llm.NewRetryingModel(model, *pendingRetry)
		if err != nil {
			return nil, err
		}
		model = wrapped
	}
	if retryEnabled {
		if retry.InitialDelay == 0 {
			retry.InitialDelay = DefaultRetryBackoff
		}
		wrapped, err := llm.NewRetryingModel(model, retry)
		if err != nil {
			return nil, err
		}
		model = wrapped
	}
	return model, nil
}
