package llm

// ModelPolicies 描述模型外层连续的内置策略装饰器中已识别的配置；
// 字段为 nil 表示链中没有对应策略层。
type ModelPolicies struct {
	Retry     *RetryConfig
	RateLimit *RateLimitConfig
}

// InspectModelPolicies 从最外层开始逐层检查内置策略装饰器
// （RetryingModel、RateLimitedModel），返回命中的策略配置。遇到非内置
// 策略的包装层即停止——其内部是否还有策略层无法判定，调用方不应据此
// 假设「没有策略」。
func InspectModelPolicies(model ChatModel) ModelPolicies {
	var policies ModelPolicies
	for {
		switch wrapped := model.(type) {
		case *RetryingModel:
			if policies.Retry == nil {
				config := wrapped.Policy()
				policies.Retry = &config
			}
			model = wrapped.UnwrapModel()
		case *RateLimitedModel:
			if policies.RateLimit == nil {
				config := wrapped.Policy()
				policies.RateLimit = &config
			}
			model = wrapped.UnwrapModel()
		default:
			return policies
		}
	}
}
