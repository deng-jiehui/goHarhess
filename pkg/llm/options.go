package llm

import (
	"reflect"

	"github.com/goharness/goharness/pkg/runnable"
)

// Options 是模型单次调用的类型化运行配置，经 Runnable Option 传入并由模型
// 自身解析。DeltaStream 选择增量输出协议；ExtraGenerateConfig 写回 Provider
// 请求体的扩展参数，与构造时配置的同名键按调用优先合并。
type Options struct {
	DeltaStream         bool
	ExtraGenerateConfig GenerateConfig
	// ResponseFormat 是本次调用的类型化输出格式；nil 表示普通消息输出。
	// 与 ExtraGenerateConfig["response_format"] 同时配置属于冲突，Provider
	// 在发送请求前返回 ErrResponseFormatConflict。
	ResponseFormat *ResponseFormat
}

// optionTypesProvider 是可选的组件描述接口：模型与包装器声明自己额外
// 接受的配置类型（Provider 扩展选项等），节点与包装层据此保留透传能力。
type optionTypesProvider interface {
	OptionTypes() []reflect.Type
}

// OptionTypes 声明 ChatModelNode 接收 llm.Options 类型的配置选项；
// 下游模型声明的扩展类型一并保留，让 Provider 专属配置能穿透节点分发。
// 解析发生在下游模型入口，节点只负责按类型承接分发。
func (n *ChatModelNode) OptionTypes() []reflect.Type {
	types := []reflect.Type{runnable.OptionType[Options]()}
	if n != nil && n.model != nil {
		if provider, ok := n.model.(optionTypesProvider); ok && provider != nil {
			types = append(types, provider.OptionTypes()...)
		}
	}
	return types
}

// WithDeltaStream 让模型按增量协议输出流式片段。
func WithDeltaStream(enabled bool) runnable.Option {
	return runnable.WrapOption(func(o *Options) { o.DeltaStream = enabled })
}

// WithGenerateConfig 把扩展生成参数整体并入本次调用配置。
func WithGenerateConfig(config GenerateConfig) runnable.Option {
	merged := GenerateConfig(cloneAnyMap(config))
	return runnable.WrapOption(func(o *Options) {
		o.ExtraGenerateConfig = MergeGenerateConfig(o.ExtraGenerateConfig, merged)
	})
}

// WithMaxTokens 限制本次调用的最大输出 Token 数。
func WithMaxTokens(max int) runnable.Option {
	return WithGenerateConfig(GenerateConfig{"max_tokens": max})
}

// WithSeed 为本次调用指定确定性采样种子。
func WithSeed(seed int64) runnable.Option {
	return WithGenerateConfig(GenerateConfig{"seed": seed})
}

// PrepareOptions 在组合组件边界解析一次通用模型配置：resolved 是本次调用
// 的有效 llm.Options，forwarded 把该类型替换为冻结结果、其余选项原样保留，
// 可直接替代原选项列表继续传递。上下文预算等需要在调用前读取有效配置的
// 一方使用它；原始设置函数不在下游重复执行。
func PrepareOptions(options []runnable.Option) (resolved Options, forwarded []runnable.Option, err error) {
	specs, err := runnable.FlattenOptions(options)
	if err != nil {
		return Options{}, nil, err
	}
	// 没有 llm.Options 规格时不注入冻结项：无模型的配方不应因一个零值
	// 广播选项而收到“无接收者”错误。
	present := false
	optionType := runnable.OptionType[Options]()
	for _, spec := range specs {
		if len(spec.Path) == 0 && spec.ConfigType == optionType {
			present = true
			break
		}
	}
	resolved, remaining, err := resolveModelOptions(options)
	if err != nil {
		return Options{}, nil, err
	}
	if !present {
		return resolved, options, nil
	}
	return resolved, append(remaining, freezeModelOptions(resolved)), nil
}

// resolveModelOptions 解析属于 llm.Options 的调用选项，返回解析结果与
// 未消费的其余选项（Provider 扩展类型等），供包装器继续转发。
func resolveModelOptions(options []runnable.Option) (Options, []runnable.Option, error) {
	return runnable.ResolveComponentOptions(Options{}, options...)
}

// freezeModelOptions 把已解析结果重新包装为单个 Option；重试、缓存等代理
// 在转发前解析一次，后续调用只应用结果而不重复执行原始设置函数。
func freezeModelOptions(resolved Options) runnable.Option {
	frozen := resolved.Clone()
	return runnable.WrapOption(func(o *Options) { *o = frozen.Clone() })
}
