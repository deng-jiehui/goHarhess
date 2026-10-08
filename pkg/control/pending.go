package control

import "context"

// CallRecorder 由图运行时装入节点上下文：每次工具调用批次经 BeginBatch
// 取得独立的恢复作用域——同一批次恢复重放时身份相同，主动发起的新批次
// 获得新身份，结果不会被错误复用。载荷为不透明字节，序列化格式由调用方
// （工具运行时）决定。
type CallRecorder interface {
	// BeginBatch 开启一批调用的恢复作用域。
	BeginBatch() CallBatch
}

// CallBatch 承载一批工具调用的恢复状态与身份。
type CallBatch interface {
	// Namespace 返回运行级身份前缀（含运行范围与节点/任务位置），
	// 供外部幂等键使用；同一运行的恢复中保持不变。
	Namespace() string
	// CallKey 返回调用在会话内的稳定身份（批次序号+调用基准键）。
	CallKey(base string) string
	// LookupPending 查询该批次下已完成调用的结果载荷。
	LookupPending(key string) (payload []byte, ok bool)
	// RecordPending 登记一次已完成调用。
	RecordPending(key string, payload []byte)
}

type callRecorderKey struct{}

// CallRecorderFromContext 返回当前调用上下文的恢复登记器。
func CallRecorderFromContext(ctx context.Context) (CallRecorder, bool) {
	if ctx == nil {
		return nil, false
	}
	recorder, ok := ctx.Value(callRecorderKey{}).(CallRecorder)
	return recorder, ok && recorder != nil
}

// WithCallRecorder 把登记器装入上下文；由图运行时在节点执行前绑定。
func WithCallRecorder(ctx context.Context, recorder CallRecorder) context.Context {
	if ctx == nil || recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, callRecorderKey{}, recorder)
}

// BeginCallBatch 开启一批调用的恢复作用域；无登记器时返回 nil。
func BeginCallBatch(ctx context.Context) CallBatch {
	if recorder, ok := CallRecorderFromContext(ctx); ok {
		return recorder.BeginBatch()
	}
	return nil
}

type callKeyContextKey struct{}

// WithCallKey 把当前调用的会话内身份装入上下文；图运行时据此把交互
// 日志归属到具体调用。
func WithCallKey(ctx context.Context, key string) context.Context {
	if ctx == nil || key == "" {
		return ctx
	}
	return context.WithValue(ctx, callKeyContextKey{}, key)
}

// CallKeyFromContext 返回当前调用身份；非调用上下文返回空串。
func CallKeyFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	key, ok := ctx.Value(callKeyContextKey{}).(string)
	return key, ok && key != ""
}

// WithoutCallKey 清除上下文中的调用身份；子运行入口使用，防止父调用
// 身份泄漏进子运行自己的会话。
func WithoutCallKey(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, callKeyContextKey{}, "")
}
