package tool

import (
	"context"
	"errors"
	"time"
)

// executionState 的 held 按调度器分域：持有组序只约束共享同一信号量池的
// 调用（同一 Runtime 的 Scope.Execute 链）。嵌套 Graph / 其他 Runtime 的
// ToolsNode 拥有独立 worker 池，不产生等待环，不参与本域的组序检查。
// MaxDepth 保持全局计数，跨 runtime 的嵌套调用仍受总深度限制。
type executionState struct {
	depth int
	held  map[*scheduler][]string
}

type executionStateKey struct{}

func stateFromContext(ctx context.Context) executionState {
	if ctx == nil {
		return executionState{}
	}
	state, _ := ctx.Value(executionStateKey{}).(executionState)
	return state
}

// heldFor 返回当前状态下指定调度器的持有组。
func (s executionState) heldFor(owner *scheduler) []string {
	if len(s.held) == 0 {
		return nil
	}
	return append([]string(nil), s.held[owner]...)
}

type controlledContext struct {
	control context.Context
	values  context.Context
	state   executionState
}

func (c *controlledContext) Deadline() (time.Time, bool) {
	controlDeadline, controlOK := c.control.Deadline()
	valueDeadline, valueOK := c.values.Deadline()
	switch {
	case controlOK && valueOK && valueDeadline.Before(controlDeadline):
		return valueDeadline, true
	case controlOK:
		return controlDeadline, true
	default:
		return valueDeadline, valueOK
	}
}

func (c *controlledContext) Done() <-chan struct{} {
	return c.control.Done()
}

func (c *controlledContext) Err() error {
	return c.control.Err()
}

func (c *controlledContext) Value(key any) any {
	if _, ok := key.(executionStateKey); ok {
		return c.state
	}
	if value := c.values.Value(key); value != nil {
		return value
	}
	return c.control.Value(key)
}

func bindContext(control context.Context, values context.Context, state executionState) (context.Context, func()) {
	base := &controlledContext{control: control, values: values, state: state}
	bound, cancel := context.WithCancelCause(base)
	stopValues := context.AfterFunc(values, func() {
		cancel(context.Cause(values))
	})
	cleanup := func() {
		stopValues()
		cancel(nil)
	}
	return bound, cleanup
}

// contextSentinelCause 把返回路径上的 context 哨兵错误归一到真实分类：
// bindContext 生成的 cancelCtx 其 Err() 恒为 context.Canceled——框架超时
// 触发时工具与中间层看到的也是 Canceled，deadline/cancel 的真实区别只
// 保留在 Cause 里。control 已死时按 control 分类（框架超时→
// DeadlineExceeded、调用方取消→Canceled）；仅 ctx 已死时按 ctx 分类；
// 都活着说明哨兵来自调用方自己的子 context，保留原错误。取消或超时
// 携带的自定义原因经 errors.Join 附加，不取代标准类别。
func contextSentinelCause(control, ctx context.Context, err error) error {
	if err == nil ||
		(!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
		return err
	}
	switch {
	case control.Err() != nil:
		return classifiedCause(control)
	case ctx.Err() != nil:
		return classifiedCause(ctx)
	default:
		return err
	}
}

// classifiedCause 返回已死 Context 的标准分类错误：类别由 Err() 决定；
// Cause 是自定义说明时经 errors.Join 附加到错误链（保留原因文字，且
// errors.Is 仍命中标准类别）；Cause 能识别为 DeadlineExceeded/Canceled
// 时直接提升为对应类别（覆盖 bound ctx 把上游超时误报为 Canceled、
// 以及调用方期限先于工具期限触发的传播场景）。
func classifiedCause(ctx context.Context) error {
	class := ctx.Err()
	cause := context.Cause(ctx)
	switch {
	case cause == nil || cause == class:
		return class
	case errors.Is(cause, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(cause, context.Canceled):
		return context.Canceled
	default:
		return errors.Join(class, cause)
	}
}

// withHeldGroup 记录本次调用持有的组：写时复制 held map，兄弟调用互不
// 可见，同链路的下游调用能看到完整持有序。
func withHeldGroup(ctx context.Context, owner *scheduler, group string) context.Context {
	state := stateFromContext(ctx)
	held := make(map[*scheduler][]string, len(state.held)+1)
	for k, v := range state.held {
		held[k] = v
	}
	held[owner] = append(append([]string(nil), held[owner]...), group)
	state.held = held
	return context.WithValue(ctx, executionStateKey{}, state)
}
