package callback

import (
	"context"
	"fmt"
	"time"

	"github.com/goharness/goharness/pkg/observe"
)

// Trace、Invocation 及其 Phase 已统一为 pkg/observe 类型；本文件以别名
// 保留旧 API，双发期内与 observe 事件共享同一 span 体系。
type (
	Trace      = observe.Trace
	Invocation = observe.Invocation
	Phase      = observe.Phase
)

const (
	PhaseStart  = observe.PhaseStart
	PhaseOutput = observe.PhaseOutput
	PhaseEnd    = observe.PhaseEnd
	PhaseError  = observe.PhaseError
)

type PanicError struct {
	Handler string
	Value   any
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("callback %q panicked: %v", e.Handler, e.Value)
}

type TimeoutError struct {
	Handler string
	Timeout time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("callback %q exceeded execution timeout %s", e.Handler, e.Timeout)
}
func (e *TimeoutError) Unwrap() error { return context.DeadlineExceeded }

type CallbackError struct {
	Handler    string
	Invocation Invocation
	Err        error
}

func (e CallbackError) Error() string {
	return fmt.Sprintf("callback %q handling %s/%s/%s: %v", e.Handler, e.Invocation.Component, e.Invocation.Operation, e.Invocation.Phase, e.Err)
}
func (e CallbackError) Unwrap() error { return e.Err }
