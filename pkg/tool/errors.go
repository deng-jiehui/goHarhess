package tool

import (
	"errors"
	"fmt"
)

var (
	// ErrPermissionDenied identifies a business-policy denial.
	ErrPermissionDenied = errors.New("tool permission denied")
	// ErrNilContext identifies a nil context passed to system execution.
	ErrNilContext = errors.New("tool execution context cannot be nil")
	// ErrExecutionPanic identifies a recovered panic in user code.
	ErrExecutionPanic = errors.New("tool execution panicked")
	// ErrMaxDepthExceeded identifies a recursive tool execution that exceeded the system limit.
	ErrMaxDepthExceeded = errors.New("maximum tool execution depth exceeded")
	// ErrDeadlockRisk identifies an unsafe nested resource acquisition.
	ErrDeadlockRisk = errors.New("tool execution deadlock risk detected")
	// ErrInvalidMiddleware identifies a missing middleware handler.
	ErrInvalidMiddleware = errors.New("invalid middleware")
	// ErrNextUnavailable identifies next used outside its Middleware lifetime.
	ErrNextUnavailable = errors.New("middleware next is no longer available")
	// ErrInvalidToolArgs identifies tool call arguments that violate the declared JSON schema.
	ErrInvalidToolArgs = errors.New("tool arguments violate the declared schema")
	// ErrMalformedArgs identifies tool call arguments that are not a valid JSON object.
	ErrMalformedArgs = errors.New("tool arguments are not a valid JSON object")
	// ErrUnknownTool identifies a call naming a tool that is not registered.
	ErrUnknownTool = errors.New("tool does not exist")
	// ErrToolAbandoned identifies an execution left running after its timeout or
	// cancellation; the abandoned worker keeps holding runtime resources.
	ErrToolAbandoned = errors.New("tool execution abandoned: the tool did not respond to cancellation")
	// ErrToolUnavailable identifies a call rejected while the tool's abandoned
	// executions reached the configured threshold.
	ErrToolUnavailable = errors.New("tool is temporarily unavailable")
)

// ModelVisibleError marks an error whose message is authored for the model.
// Tool and middleware code wrap feedback meant for the next model turn; every
// other error class is replaced by a sanitized generic message before reaching
// the model, while the full error remains in logs and callbacks.
type ModelVisibleError struct {
	Err error
}

// ModelVisible wraps err so its message is shown to the model in tool results.
func ModelVisible(err error) error {
	if err == nil {
		return nil
	}
	return &ModelVisibleError{Err: err}
}

func (e *ModelVisibleError) Error() string {
	return e.Err.Error()
}

func (e *ModelVisibleError) Unwrap() error {
	return e.Err
}

// ExecutionPanicError describes a panic recovered by the fixed execution pipeline.
type ExecutionPanicError struct {
	Component string
	Value     any
	Err       error
}

func (e *ExecutionPanicError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s panicked: %v", e.Component, e.Err)
	}
	return fmt.Sprintf("%s panicked: %v", e.Component, e.Value)
}

func (e *ExecutionPanicError) Unwrap() error {
	return e.Err
}

func (e *ExecutionPanicError) Is(target error) bool {
	return target == ErrExecutionPanic
}

// PermissionDeniedError identifies the business extension that rejected a call.
type PermissionDeniedError struct {
	ToolName string
	Layer    string
	Err      error
}

// Deny creates a business-policy denial for Middleware code.
// Runtime adds the tool and extension names.
func Deny(reason error) error {
	return &PermissionDeniedError{Err: reason}
}

func (e *PermissionDeniedError) Error() string {
	prefix := fmt.Sprintf("tool %q permission denied", e.ToolName)
	if e.Layer != "" {
		prefix += fmt.Sprintf(" by %s", e.Layer)
	}
	if e.Err == nil {
		return prefix
	}
	return fmt.Sprintf("%s: %v", prefix, e.Err)
}

func (e *PermissionDeniedError) Unwrap() error {
	return e.Err
}

func (e *PermissionDeniedError) Is(target error) bool {
	return target == ErrPermissionDenied
}

// InvalidArgsError reports arguments that failed the tool's JSON schema. It is
// a model-visible tool error (isError result), not an execution failure.
type InvalidArgsError struct {
	ToolName string
	Err      error
}

func (e *InvalidArgsError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("tool %q arguments violate the declared schema", e.ToolName)
	}
	return fmt.Sprintf("tool %q arguments violate the declared schema: %v", e.ToolName, e.Err)
}

func (e *InvalidArgsError) Unwrap() error {
	return e.Err
}

func (e *InvalidArgsError) Is(target error) bool {
	return target == ErrInvalidToolArgs
}

// MaxDepthError reports the current depth and the fixed system limit.
type MaxDepthError struct {
	Depth    int
	MaxDepth int
}

func (e *MaxDepthError) Error() string {
	return fmt.Sprintf("maximum tool execution depth exceeded: depth=%d max_depth=%d", e.Depth, e.MaxDepth)
}

func (e *MaxDepthError) Is(target error) bool {
	return target == ErrMaxDepthExceeded
}

// DeadlockError describes a nested group acquisition rejected by lock ordering.
type DeadlockError struct {
	RequestedGroup string
	HeldGroups     []string
}

func (e *DeadlockError) Error() string {
	return fmt.Sprintf("tool execution deadlock risk detected: requested group %q while holding %v", e.RequestedGroup, e.HeldGroups)
}

func (e *DeadlockError) Is(target error) bool {
	return target == ErrDeadlockRisk
}

// AbandonedError 是放弃事件的告警载荷：Execute 因超时或取消先返回后，工具仍
// 在后台运行，归还前持续占用 worker、批次与并发组资源。
type AbandonedError struct {
	ToolName  string
	Abandoned int
	Threshold int
	Err       error
}

func (e *AbandonedError) Error() string {
	return fmt.Sprintf("tool %q ignored cancellation (%v); %d abandoned execution(s) still hold runtime resources", e.ToolName, e.Err, e.Abandoned)
}
func (e *AbandonedError) Unwrap() error        { return e.Err }
func (e *AbandonedError) Is(target error) bool { return target == ErrToolAbandoned }

// UnavailableError 报告熔断期间被拒绝的调用：该工具的未归还执行数已达阈值。
type UnavailableError struct {
	ToolName  string
	Abandoned int
	Threshold int
}

func (e *UnavailableError) Error() string {
	return fmt.Sprintf("tool %q is temporarily unavailable: %d abandoned execution(s) reached the threshold %d", e.ToolName, e.Abandoned, e.Threshold)
}
func (e *UnavailableError) Is(target error) bool { return target == ErrToolUnavailable }

// ExtensionError adds the middleware name and lifecycle phase to a business error.
type ExtensionError struct {
	Kind  string
	Name  string
	Phase string
	Err   error
}

func (e *ExtensionError) Error() string {
	return fmt.Sprintf("%s %q %s failed: %v", e.Kind, e.Name, e.Phase, e.Err)
}

func (e *ExtensionError) Unwrap() error {
	return e.Err
}
