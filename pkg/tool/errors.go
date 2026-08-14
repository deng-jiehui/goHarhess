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
)

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
