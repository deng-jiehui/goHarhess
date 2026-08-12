package graph

import (
	"fmt"
	"strings"
)

type ValidationError struct {
	Code    string
	Path    string
	Message string
}

func (e ValidationError) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %s", e.Code, e.Path, e.Message)
}

type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	parts := make([]string, 0, len(e))
	for _, item := range e {
		parts = append(parts, item.Error())
	}
	return "graph validation failed: " + strings.Join(parts, "; ")
}

func validationError(code, path, format string, args ...any) ValidationError {
	return ValidationError{
		Code:    code,
		Path:    path,
		Message: fmt.Sprintf(format, args...),
	}
}

type ExecutionError struct {
	Code   string
	NodeID string
	Step   int
	Err    error
}

func (e *ExecutionError) Error() string {
	location := fmt.Sprintf("step %d", e.Step)
	if e.NodeID != "" {
		location += fmt.Sprintf(", node %q", e.NodeID)
	}
	return fmt.Sprintf("graph execution failed [%s] at %s: %v", e.Code, location, e.Err)
}

func (e *ExecutionError) Unwrap() error {
	return e.Err
}
