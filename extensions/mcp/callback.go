package mcp

import (
	"context"
	"errors"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/tool"
)

type Operation string

const (
	OperationInitialize Operation = "initialize"
	OperationListTools  Operation = "list_tools"
	OperationCallTool   Operation = "call_tool"
)

type InitializeRequest struct{ ClientName, ClientVersion string }
type InitializeResponse struct{ ProtocolVersion string }
type ListToolsRequest struct{ Refresh bool }
type ListToolsResponse struct{ Tools []tool.Spec }
type CallToolRequest struct {
	Name string
	Args map[string]any
}
type CallToolResponse struct{ Result tool.Result }

type CallbackRequest struct {
	Operation  Operation
	Initialize *InitializeRequest
	ListTools  *ListToolsRequest
	CallTool   *CallToolRequest
}
type CallbackResponse struct {
	Operation  Operation
	Initialize *InitializeResponse
	ListTools  *ListToolsResponse
	CallTool   *CallToolResponse
}
type CallbackFailure struct {
	Request CallbackRequest
	Err     error
}

type Callback interface {
	callback.Handler
	OnMCPStart(context.Context, CallbackRequest) error
	OnMCPEnd(context.Context, CallbackResponse) error
	OnMCPError(context.Context, CallbackFailure) error
}

type CallbackFuncs struct {
	CallbackName string
	Start        func(context.Context, CallbackRequest) error
	End          func(context.Context, CallbackResponse) error
	Error        func(context.Context, CallbackFailure) error
}

func (f CallbackFuncs) Name() string { return f.CallbackName }
func (f CallbackFuncs) OnMCPStart(ctx context.Context, v CallbackRequest) error {
	if f.Start == nil {
		return nil
	}
	return f.Start(ctx, v.Clone())
}
func (f CallbackFuncs) OnMCPEnd(ctx context.Context, v CallbackResponse) error {
	if f.End == nil {
		return nil
	}
	return f.End(ctx, v.Clone())
}
func (f CallbackFuncs) OnMCPError(ctx context.Context, v CallbackFailure) error {
	if f.Error == nil {
		return nil
	}
	v.Request = v.Request.Clone()
	return f.Error(ctx, v)
}

func (r CallbackRequest) Validate() error {
	count := 0
	for _, active := range []bool{r.Initialize != nil, r.ListTools != nil, r.CallTool != nil} {
		if active {
			count++
		}
	}
	if r.Operation == "" || count != 1 {
		return errors.New("MCP callback request requires one operation-specific value")
	}
	return nil
}
func (r CallbackRequest) Clone() CallbackRequest   { return callback.CloneValue(r).(CallbackRequest) }
func (r CallbackResponse) Clone() CallbackResponse { return callback.CloneValue(r).(CallbackResponse) }

func beginCallback(ctx context.Context, request CallbackRequest) (context.Context, callback.Invocation) {
	operationCtx, invocation := callback.Begin(ctx, "mcp", string(request.Operation), nil)
	_ = callback.Dispatch[Callback](operationCtx, invocation.WithPhase(callback.PhaseStart), func(ctx context.Context, h Callback) error { return h.OnMCPStart(ctx, request.Clone()) })
	return operationCtx, invocation
}
func endCallback(ctx context.Context, invocation callback.Invocation, response CallbackResponse) {
	_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseEnd), func(ctx context.Context, h Callback) error { return h.OnMCPEnd(ctx, response.Clone()) })
}
func errorCallback(ctx context.Context, invocation callback.Invocation, request CallbackRequest, err error) {
	_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseError), func(ctx context.Context, h Callback) error {
		return h.OnMCPError(ctx, CallbackFailure{Request: request.Clone(), Err: err})
	})
}

// ToolsChanged is the real-time signal emitted for tools/list_changed.
type ToolsChanged struct{}
