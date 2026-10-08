package mcp

import (
	"context"
	"errors"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/tool"
)

// kindMCP 是 MCP 协议层操作的观测事件类别；工具调用本体另有 tool 事件。
const kindMCP = observe.Kind("mcp")

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

// CallbackRequest/CallbackResponse/CallbackFailure 是观测事件的原始载荷：
// 经 observe.CapturePayload 脱敏后才进入 Event.Payload。
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
func (r CallbackRequest) Clone() CallbackRequest   { return observe.CloneValue(r).(CallbackRequest) }
func (r CallbackResponse) Clone() CallbackResponse { return observe.CloneValue(r).(CallbackResponse) }

func beginCallback(ctx context.Context, request CallbackRequest) (context.Context, observe.Invocation) {
	if !observe.Active(ctx) {
		return ctx, observe.Invocation{}
	}
	operationCtx, invocation := observe.Begin(ctx, "mcp", string(request.Operation), nil)
	observe.EmitPayload(operationCtx, observe.EventOf(invocation.WithPhase(observe.PhaseStart), kindMCP, nil), request)
	return operationCtx, invocation
}
func endCallback(ctx context.Context, invocation observe.Invocation, response CallbackResponse) {
	if invocation.Component == "" {
		return
	}
	observe.EmitPayload(ctx, observe.EventOf(invocation.WithPhase(observe.PhaseEnd), kindMCP, nil), response)
}
func errorCallback(ctx context.Context, invocation observe.Invocation, request CallbackRequest, err error) {
	if invocation.Component == "" {
		return
	}
	failure := CallbackFailure{Request: request.Clone(), Err: err}
	event := observe.EventOf(invocation.WithPhase(observe.PhaseError), kindMCP, nil)
	event.Err = err
	observe.EmitPayload(ctx, event, failure)
}

// ToolsChanged is the real-time signal emitted for tools/list_changed.
type ToolsChanged struct{}
