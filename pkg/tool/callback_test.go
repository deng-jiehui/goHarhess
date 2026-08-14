package tool_test

import (
	"context"
	"errors"
	"testing"

	"goharness/pkg/callback"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestToolModelVisibleErrorUsesEndLifecycle(t *testing.T) {
	endCalls, errorCalls := 0, 0
	observer := tool.CallbackFuncs{CallbackName: "audit", End: func(context.Context, tool.Spec, tool.CallInput, tool.Result) error { endCalls++; return nil }, Error: func(context.Context, tool.Spec, tool.CallInput, error) error { errorCalls++; return nil }}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	ctx, invocation := tool.BeginCallback(callback.WithManager(context.Background(), manager), tool.Spec{Name: "search"}, tool.CallInput{})
	tool.EndCallback(ctx, invocation, tool.Spec{Name: "search"}, tool.CallInput{}, tool.ErrorResult(schema.NewTextBlock("denied")), nil)
	if endCalls != 1 || errorCalls != 0 {
		t.Fatalf("end/error=%d/%d", endCalls, errorCalls)
	}
	tool.EndCallback(ctx, invocation, tool.Spec{Name: "search"}, tool.CallInput{}, tool.Result{}, errors.New("transport"))
	if errorCalls != 1 {
		t.Fatalf("error calls=%d", errorCalls)
	}
}
