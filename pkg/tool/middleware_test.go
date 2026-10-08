package tool_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/goharness/pkg/tool"
)

func TestMiddlewareFuncCallsNext(t *testing.T) {
	middleware := tool.MiddlewareFunc(func(
		ctx context.Context,
		spec tool.Spec,
		input tool.CallInput,
		next tool.Next,
	) (tool.Result, error) {
		input.Args["checked"] = true
		return next(ctx, input)
	})
	result, err := middleware.Handle(
		context.Background(),
		tool.Spec{Name: "read"},
		tool.CallInput{Args: map[string]any{}},
		func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
			if input.Args["checked"] != true {
				t.Fatalf("input = %#v", input.Args)
			}
			return tool.TextResult("ok"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := result.ToContentBlocks()
	if err != nil || len(blocks) != 1 || blocks[0].Text == nil || blocks[0].Text.Text != "ok" {
		t.Fatalf("result = %#v, blocks = %#v, err = %v", result, blocks, err)
	}
}

func TestNilMiddlewareFuncReturnsTypedError(t *testing.T) {
	var middleware tool.MiddlewareFunc
	_, err := middleware.Handle(context.Background(), tool.Spec{Name: "read"}, tool.CallInput{}, nil)
	if !errors.Is(err, tool.ErrInvalidMiddleware) {
		t.Fatalf("Handle error = %v, want ErrInvalidMiddleware", err)
	}
}

func TestDenyPreservesReason(t *testing.T) {
	reason := errors.New("approval required")
	err := tool.Deny(reason)
	if !errors.Is(err, tool.ErrPermissionDenied) || !errors.Is(err, reason) {
		t.Fatalf("Deny error = %v", err)
	}
}
