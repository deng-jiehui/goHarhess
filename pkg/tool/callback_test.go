package tool_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func TestToolModelVisibleErrorUsesEndLifecycle(t *testing.T) {
	endCalls, errorCalls := 0, 0
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind != observe.KindTool {
			return
		}
		switch event.Phase {
		case observe.PhaseEnd:
			endCalls++
		case observe.PhaseError:
			errorCalls++
		}
	})
	ctx, invocation := tool.BeginCallback(observe.With(context.Background(), observer), tool.Spec{Name: "search"}, tool.CallInput{})
	tool.EndCallback(ctx, invocation, tool.Spec{Name: "search"}, tool.CallInput{}, tool.ErrorResult(schema.NewTextBlock("denied")), nil)
	if endCalls != 1 || errorCalls != 0 {
		t.Fatalf("end/error=%d/%d", endCalls, errorCalls)
	}
	tool.EndCallback(ctx, invocation, tool.Spec{Name: "search"}, tool.CallInput{}, tool.Result{}, errors.New("transport"))
	if errorCalls != 1 {
		t.Fatalf("error calls=%d", errorCalls)
	}
}
