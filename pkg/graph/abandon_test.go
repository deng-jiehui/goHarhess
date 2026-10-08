package graph_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/state"
)

var errInterruptForAbandon = control.RequestInterrupt("pause before abandon", nil)

// 放弃后的运行不能再恢复，新运行可以直接开始。
func TestAbandonedRunCannotResumeAndNewRunStarts(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	var first, post atomic.Int32
	compiled := mustCompile(t, newInterruptibleGraph(t, nil, &first, &post))
	option := graph.WithCheckpoint(store, "abandon")

	pausedCtx, cancel := context.WithCancelCause(ctx)
	cancel(errInterruptForAbandon)
	if _, err := compiled.Invoke(pausedCtx, "input", option); !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("pause err=%v", err)
	}
	if _, err := compiled.Invoke(ctx, "input", option); !errors.Is(err, state.ErrUnfinishedRun) {
		t.Fatalf("new run before abandon err=%v", err)
	}
	if _, err := state.AbandonRun(ctx, store, "abandon"); err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Resume(ctx, option); !errors.Is(err, graph.ErrRunAbandoned) {
		t.Fatalf("resume abandoned err=%v", err)
	}
	if _, err := compiled.PendingInterrupts(ctx, option); !errors.Is(err, graph.ErrRunAbandoned) {
		t.Fatalf("pending interrupts err=%v", err)
	}
	output, err := compiled.Invoke(ctx, "fresh", option)
	if err != nil || output != "fresh-first-second-1" {
		t.Fatalf("new run output=%q err=%v", output, err)
	}
}
