package agent

import (
	"context"
	"errors"
	"testing"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func lifecycleStream(t *testing.T, values []schema.Message, terminal error) *runnable.StreamReader[[]schema.Message] {
	t.Helper()
	reader, writer := runnable.Pipe[[]schema.Message](context.Background())
	go func() {
		for _, value := range values {
			_ = writer.Send([]schema.Message{value})
		}
		if terminal != nil {
			_ = writer.CloseWithError(terminal)
		} else {
			_ = writer.Close()
		}
	}()
	return reader
}

func TestRunLifecycleReturnsOnlyFinalMessages(t *testing.T) {
	lifecycle := &runLifecycle{ctx: context.Background()}
	wrapped, err := lifecycle.wrap(lifecycleStream(t, []schema.Message{schema.AssistantMessage("final")}, nil))
	if err != nil {
		t.Fatal(err)
	}
	output, err := wrapped.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 || output[0].JoinedText("") != "final" {
		t.Fatalf("output=%#v", output)
	}
}

func TestRunLifecyclePreservesEmptyFinalMessages(t *testing.T) {
	lifecycle := &runLifecycle{ctx: context.Background()}
	wrapped, err := lifecycle.wrap(lifecycleStream(t, []schema.Message{}, nil))
	if err != nil {
		t.Fatal(err)
	}
	output, err := wrapped.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output == nil || len(output) != 0 {
		t.Fatalf("output=%#v", output)
	}
}

func TestRunLifecyclePropagatesUpstreamError(t *testing.T) {
	want := errors.New("upstream")
	lifecycle := &runLifecycle{ctx: context.Background()}
	wrapped, err := lifecycle.wrap(lifecycleStream(t, nil, want))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wrapped.Collect(context.Background()); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
}
