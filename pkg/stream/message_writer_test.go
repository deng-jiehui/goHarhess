package stream

import (
	"context"
	"errors"
	"github.com/goharness/goharness/pkg/schema"
	"io"
	"testing"
)

func TestMessageWriterIdentityAndCompletion(t *testing.T) {
	r, err := Run(context.Background(), func(ctx context.Context) (schema.Message, error) {
		w, err := StartMessage(ctx, schema.RoleAssistant)
		if err != nil {
			return schema.Message{}, err
		}
		for _, s := range []string{"哈", "哈"} {
			if err := w.AppendText(s); err != nil {
				return schema.Message{}, err
			}
		}
		result, err := w.End()
		if err != nil {
			return result, err
		}
		again, err := w.End()
		if err != nil || again.ID != result.ID {
			return result, errors.New("End is not idempotent")
		}
		if err := w.AppendText("late"); err == nil {
			return result, errors.New("append after completion accepted")
		}
		return result, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	accumulator := schema.NewMessageAccumulator()
	var id string
	ends := 0
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Validate(); err != nil {
			t.Fatal(err)
		}
		if e.Message != nil {
			id = e.Message.MessageID
			if err := accumulator.Apply(*e.Message.Delta); err != nil {
				t.Fatal(err)
			}
			if e.Message.Delta.Op == schema.MessageEnd {
				ends++
			}
		}
		if e.Result != nil {
			m, _ := accumulator.Message(id)
			if e.Result.ID != id || m.ID != id || e.Result.JoinedText("") != "哈哈" {
				t.Fatal(e.Result)
			}
		}
	}
	if ends != 1 {
		t.Fatal(ends)
	}
}

func TestConsumerMutationCannotCorruptTerminalActivity(t *testing.T) {
	release := make(chan struct{})
	r, _ := Run(context.Background(), func(ctx context.Context) (int, error) { StartActivity(ctx, "query"); <-release; return 1, nil })
	defer r.Close()
	first, err := r.Recv()
	if err != nil {
		t.Fatal(err)
	}
	id := first.Activity.ID
	first.Activity.ID = "modified"
	first.Activity.Status = ActivityCompleted
	close(release)
	end, err := r.Recv()
	if err != nil || end.Activity == nil || end.Activity.ID != id || end.Activity.Status != ActivityCanceled {
		t.Fatalf("%+v %v", end, err)
	}
}

func TestPublicProtocolRejectsInvalidPayloadsAndOrdering(t *testing.T) {
	r, _ := Run(context.Background(), func(ctx context.Context) (int, error) {
		invalid := []MessageUpdate{
			{MessageID: "x", Delta: &schema.MessageDelta{MessageID: "x", Op: schema.BlockAppend, Text: "before start"}},
			{MessageID: "x", Snapshot: &schema.Message{ID: "different", Role: schema.RoleAssistant}},
		}
		for _, m := range invalid {
			if PublishMessage(ctx, m) == nil {
				return 0, errors.New("invalid message accepted")
			}
		}
		if PublishActivity(ctx, ActivityUpdate{ID: "a", Status: "typo"}) == nil {
			return 0, errors.New("invalid status accepted")
		}
		if PublishLifecycle(ctx, Lifecycle{Scope: ScopeNode, Phase: "typo", Name: "n"}) == nil {
			return 0, errors.New("invalid phase accepted")
		}
		d := schema.MessageDelta{MessageID: "m", Op: schema.MessageStart, Role: schema.RoleAssistant}
		if err := PublishMessage(ctx, MessageUpdate{MessageID: "m", Delta: &d}); err != nil {
			return 0, err
		}
		if PublishMessage(ctx, MessageUpdate{MessageID: "m", Delta: &d}) == nil {
			return 0, errors.New("duplicate start accepted")
		}
		return 1, nil
	})
	defer r.Close()
	for {
		_, err := r.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	bad := Event[int]{Kind: KindResult, Activity: &ActivityUpdate{ID: "a", Status: ActivityStarted}}
	if bad.Validate() == nil {
		t.Fatal("kind mismatch accepted")
	}
}

func TestCanceledRunClosesPendingUIState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := Run(ctx, func(ctx context.Context) (int, error) {
		_ = PublishLifecycle(ctx, Lifecycle{Scope: ScopeNode, Phase: PhaseStart, Name: "query"})
		StartActivity(ctx, "query")
		cancel()
		return 0, ctx.Err()
	}, Config{Buffer: 8, Lifecycle: true})
	defer r.Close()
	canceledActivity, canceledNode := false, false
	for {
		e, err := r.Recv()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			break
		}
		if e.Activity != nil && e.Activity.Status == ActivityCanceled {
			canceledActivity = true
		}
		if e.Lifecycle != nil && e.Lifecycle.Phase == PhaseCanceled {
			canceledNode = true
		}
	}
	if !canceledActivity || !canceledNode {
		t.Fatalf("activity=%v node=%v", canceledActivity, canceledNode)
	}
}
