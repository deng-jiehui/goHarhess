package llm

import (
	"context"
	"errors"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
	"io"
	"sync/atomic"
	"testing"
)

func TestWirePublicDeltasMatchFinalRepeatedText(t *testing.T) {
	events, _ := stream.Run(context.Background(), func(ctx context.Context) (Response, error) {
		wire := make(chan wireEvent, 3)
		for i := 0; i < 2; i++ {
			wire <- wireEvent{kind: wireEventDelta, messages: []schema.Message{schema.AssistantMessage("哈")}}
		}
		wire <- wireEvent{kind: wireEventDone}
		close(wire)
		r, err := responseReaderFromWire(ctx, wire, nil)
		if err != nil {
			return Response{}, err
		}
		return CollectResponse(ctx, r)
	})
	defer events.Close()
	a := schema.NewMessageAccumulator()
	var id string
	var chunks int
	for {
		e, err := events.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Message != nil && e.Message.Delta != nil {
			id = e.Message.MessageID
			d := *e.Message.Delta
			if d.Op == schema.BlockAppend {
				chunks++
			}
			if err := a.Apply(d); err != nil {
				t.Fatal(err)
			}
		}
		if e.Result != nil {
			if e.Result.Messages[0].ID != id || e.Result.Messages[0].JoinedText("") != "哈哈" {
				t.Fatalf("%+v", e.Result)
			}
		}
	}
	m, ok := a.Message(id)
	if !ok || m.JoinedText("") != "哈哈" || chunks != 2 {
		t.Fatalf("message=%+v chunks=%d", m, chunks)
	}
}

type publicFailingModel struct {
	*ScriptedModel
	calls atomic.Int32
}

func (m *publicFailingModel) Stream(ctx context.Context, _ Request, _ ...runnable.Option) (*runnable.StreamReader[Response], error) {
	m.calls.Add(1)
	_ = stream.PublishSnapshots(ctx, []schema.Message{schema.AssistantMessage("partial")})
	return nil, errors.New("temporary")
}
func TestPublicMessagePreventsRetry(t *testing.T) {
	base := &publicFailingModel{ScriptedModel: NewScriptedModel()}
	retry, _ := NewRetryingModel(base, RetryConfig{MaxAttempts: 3, Retryable: func(error) bool { return true }})
	r, _ := stream.Run(context.Background(), func(ctx context.Context) (Response, error) {
		s, err := retry.Stream(ctx, Request{})
		if err != nil {
			return Response{}, err
		}
		return CollectResponse(ctx, s)
	})
	defer r.Close()
	if e, err := r.Recv(); err != nil || e.Message == nil {
		t.Fatalf("%v %v", e, err)
	}
	if _, err := r.Recv(); err == nil {
		t.Fatal("missing error")
	}
	if base.calls.Load() != 1 {
		t.Fatalf("retried %d times", base.calls.Load())
	}
}
