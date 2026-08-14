package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func TestAgentStreamPublishesLiveOutputBeforeFinalResultIsConsumed(t *testing.T) {
	model := &burstModel{produced: make(chan struct{})}
	value, err := agent.NewBasicAgent(
		agent.BaseConfig{Name: "async", Model: model},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := value.Stream(
		ctx,
		[]schema.Message{schema.UserMessage("burst")},
		agent.WithDeltaStream(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	output, err := stream.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 || output[0].JoinedText("") != "01234567" {
		t.Fatalf("output=%#v", output)
	}
	select {
	case <-model.produced:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("model producer did not finish")
	}
}

func TestBasicAgentRejectsModelStreamWithoutDone(t *testing.T) {
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: incompleteModel{}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("truncated")})
	if err == nil || !strings.Contains(err.Error(), "without a result") {
		t.Fatalf("Invoke error = %v", err)
	}
}

type burstModel struct {
	produced chan struct{}
}

func (m *burstModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return stream.Collect(ctx)
}

func (m *burstModel) Stream(ctx context.Context, request llm.Request, _ ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() {
		defer close(m.produced)
		accumulator := llm.NewDeltaAccumulator(request.Options.DeltaStream)
		for index := range 8 {
			messages, err := accumulator.Apply(llm.Response{Messages: []schema.Message{schema.AssistantMessage(fmt.Sprint(index))}})
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			if err := writer.Send(llm.Response{Messages: messages}); err != nil {
				return
			}
		}
		_ = writer.Close()
	}()
	return reader, nil
}

func (m *burstModel) Capabilities() llm.Capabilities {
	return llm.Capabilities{Streaming: true}
}

func (m *burstModel) ModelInfo() llm.ModelInfo {
	return llm.ModelInfo{Name: "burst", Provider: "test", ModelType: "scripted"}
}
func (m *burstModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m *burstModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}

type incompleteModel struct{}

func (incompleteModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
	stream, err := (incompleteModel{}).Stream(ctx, request, options...)
	if err != nil {
		return llm.Response{}, err
	}
	return stream.Collect(ctx)
}

func (incompleteModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	reader, writer := runnable.Pipe[llm.Response](ctx)
	go func() { _ = writer.Close() }()
	return reader, nil
}

func (incompleteModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (incompleteModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "incomplete"} }
func (m incompleteModel) Collect(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, in, m.Invoke, opts...)
}
func (m incompleteModel) Transform(ctx context.Context, in *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, in, m.Invoke, opts...)
}
