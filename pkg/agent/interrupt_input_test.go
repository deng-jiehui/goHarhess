package agent_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
	"github.com/goharness/goharness/pkg/tool"
)

func TestAgentResumeInputControlsToolAdmission(t *testing.T) {
	for _, approved := range []bool{true, false} {
		name := "rejected"
		if approved {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			denied := errors.New("input rejected by application")
			value, err := agent.New(agent.BaseConfig{Name: "interactive-agent", Model: llm.NewScriptedModel(
				[]schema.Message{functionCallMessage("echo", "call-1")}, []schema.Message{schema.AssistantMessage("finished")}),
				Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
					calls.Add(1)
					return tool.TextResult("done"), nil
				})},
			}, agent.WithBeforeToolBatch(tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
				input, err := graph.Interrupt(ctx, map[string]any{"calls": batch.Calls})
				if err != nil {
					return err
				}
				if input != true {
					return denied
				}
				return nil
			})))
			if err != nil {
				t.Fatal(err)
			}
			option := agent.WithCheckpoint(&agentExecutionStore{}, "input")
			_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, option)
			var suspended *graph.InterruptedError
			if !errors.As(err, &suspended) || calls.Load() != 0 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
			pending := suspended.PendingInterrupts()
			if len(pending) != 1 {
				t.Fatal(pending)
			}
			r, err := agent.ResumeStreamEvents(context.Background(), value, stream.Config{Lifecycle: true}, option, agent.WithResume(pending[0].ID, approved))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			results := 0
			var terminal error
			for {
				e, err := r.Recv()
				if err != nil {
					terminal = err
					break
				}
				if e.Result != nil {
					results++
					messages := *e.Result
					if messages[len(messages)-1].JoinedText("") != "finished" {
						t.Fatal(messages)
					}
				}
			}
			if approved {
				if calls.Load() != 1 || results != 1 || !errors.Is(terminal, io.EOF) {
					t.Fatalf("calls=%d results=%d err=%v", calls.Load(), results, terminal)
				}
			} else {
				if calls.Load() != 0 || results != 0 || !errors.Is(terminal, denied) {
					t.Fatalf("calls=%d results=%d err=%v", calls.Load(), results, terminal)
				}
			}
		})
	}
}
