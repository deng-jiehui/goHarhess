package agent_test

import (
	"context"
	"errors"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
	"io"
	"testing"
	"time"
)

func TestAgentPublicStreamHasSnapshotAndSingleFinalResult(t *testing.T) {
	a, err := agent.NewBasicAgent(agent.BaseConfig{Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("hello")})})
	if err != nil {
		t.Fatal(err)
	}
	r, err := agent.StreamEvents(context.Background(), a, []schema.Message{schema.UserMessage("hi")}, stream.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	messages, results := 0, 0
	var messageID string
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, ns := range e.Meta.Namespace {
			if ns == "" {
				t.Fatalf("empty namespace element: %+v", e.Meta)
			}
		}
		if e.Message != nil {
			messages++
			messageID = e.Message.MessageID
		}
		if e.Result != nil {
			results++
			if (*e.Result)[0].ID != messageID || (*e.Result)[0].JoinedText("") != "hello" {
				t.Fatal(e)
			}
		}
	}
	if messages != 1 || results != 1 {
		t.Fatalf("messages=%d results=%d", messages, results)
	}
}

func TestAgentPublicStreamDoesNotPublishResultAfterCommitFailure(t *testing.T) {
	a, err := agent.NewBasicAgent(agent.BaseConfig{Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("answer")}), CheckpointStore: blockingCheckpointStore{}}, agent.WithCheckpointTimeout(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	r, err := agent.StreamEvents(context.Background(), a, []schema.Message{schema.UserMessage("hi")}, stream.Config{Buffer: 8}, agent.WithConversationID("commit-failure"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for {
		e, err := r.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatal("commit failure lost")
			}
			break
		}
		if e.Result != nil {
			t.Fatal("success before commit")
		}
	}
}
