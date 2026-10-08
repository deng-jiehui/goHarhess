package llm_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

func TestChatModelNodeOwnsOneLifecycleAndPreservesResponse(t *testing.T) {
	var starts, ends int
	collector := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind != observe.KindLLM {
			return
		}
		switch event.Phase {
		case observe.PhaseStart:
			starts++
		case observe.PhaseEnd:
			ends++
		}
	})
	node, err := llm.NewChatModelNode(llm.NewScriptedModel([]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("done"))}))
	if err != nil {
		t.Fatal(err)
	}
	response, err := node.Invoke(observe.With(context.Background(), collector), llm.Request{
		Messages: []schema.Message{schema.NewMessage(schema.RoleUser, schema.NewTextBlock("run"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Messages) != 1 || response.Messages[0].JoinedText("") != "done" {
		t.Fatalf("response = %#v", response)
	}
	if starts != 1 || ends != 1 {
		t.Fatalf("callbacks start/end = %d/%d", starts, ends)
	}
	if node.NodeKind() != runnable.NodeKindChatModel {
		t.Fatalf("node kind = %q", node.NodeKind())
	}
}
