package llm_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

func TestChatModelNodeOwnsOneLifecycleAndPreservesResponse(t *testing.T) {
	var starts, ends int
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{llm.CallbackFuncs{
		CallbackName: "audit",
		Start: func(context.Context, llm.Request) error {
			starts++
			return nil
		},
		End: func(context.Context, llm.Response) error {
			ends++
			return nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	node, err := llm.NewChatModelNode(llm.NewScriptedModel([]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("done"))}))
	if err != nil {
		t.Fatal(err)
	}
	response, err := node.Invoke(callback.WithManager(context.Background(), manager), llm.Request{
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
