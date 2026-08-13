package llm_test

import (
	"context"
	"testing"

	"goharness/pkg/callback"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
)

func TestChatModelCallbackReceivesIsolatedRequest(t *testing.T) {
	secondSawOriginal := false
	first := llm.CallbackFuncs{CallbackName: "first", Start: func(_ context.Context, request llm.Request) error {
		request.Messages[0].Extra["owner"] = "changed"
		return nil
	}}
	second := llm.CallbackFuncs{CallbackName: "second", Start: func(_ context.Context, request llm.Request) error {
		secondSawOriginal = request.Messages[0].Extra["owner"] == "original"
		return nil
	}}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{first, second})
	ctx := callback.WithManager(context.Background(), manager)
	request := llm.Request{Messages: []schema.Message{{Role: schema.RoleUser, Content: schema.TextContent("hello"), Extra: map[string]any{"owner": "original"}}}}
	llm.BeginCallback(ctx, "model", nil, request)
	if !secondSawOriginal || request.Messages[0].Extra["owner"] != "original" {
		t.Fatal("ChatModel callback mutation leaked")
	}
}
