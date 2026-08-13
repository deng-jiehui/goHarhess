package tool_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"goharness/pkg/control"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestBeforeBatchInterruptsEveryToolBeforeExecution(t *testing.T) {
	var toolCalls atomic.Int32
	read := tool.NewFunctionTool(tool.Spec{Name: "read", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) {
		toolCalls.Add(1)
		return tool.TextResult("read"), nil
	})
	write := tool.NewFunctionTool(tool.Spec{Name: "write"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		toolCalls.Add(1)
		return tool.TextResult("write"), nil
	})
	hookCalls := 0
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.BaseTool{read, write},
		BeforeBatch: []tool.BeforeBatchHook{tool.BeforeBatchFunc(func(_ context.Context, batch tool.Batch) error {
			hookCalls++
			if len(batch.Calls) != 2 || batch.Calls[0].Spec.Name != "read" || batch.Calls[1].Spec.Name != "write" {
				t.Fatalf("batch=%#v", batch)
			}
			return control.RequestInterrupt("approve write", map[string]any{"tool": "write"})
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	messages := []schema.Message{
		toolCallMessage("read", `{"value":"one"}`, "call-1"),
		toolCallMessage("write", `{"value":"two"}`, "call-2"),
	}
	_, err = node.Invoke(context.Background(), messages)
	request, interrupted := control.AsInterrupt(err)
	if !interrupted || request.Reason != "approve write" {
		t.Fatalf("Invoke error=%v request=%#v", err, request)
	}
	if hookCalls != 1 || toolCalls.Load() != 0 {
		t.Fatalf("hook calls=%d tool calls=%d", hookCalls, toolCalls.Load())
	}
}

func TestBeforeBatchReceivesDefensiveSnapshotAndSubject(t *testing.T) {
	value := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		if input.Args["value"] != "original" {
			t.Fatalf("tool args=%#v", input.Args)
		}
		return tool.TextResult("ok"), nil
	})
	hook := tool.BeforeBatchFunc(func(_ context.Context, batch tool.Batch) error {
		if batch.Subject.ID != "user-1" || batch.Subject.Roles[0] != "operator" {
			t.Fatalf("subject=%#v", batch.Subject)
		}
		batch.Calls[0].Args["value"] = "mutated"
		batch.Calls[0].Spec.Parameters["mutated"] = true
		batch.Subject.Roles[0] = "mutated"
		return nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{value}, BeforeBatch: []tool.BeforeBatchHook{hook}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := tool.WithSubject(context.Background(), tool.Subject{ID: "user-1", Roles: []tool.Role{"operator"}})
	output, err := node.Invoke(ctx, []schema.Message{toolCallMessage("echo", `{"value":"original"}`, "call-1")})
	if err != nil || len(output) != 1 || output[0].Text() != "ok" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
	subject, ok := tool.SubjectFromContext(ctx)
	if !ok || subject.Roles[0] != "operator" {
		t.Fatalf("context subject=%#v ok=%t", subject, ok)
	}
}

func TestBeforeBatchIsolatesHooksFromEachOther(t *testing.T) {
	value := tool.NewFunctionTool(tool.Spec{Name: "echo", Parameters: map[string]any{"owner": "original"}}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	first := tool.BeforeBatchFunc(func(_ context.Context, batch tool.Batch) error {
		batch.Calls[0].Args["value"] = "changed"
		batch.Calls[0].Spec.Parameters["owner"] = "changed"
		return nil
	})
	second := tool.BeforeBatchFunc(func(_ context.Context, batch tool.Batch) error {
		if batch.Calls[0].Args["value"] != "original" || batch.Calls[0].Spec.Parameters["owner"] != "original" {
			t.Fatalf("second hook observed mutation: %#v", batch.Calls[0])
		}
		return nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools:       []tool.BaseTool{value},
		BeforeBatch: []tool.BeforeBatchHook{first, second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("echo", `{"value":"original"}`, "call-1")}); err != nil {
		t.Fatal(err)
	}
}

func TestBeforeBatchStopsAtFirstErrorAndRecoversPanic(t *testing.T) {
	value := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("unused"), nil
	})
	secondCalls := 0
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{value}, BeforeBatch: []tool.BeforeBatchHook{
		tool.BeforeBatchFunc(func(context.Context, tool.Batch) error { panic("broken approval") }),
		tool.BeforeBatchFunc(func(context.Context, tool.Batch) error { secondCalls++; return nil }),
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = node.Invoke(context.Background(), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err == nil || errors.Is(err, control.ErrInterruptRequested) || secondCalls != 0 {
		t.Fatalf("Invoke error=%v second calls=%d", err, secondCalls)
	}
}
