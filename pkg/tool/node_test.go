package tool_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"goharness/pkg/callback"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

func TestToolsNodeDispatchesLifecycleCallbacksWithoutChangingResult(t *testing.T) {
	events := make(chan string, 3)
	observer := tool.CallbackFuncs{
		CallbackName: "tool-audit",
		Start:        func(context.Context, tool.Spec, tool.CallInput) error { events <- "start"; return nil },
		End:          func(context.Context, tool.Spec, tool.CallInput, tool.Result) error { events <- "end"; return nil },
	}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	if err != nil {
		t.Fatal(err)
	}
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(callback.WithManager(context.Background(), manager), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err != nil || len(output) != 1 || output[0].Text() != "ok" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
	close(events)
	var got []string
	for event := range events {
		got = append(got, event)
	}
	if fmt.Sprint(got) != "[start end]" {
		t.Fatalf("events=%v", got)
	}
}

func TestToolsNodeCallbackFailureDoesNotChangeBusinessResult(t *testing.T) {
	observer := tool.CallbackFuncs{CallbackName: "broken", Start: func(context.Context, tool.Spec, tool.CallInput) error { return errors.New("audit failed") }}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	if err != nil {
		t.Fatal(err)
	}
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(callback.WithManager(context.Background(), manager), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err != nil || len(output) != 1 || output[0].Text() != "ok" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestToolsNodeExecutesOnlyTrailingCallsAndPreservesOrder(t *testing.T) {
	var mu sync.Mutex
	started := []string{}
	first := tool.NewFunctionTool(tool.Spec{Name: "first", Concurrency: tool.ConcurrencyPolicy{Group: "first", MaxParallel: 1}}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		mu.Lock()
		started = append(started, "first")
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return tool.TextResult("one:" + input.Args["value"].(string)), nil
	})
	second := tool.NewFunctionTool(tool.Spec{Name: "second", Concurrency: tool.ConcurrencyPolicy{Group: "second", MaxParallel: 1}}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		mu.Lock()
		started = append(started, "second")
		mu.Unlock()
		return tool.TextResult("two:" + input.Args["value"].(string)), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{first, second}, MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	messages := []schema.Message{
		schema.UserMessage("old"),
		schema.AssistantMessage("old call"),
		toolCallMessage("first", `{"value":"a"}`, "id-1"),
		toolCallMessage("second", `{"value":"b"}`, "id-2"),
	}
	output, err := node.Invoke(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 2 || output[0].Text() != "one:a" || output[1].Text() != "two:b" {
		t.Fatalf("output = %#v", output)
	}
	if output[0].FunctionID("") != "id-1" || output[1].FunctionID("") != "id-2" {
		t.Fatalf("function IDs = %q, %q", output[0].FunctionID(""), output[1].FunctionID(""))
	}
	if len(started) != 2 {
		t.Fatalf("started = %#v", started)
	}

	noCall, err := node.Invoke(context.Background(), append(messages[:2], schema.FunctionMessage("first", schema.TextContent("already done"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(noCall) != 0 {
		t.Fatalf("historical call was re-executed: %#v", noCall)
	}
}

func TestToolsNodeSerializesZeroPolicyAcrossInvocations(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	value := tool.NewFunctionTool(tool.Spec{Name: "mutate"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		started <- struct{}{}
		select {
		case <-ctx.Done():
			return tool.Result{}, ctx.Err()
		case <-release:
			return tool.TextResult("ok"), nil
		}
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{value}, MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	invoke := func(id string) {
		_, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("mutate", `{}`, id)})
		done <- err
	}
	go invoke("first")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first call did not start")
	}
	go invoke("second")
	select {
	case <-started:
		t.Fatal("zero-policy calls ran concurrently")
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second call did not start")
	}
	release <- struct{}{}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestToolsNodeRejectsConflictingGroupLimits(t *testing.T) {
	first := tool.NewFunctionTool(tool.Spec{Name: "first", Concurrency: tool.ConcurrencyPolicy{Group: "shared", MaxParallel: 1}}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("first"), nil
	})
	second := tool.NewFunctionTool(tool.Spec{Name: "second", Concurrency: tool.ConcurrencyPolicy{Group: "shared", MaxParallel: 2}}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("second"), nil
	})
	_, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{first, second}})
	if err == nil || !strings.Contains(err.Error(), "already has max parallel") {
		t.Fatalf("group limit error=%v", err)
	}
}

func TestToolsNodeSharesMaxConcurrencyAcrossInvocations(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	makeTool := func(name string) tool.BaseTool {
		return tool.NewFunctionTool(tool.Spec{Name: name, Concurrency: tool.ConcurrencyPolicy{Group: name, MaxParallel: 1}}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
			started <- name
			select {
			case <-ctx.Done():
				return tool.Result{}, ctx.Err()
			case <-release:
				return tool.TextResult(name), nil
			}
		})
	}
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{makeTool("first"), makeTool("second")}, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	go func() {
		_, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("first", `{}`, "first")})
		done <- err
	}()
	select {
	case name := <-started:
		if name != "first" {
			t.Fatalf("started=%q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("first did not start")
	}
	go func() {
		_, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("second", `{}`, "second")})
		done <- err
	}()
	select {
	case name := <-started:
		t.Fatalf("node limit allowed %q", name)
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case name := <-started:
		if name != "second" {
			t.Fatalf("started=%q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("second did not start")
	}
	release <- struct{}{}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestToolsNodeConvertsErrorsToAppendOnlyToolMessages(t *testing.T) {
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{
		tool.NewFunctionTool(tool.Spec{Name: "ok"}, func(context.Context, tool.CallInput) (tool.Result, error) {
			return tool.TextResult("ok"), nil
		}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(context.Background(), []schema.Message{
		toolCallMessage("missing", `{}`, "missing-1"),
		toolCallMessage("ok", `{bad}`, "bad-1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 2 || output[0].Extra["tool_error"] != true || output[1].Extra["tool_error"] != true {
		t.Fatalf("error outputs = %#v", output)
	}
}

func TestToolsNodeDeepClonesMessagesForConcurrentCalls(t *testing.T) {
	firstSet := make(chan struct{})
	secondSet := make(chan struct{})
	first := tool.NewFunctionTool(tool.Spec{Name: "first", Concurrency: tool.ConcurrencyPolicy{Group: "first", MaxParallel: 1}}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		nested := input.Messages[0].Extra["nested"].(map[string]any)
		nested["owner"] = "first"
		close(firstSet)
		<-secondSet
		if nested["owner"] != "first" {
			return tool.Result{}, fmt.Errorf("first tool observed shared owner %#v", nested["owner"])
		}
		return tool.TextResult("first"), nil
	})
	second := tool.NewFunctionTool(tool.Spec{Name: "second", Concurrency: tool.ConcurrencyPolicy{Group: "second", MaxParallel: 1}}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		<-firstSet
		nested := input.Messages[0].Extra["nested"].(map[string]any)
		nested["owner"] = "second"
		close(secondSet)
		if nested["owner"] != "second" {
			return tool.Result{}, fmt.Errorf("second tool observed shared owner %#v", nested["owner"])
		}
		return tool.TextResult("second"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{first, second}, MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	contextMessage := schema.UserMessage("context")
	contextMessage.Extra = map[string]any{"nested": map[string]any{"owner": "original"}}
	messages := []schema.Message{
		contextMessage,
		toolCallMessage("first", `{}`, "id-1"),
		toolCallMessage("second", `{}`, "id-2"),
	}
	output, err := node.Invoke(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 2 || output[0].Extra["tool_error"] != false || output[1].Extra["tool_error"] != false {
		t.Fatalf("tool output = %#v", output)
	}
	if messages[0].Extra["nested"].(map[string]any)["owner"] != "original" {
		t.Fatalf("source messages were mutated: %#v", messages[0].Extra)
	}
}

func toolCallMessage(name, args, id string) schema.Message {
	message := schema.AssistantMessage("")
	message.FunctionCall = &schema.FunctionCall{Name: name, Arguments: args}
	message.Extra = map[string]any{"function_id": id}
	return message
}

var _ runnable.Runnable[[]schema.Message, []schema.Message] = (*tool.ToolsNode)(nil)
