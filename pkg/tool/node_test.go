package tool_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
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
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(callback.WithManager(context.Background(), manager), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err != nil || len(output) != 1 || toolResultText(output[0]) != "ok" {
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

func TestToolsNodeHonorsPerInvocationConcurrency(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	makeTool := func(name string) tool.Tool {
		return tool.NewFunctionTool(tool.Spec{Name: name}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
			started <- name
			select {
			case <-ctx.Done():
				return tool.Result{}, ctx.Err()
			case <-release:
				return tool.TextResult(name), nil
			}
		})
	}
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{makeTool("first"), makeTool("second")}})
	if err != nil {
		t.Fatal(err)
	}
	messages := []schema.Message{
		toolCallMessage("first", `{}`, "call-1"),
		toolCallMessage("second", `{}`, "call-2"),
	}
	done := make(chan error, 1)
	go func() {
		_, invokeErr := node.Invoke(context.Background(), messages, tool.WithMaxConcurrency(1))
		done <- invokeErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first tool did not start")
	}
	select {
	case name := <-started:
		t.Fatalf("tool %q started before the per-invocation slot was released", name)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestToolsNodeBuildsCallInputFromRuntimeContext(t *testing.T) {
	var received tool.CallInput
	read := tool.NewFunctionTool(tool.Spec{Name: "read", FileAccess: true}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		received = input
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{read}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithMetadata(context.Background(), map[string]any{"tenant": "acme"})
	messages := []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewFileBlock("guide.pdf", "application/pdf")),
		toolCallMessage("read", `{}`, "call-1"),
	}
	if _, err := node.Invoke(ctx, messages); err != nil {
		t.Fatal(err)
	}
	if received.Metadata["tenant"] != "acme" {
		t.Fatalf("metadata = %#v", received.Metadata)
	}
	if len(received.Files) != 1 || received.Files[0] != "guide.pdf" {
		t.Fatalf("files = %#v", received.Files)
	}
}

func TestToolsNodeCallExtraReachesMetadataWithoutLegacyCorrelationOverride(t *testing.T) {
	var received tool.CallInput
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		received = input
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{echo}})
	if err != nil {
		t.Fatal(err)
	}
	call := schema.NewFunctionToolCallBlock("call-1", "echo", `{}`)
	call.FunctionToolCall.Extra = map[string]any{"trace": "abc"}
	message := schema.NewMessage(schema.RoleAssistant, call)
	message.Extra = map[string]any{"function_id": "wrong-id", "name": "wrong-name", "legacy": true}
	output, err := node.Invoke(context.Background(), []schema.Message{message})
	if err != nil {
		t.Fatal(err)
	}
	if received.Metadata["trace"] != "abc" {
		t.Fatalf("metadata = %#v", received.Metadata)
	}
	results := output[0].FunctionToolResults()
	if len(results) != 1 || results[0].CallID != "call-1" || results[0].Name != "echo" {
		t.Fatalf("results = %#v", results)
	}
}

func TestToolsNodeCallbackFailureDoesNotChangeBusinessResult(t *testing.T) {
	observer := tool.CallbackFuncs{CallbackName: "broken", Start: func(context.Context, tool.Spec, tool.CallInput) error { return errors.New("audit failed") }}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	if err != nil {
		t.Fatal(err)
	}
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(callback.WithManager(context.Background(), manager), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err != nil || len(output) != 1 || toolResultText(output[0]) != "ok" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestToolsNodeUsesPublicRuntimeMiddleware(t *testing.T) {
	var middlewareCalls int
	middleware := tool.MiddlewareFunc(func(ctx context.Context, spec tool.Spec, input tool.CallInput, next tool.Next) (tool.Result, error) {
		middlewareCalls++
		input.Args["observed"] = true
		return next(ctx, input)
	})
	value := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		if input.Args["observed"] != true {
			return tool.Result{}, errors.New("middleware did not run")
		}
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools:       []tool.Tool{value},
		Middlewares: []tool.Middleware{middleware},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err != nil || len(output) != 1 || toolResultText(output[0]) != "ok" || middlewareCalls != 1 {
		t.Fatalf("output=%#v middlewareCalls=%d err=%v", output, middlewareCalls, err)
	}
}

func TestToolsNodeConfiguresReadOnlyRuntimeRetries(t *testing.T) {
	var calls int
	value := tool.NewFunctionTool(tool.Spec{Name: "read", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) {
		calls++
		if calls == 1 {
			return tool.Result{}, errors.New("temporary failure")
		}
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools:        []tool.Tool{value},
		MaxRetries:   1,
		RetryBackoff: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("read", `{}`, "call-1")})
	if err != nil || len(output) != 1 || toolResultText(output[0]) != "ok" || calls != 2 {
		t.Fatalf("output=%#v calls=%d err=%v", output, calls, err)
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
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{first, second}, MaxConcurrency: 2})
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
	if len(output) != 2 || toolResultText(output[0]) != "one:a" || toolResultText(output[1]) != "two:b" {
		t.Fatalf("output = %#v", output)
	}
	firstResults, secondResults := output[0].FunctionToolResults(), output[1].FunctionToolResults()
	if firstResults[0].CallID != "id-1" || secondResults[0].CallID != "id-2" {
		t.Fatalf("function IDs = %q, %q", firstResults[0].CallID, secondResults[0].CallID)
	}
	if len(started) != 2 {
		t.Fatalf("started = %#v", started)
	}

	noCall, err := node.Invoke(context.Background(), append(messages[:2], schema.NewMessage(schema.RoleTool,
		schema.NewFunctionToolResultBlock("id-1", "first", false, schema.NewTextBlock("already done")),
	)))
	if err != nil {
		t.Fatal(err)
	}
	if len(noCall) != 0 {
		t.Fatalf("historical call was re-executed: %#v", noCall)
	}
}

func TestToolsNodeExecutesMultipleBlocksFromOneMessage(t *testing.T) {
	firstTool := tool.NewFunctionTool(tool.Spec{Name: "first"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("one"), nil
	})
	secondTool := tool.NewFunctionTool(tool.Spec{Name: "second"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("two"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{firstTool, secondTool}})
	if err != nil {
		t.Fatal(err)
	}
	input := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("run both"),
		schema.NewFunctionToolCallBlock("call-1", "first", `{}`),
		schema.NewFunctionToolCallBlock("call-2", "second", `{}`),
	)}
	output, err := node.Invoke(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 2 {
		t.Fatalf("output=%d", len(output))
	}
	first := output[0].FunctionToolResults()
	second := output[1].FunctionToolResults()
	if len(first) != 1 || len(second) != 1 || first[0].CallID != "call-1" || second[0].CallID != "call-2" {
		t.Fatalf("results=%#v %#v", first, second)
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
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{value}, MaxConcurrency: 2})
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
	_, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{first, second}})
	if err == nil || !strings.Contains(err.Error(), "already has max parallel") {
		t.Fatalf("group limit error=%v", err)
	}
}

func TestToolsNodeSharesMaxConcurrencyAcrossInvocations(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	makeTool := func(name string) tool.Tool {
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
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{makeTool("first"), makeTool("second")}, MaxConcurrency: 1})
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
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{
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
	if len(output) != 2 || !toolResultIsError(output[0]) || !toolResultIsError(output[1]) {
		t.Fatalf("error outputs = %#v", output)
	}
}

func TestToolsNodeInjectsMessagesOnlyForNeedsMessages(t *testing.T) {
	var sawMessages []schema.Message
	seen := make(chan struct{})
	reader := tool.NewFunctionTool(tool.Spec{Name: "reader", NeedsMessages: true, Concurrency: tool.ConcurrencyPolicy{Group: "reader", MaxParallel: 1}}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		sawMessages = input.Messages
		// 只读视图：替换本调用持有的元素不得污染调用方的历史 slice。
		input.Messages[0] = schema.UserMessage("rewritten")
		close(seen)
		return tool.TextResult("reader"), nil
	})
	blind := tool.NewFunctionTool(tool.Spec{Name: "blind", Concurrency: tool.ConcurrencyPolicy{Group: "blind", MaxParallel: 1}}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		if input.Messages != nil {
			return tool.Result{}, fmt.Errorf("undeclared tool received %d messages", len(input.Messages))
		}
		return tool.TextResult("blind"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{reader, blind}, MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	contextMessage := schema.UserMessage("context")
	contextMessage.Extra = map[string]any{"nested": map[string]any{"owner": "original"}}
	messages := []schema.Message{
		contextMessage,
		toolCallMessage("reader", `{}`, "id-1"),
		toolCallMessage("blind", `{}`, "id-2"),
	}
	output, err := node.Invoke(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	<-seen
	if len(output) != 2 || toolResultIsError(output[0]) || toolResultIsError(output[1]) {
		t.Fatalf("tool output = %#v", output)
	}
	if len(sawMessages) != len(messages) || sawMessages[1].FunctionToolCalls()[0].Name != "reader" {
		t.Fatalf("declared tool messages = %#v", sawMessages)
	}
	if messages[0].Role != schema.RoleUser {
		t.Fatalf("caller message slice was mutated: %#v", messages[0])
	}
}

func toolCallMessage(name, args, id string) schema.Message {
	return schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(id, name, args))
}

func toolResultText(message schema.Message) string {
	results := message.FunctionToolResults()
	if len(results) != 1 || len(results[0].ContentBlocks) != 1 || results[0].ContentBlocks[0].Text == nil {
		return ""
	}
	return results[0].ContentBlocks[0].Text.Text
}

func toolResultIsError(message schema.Message) bool {
	results := message.FunctionToolResults()
	return len(results) == 1 && results[0].IsError
}

var _ runnable.Runnable[[]schema.Message, []schema.Message] = (*tool.ToolsNode)(nil)
