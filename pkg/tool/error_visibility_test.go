package tool_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// invokeOne 调用单个工具并返回模型可见的结果文本。
func invokeOne(t *testing.T, value tool.Tool, args string) (string, bool) {
	t.Helper()
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{value}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage(value.Spec().Name, args, "call-1")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("messages=%d", len(out))
	}
	return toolResultText(out[0]), toolResultIsError(out[0])
}

// 内部错误默认脱敏：模型只看到通用失败，完整错误保留在 callback 侧。
func TestToolErrorSanitizedByDefault(t *testing.T) {
	internal := errors.New("dial tcp 10.0.0.8:5432: connection refused (secret db)")
	var captured error
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind == observe.KindTool && event.Phase == observe.PhaseError {
			captured = event.Err
		}
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "db"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.Result{}, internal
	})}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := node.Invoke(observe.With(context.Background(), observer), []schema.Message{toolCallMessage("db", `{}`, "call-1")})
	if err != nil {
		t.Fatal(err)
	}
	text := toolResultText(out[0])
	if !toolResultIsError(out[0]) || text != `tool "db" failed` {
		t.Fatalf("model text=%q isError=%v", text, toolResultIsError(out[0]))
	}
	if strings.Contains(text, "10.0.0.8") || strings.Contains(text, "secret") {
		t.Fatalf("internal detail leaked: %q", text)
	}
	// callback 仍收到完整错误。
	if !errors.Is(captured, internal) {
		t.Fatalf("callback error=%v", captured)
	}
}

// 工具显式声明的 ModelVisible 反馈原样到达模型。
func TestToolModelVisibleErrorReachesModel(t *testing.T) {
	text, isError := invokeOne(t, tool.NewFunctionTool(tool.Spec{Name: "quota"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.Result{}, tool.ModelVisible(errors.New("rate limited, retry in 30s"))
	}), `{}`)
	if !isError || text != `tool "quota" failed: rate limited, retry in 30s` {
		t.Fatalf("text=%q isError=%v", text, isError)
	}
}

// 参数语法错误与 schema 违规是框架为模型自纠设计的可见错误。
func TestArgsErrorsVisibleForSelfCorrection(t *testing.T) {
	params := map[string]any{
		"type":       "object",
		"properties": map[string]any{"q": map[string]any{"type": "string"}},
		"required":   []any{"q"},
	}
	search := tool.NewFunctionTool(tool.Spec{Name: "search", Parameters: params}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	text, isError := invokeOne(t, search, `{invalid`)
	if !isError || !strings.Contains(text, `tool "search" failed:`) || !strings.Contains(text, "JSON") {
		t.Fatalf("malformed args text=%q", text)
	}
	text, isError = invokeOne(t, search, `{}`)
	if !isError || !strings.Contains(text, "violate the declared schema") {
		t.Fatalf("schema violation text=%q", text)
	}
}

// 未注册工具名、权限拒绝与超时各有确定的模型可见语义。
func TestToolErrorVisibilityMatrix(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{echo}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("ghost", `{}`, "call-1")})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResultText(out[0]); got != `tool "ghost" does not exist` {
		t.Fatalf("unknown tool text=%q", got)
	}

	// 权限拒绝：理由未标 ModelVisible 时不暴露（连中间件名也不给）。
	deny := tool.MiddlewareFunc(func(_ context.Context, _ tool.Spec, _ tool.CallInput, _ tool.Next) (tool.Result, error) {
		return tool.Result{}, tool.Deny(errors.New("ldap://internal.corp unreachable"))
	})
	node, err = tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{echo}, Middlewares: []tool.Middleware{deny}})
	if err != nil {
		t.Fatal(err)
	}
	out, err = node.Invoke(context.Background(), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResultText(out[0]); got != `tool "echo" permission denied` {
		t.Fatalf("denied text=%q", got)
	}

	// Deny 理由显式可见时附带理由。
	denyVisible := tool.MiddlewareFunc(func(_ context.Context, _ tool.Spec, _ tool.CallInput, _ tool.Next) (tool.Result, error) {
		return tool.Result{}, tool.Deny(tool.ModelVisible(errors.New("missing scope write:files")))
	})
	node, err = tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{echo}, Middlewares: []tool.Middleware{denyVisible}})
	if err != nil {
		t.Fatal(err)
	}
	out, err = node.Invoke(context.Background(), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResultText(out[0]); got != `tool "echo" permission denied: missing scope write:files` {
		t.Fatalf("denied-visible text=%q", got)
	}

	// 超时：模型得到稳定的 timed out 语义而非原始 context 错误。
	slow := tool.NewFunctionTool(tool.Spec{Name: "slow"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		<-ctx.Done()
		return tool.Result{}, ctx.Err()
	})
	node, err = tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{slow}, Timeout: 5_000_000})
	if err != nil {
		t.Fatal(err)
	}
	out, err = node.Invoke(context.Background(), []schema.Message{toolCallMessage("slow", `{}`, "call-1")})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResultText(out[0]); got != `tool "slow" timed out` {
		t.Fatalf("timeout text=%q", got)
	}
}

// 工具 panic 的恢复值（可能含内部细节）也不进模型文本。
func TestToolPanicErrorSanitized(t *testing.T) {
	text, isError := invokeOne(t, tool.NewFunctionTool(tool.Spec{Name: "fragile"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		panic("db dump at /etc/secrets/prod.sql")
	}), `{}`)
	if !isError || text != `tool "fragile" failed` {
		t.Fatalf("panic text=%q isError=%v", text, isError)
	}
}

// 参数错误被业务代码包装（fmt.Errorf %w）或与内部错误合并（errors.Join）后，
// 模型只看到参数错误自身的提示，外层内部诊断不外泄。
func TestWrappedArgsErrorDoesNotLeakOuterContext(t *testing.T) {
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	invalid := &tool.InvalidArgsError{ToolName: "echo", Err: errors.New(`missing required property "q"`)}
	cases := []struct {
		name string
		err  error
	}{
		{"fmt wrap", fmt.Errorf("tenant ctx shard=10.0.0.8 secret=x: %w", invalid)},
		{"errors.Join", errors.Join(errors.New("dial internal-ldap.corp:389"), invalid)},
		{"joined malformed", errors.Join(fmt.Errorf("pipeline failed at host db-01: %w", tool.ErrMalformedArgs), errors.New("cleanup failed"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mw := tool.MiddlewareFunc(func(_ context.Context, _ tool.Spec, _ tool.CallInput, _ tool.Next) (tool.Result, error) {
				return tool.Result{}, tc.err
			})
			node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{echo}, Middlewares: []tool.Middleware{mw}})
			if err != nil {
				t.Fatal(err)
			}
			out, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("echo", `{}`, "call-1")})
			if err != nil {
				t.Fatal(err)
			}
			text := toolResultText(out[0])
			if !toolResultIsError(out[0]) {
				t.Fatalf("not an error result: %#v", out[0])
			}
			// 内部上下文不得外泄。
			for _, leak := range []string{"10.0.0.8", "secret", "internal-ldap", "db-01", "cleanup"} {
				if strings.Contains(text, leak) {
					t.Fatalf("leaked %q in %q", leak, text)
				}
			}
			// 参数错误仍保留可用的自纠提示。
			if tc.name == "joined malformed" {
				if !strings.Contains(text, "valid JSON object") {
					t.Fatalf("lost args hint: %q", text)
				}
			} else if !strings.Contains(text, `missing required property "q"`) {
				t.Fatalf("lost schema hint: %q", text)
			}
		})
	}
}

// 结果转换失败属调用生命周期内的失败：模型收到脱敏 isError，
// 诊断侧走错误回调拿完整错误，不会先收到成功终态。
func TestResultConversionFailureReportsErrorEvent(t *testing.T) {
	endCalls, errorCalls := 0, 0
	var captured error
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind != observe.KindTool {
			return
		}
		switch event.Phase {
		case observe.PhaseEnd:
			endCalls++
		case observe.PhaseError:
			errorCalls++
			captured = event.Err
		}
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "db"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.Result{Data: func() {}}, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := node.Invoke(observe.With(context.Background(), observer), []schema.Message{toolCallMessage("db", `{}`, "call-1")})
	if err != nil {
		t.Fatal(err)
	}
	text := toolResultText(out[0])
	if !toolResultIsError(out[0]) || text != `tool "db" failed` {
		t.Fatalf("model text=%q isError=%v", text, toolResultIsError(out[0]))
	}
	if errorCalls != 1 || endCalls != 0 {
		t.Fatalf("end/error=%d/%d", endCalls, errorCalls)
	}
	if captured == nil || !strings.Contains(captured.Error(), "convert") {
		t.Fatalf("callback error=%v", captured)
	}
}
