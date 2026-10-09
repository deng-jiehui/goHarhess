package tool_test

import (
	"context"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/tool"
)

type searchArgs struct {
	Query string `json:"query" jsonschema:"搜索关键词"`
	Limit int    `json:"limit,omitempty"`
}

// 泛型构造：Schema 从参数类型推导，jsonschema 标签成为描述。
func TestNewDerivesSchema(t *testing.T) {
	search := tool.New("search", "搜索订单",
		func(_ context.Context, a searchArgs) (string, error) {
			return a.Query, nil
		}, tool.ReadOnly())
	spec := search.Spec()
	if spec.Name != "search" || !spec.IsReadOnly {
		t.Fatalf("spec=%+v", spec)
	}
	params := spec.Parameters
	if params["type"] != "object" {
		t.Fatalf("params=%v", params)
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties=%v", params["properties"])
	}
	query, ok := props["query"].(map[string]any)
	if !ok || query["type"] != "string" || query["description"] != "搜索关键词" {
		t.Fatalf("query schema=%v", props["query"])
	}
	required, _ := params["required"].([]any)
	if len(required) != 1 || required[0] != "query" {
		t.Fatalf("required=%v", params["required"])
	}
}

// RawArgs 严格解码进类型化参数；未知字段拒绝。
func TestNewDecodesRawArgs(t *testing.T) {
	var got searchArgs
	search := tool.New("search", "搜索",
		func(_ context.Context, a searchArgs) (string, error) {
			got = a
			return "ok", nil
		})
	out, err := search.Invoke(context.Background(), tool.CallInput{
		RawArgs: `{"query":"q","limit":3}`,
	})
	if err != nil || got.Query != "q" || got.Limit != 3 {
		t.Fatalf("out=%v got=%+v err=%v", out, got, err)
	}
	if _, err := search.Invoke(context.Background(), tool.CallInput{
		RawArgs: `{"query":"q","extra":1}`,
	}); err == nil {
		t.Fatal("expected malformed args error for unknown field")
	}
}

// 无 RawArgs 时回退到 Args map；空参数走零值。
func TestNewArgsFallback(t *testing.T) {
	var got searchArgs
	search := tool.New("search", "搜索",
		func(_ context.Context, a searchArgs) (string, error) {
			got = a
			return "ok", nil
		})
	if _, err := search.Invoke(context.Background(), tool.CallInput{
		Args: map[string]any{"query": "m"},
	}); err != nil || got.Query != "m" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	got = searchArgs{}
	if _, err := search.Invoke(context.Background(), tool.CallInput{}); err != nil {
		t.Fatal(err)
	}
}

// 返回值映射：Result 原样，string 转文本，结构体转 Data。
func TestNewResultMapping(t *testing.T) {
	asResult := tool.New("r", "d",
		func(_ context.Context, _ searchArgs) (tool.Result, error) {
			return tool.ErrorResult(), nil
		})
	out, err := asResult.Invoke(context.Background(), tool.CallInput{})
	if err != nil || !out.IsError {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	asStruct := tool.New("s", "d",
		func(_ context.Context, _ searchArgs) (struct {
			Hits int `json:"hits"`
		}, error) {
			return struct {
				Hits int `json:"hits"`
			}{Hits: 2}, nil
		})
	out, err = asStruct.Invoke(context.Background(), tool.CallInput{})
	if err != nil || out.Data == nil {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	blocks, err := out.ToContentBlocks()
	if err != nil || len(blocks) != 1 || blocks[0].Text == nil || !strings.Contains(blocks[0].Text.Text, "hits") {
		t.Fatalf("blocks=%v err=%v", blocks, err)
	}
}

// 函数错误原样向上传递。
func TestNewPropagatesError(t *testing.T) {
	sentinel := errSentinel{}
	failing := tool.New("f", "d",
		func(_ context.Context, _ searchArgs) (string, error) { return "", sentinel })
	if _, err := failing.Invoke(context.Background(), tool.CallInput{}); err == nil {
		t.Fatal("expected sentinel error")
	}
}

type errSentinel struct{}

func (errSentinel) Error() string { return "sentinel" }
