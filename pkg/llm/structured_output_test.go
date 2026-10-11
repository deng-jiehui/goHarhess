package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/datamodel"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/google/jsonschema-go/jsonschema"
)

type decision struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason"`
}

type nestedAnswer struct {
	Title  string `json:"title"`
	Detail struct {
		Score float64 `json:"score"`
	} `json:"detail"`
}

type pointerFields struct {
	Name    string   `json:"name"`
	Comment *string  `json:"comment,omitempty"`
	Note    *int     `json:"note"`
	Tags    []string `json:"tags"`
}

func assistantOf(text string) schema.Message {
	return schema.AssistantMessage(text)
}

func newDecisionOutput(t *testing.T, options ...llm.StructuredOutputOption) *llm.StructuredOutput[decision] {
	t.Helper()
	output, err := llm.NewStructuredOutput[decision](options...)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func TestStructuredOutputDecodeSuccess(t *testing.T) {
	output := newDecisionOutput(t)
	value, err := output.Decode([]schema.Message{
		schema.UserMessage("决定?"),
		assistantOf(`{"approved":false,"reason":"拒绝"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if value.Approved != false || value.Reason != "拒绝" {
		t.Fatalf("value = %#v", value)
	}
}

func TestStructuredOutputDecodeZeroValuesAccepted(t *testing.T) {
	type zeros struct {
		Count int    `json:"count"`
		Flag  bool   `json:"flag"`
		Text  string `json:"text"`
	}
	output, err := llm.NewStructuredOutput[zeros]()
	if err != nil {
		t.Fatal(err)
	}
	value, err := output.Decode([]schema.Message{assistantOf(`{"count":0,"flag":false,"text":""}`)})
	if err != nil {
		t.Fatal(err)
	}
	if value.Count != 0 || value.Flag || value.Text != "" {
		t.Fatalf("value = %#v", value)
	}
}

func TestStructuredOutputDecodeMissingRequiredField(t *testing.T) {
	output := newDecisionOutput(t)
	// 缺少 approved 不得静默填充零值。
	value, err := output.Decode([]schema.Message{assistantOf(`{"reason":"ok"}`)})
	if !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	if value != (decision{}) {
		t.Fatalf("partial value delivered: %#v", value)
	}
}

func TestStructuredOutputDecodeFailures(t *testing.T) {
	output := newDecisionOutput(t)
	cases := map[string]string{
		"unknown field":     `{"approved":true,"reason":"ok","extra":1}`,
		"wrong type":        `{"approved":"yes","reason":"ok"}`,
		"malformed json":    `{"approved":true,`,
		"trailing text":     `{"approved":true,"reason":"ok"} tail`,
		"trailing value":    `{"approved":true,"reason":"ok"} {"x":1}`,
		"markdown fence":    "```json\n{\"approved\":true,\"reason\":\"ok\"}\n```",
		"prose around json": `sure: {"approved":true,"reason":"ok"}`,
		"null":              `null`,
		"array":             `[{"approved":true,"reason":"ok"}]`,
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			value, err := output.Decode([]schema.Message{assistantOf(text)})
			if !errors.Is(err, llm.ErrInvalidStructuredOutput) {
				t.Fatalf("err = %v", err)
			}
			if value != (decision{}) {
				t.Fatalf("partial value delivered: %#v", value)
			}
		})
	}
}

func TestStructuredOutputDecodeNumericOverflow(t *testing.T) {
	type small struct {
		N int8 `json:"n"`
	}
	output, err := llm.NewStructuredOutput[small]()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := output.Decode([]schema.Message{assistantOf(`{"n":300}`)}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	// UseNumber 精度：整数按原值进入目标类型。
	if value, err := output.Decode([]schema.Message{assistantOf(`{"n":-5}`)}); err != nil || value.N != -5 {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
}

func TestStructuredOutputPointerFields(t *testing.T) {
	output, err := llm.NewStructuredOutput[pointerFields]()
	if err != nil {
		t.Fatal(err)
	}
	// omitempty 指针字段仍为 required，但允许 null。
	value, err := output.Decode([]schema.Message{assistantOf(
		`{"name":"x","comment":null,"note":null,"tags":null}`)})
	if err != nil {
		t.Fatal(err)
	}
	if value.Comment != nil || value.Note != nil || value.Tags != nil {
		t.Fatalf("value = %#v", value)
	}
	// 非指针字段带 null 拒绝。
	if _, err := output.Decode([]schema.Message{assistantOf(
		`{"name":null,"comment":null,"note":null,"tags":null}`)}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	// omitempty 指针字段缺失仍拒绝：nullable 不等于 optional。
	if _, err := output.Decode([]schema.Message{assistantOf(
		`{"name":"x","note":null,"tags":null}`)}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
}

type anonInner struct {
	V string `json:"v"`
}

func TestStructuredOutputRejectsNamedAnonymousField(t *testing.T) {
	// 带显式 json 名的匿名内嵌字段：encoding/json 视为具名属性，但推导
	// Schema 不存在该属性，两侧不一致必须构造期拒绝。
	type BadOuter struct {
		anonInner `json:"in"`
	}
	if _, err := llm.NewStructuredOutput[BadOuter](); err == nil || !strings.Contains(err.Error(), "anonymous") {
		t.Fatalf("err = %v", err)
	}
}

func TestStructuredOutputRejectsNonPointerOmitEmpty(t *testing.T) {
	type bad struct {
		Name string `json:"name,omitempty"`
	}
	if _, err := llm.NewStructuredOutput[bad](); err == nil || !strings.Contains(err.Error(), "omitempty") {
		t.Fatalf("err = %v", err)
	}
}

func TestStructuredOutputRejectsNonStructTypes(t *testing.T) {
	if _, err := llm.NewStructuredOutput[*decision](); err == nil {
		t.Fatal("pointer target accepted")
	}
	if _, err := llm.NewStructuredOutput[string](); err == nil {
		t.Fatal("string target accepted")
	}
	if _, err := llm.NewStructuredOutput[[]decision](); err == nil {
		t.Fatal("slice target accepted")
	}
	if _, err := llm.NewStructuredOutput[map[string]any](); err == nil {
		t.Fatal("map target accepted")
	}
}

type customCodec struct{ Inner string }

func (c *customCodec) UnmarshalJSON([]byte) error  { return nil }
func (c customCodec) MarshalJSON() ([]byte, error) { return []byte(`"x"`), nil }

func TestStructuredOutputRejectsCustomCodec(t *testing.T) {
	type bad struct {
		Codec customCodec `json:"codec"`
	}
	if _, err := llm.NewStructuredOutput[bad](); err == nil || !strings.Contains(err.Error(), "codec") {
		t.Fatalf("err = %v", err)
	}
	// json.RawMessage 字段同样是自定义编解码，推导 schema 与解码不一致。
	type raw struct {
		Data json.RawMessage `json:"data"`
	}
	if _, err := llm.NewStructuredOutput[raw](); err == nil {
		t.Fatal("json.RawMessage field accepted")
	}
}

func TestStructuredOutputRejectsAnyField(t *testing.T) {
	// any 推导为不限制约束的 Schema，strict 无法约束该字段，构造期拒绝。
	type withAny struct {
		Data any `json:"data"`
	}
	if _, err := llm.NewStructuredOutput[withAny](); err == nil || !strings.Contains(err.Error(), "data") {
		t.Fatalf("err = %v", err)
	}
	type withNestedAny struct {
		Items []any `json:"items"`
	}
	if _, err := llm.NewStructuredOutput[withNestedAny](); err == nil {
		t.Fatal("[]any field accepted")
	}
}

func TestStructuredOutputNestedStrict(t *testing.T) {
	output, err := llm.NewStructuredOutput[nestedAnswer]()
	if err != nil {
		t.Fatal(err)
	}
	// 嵌套对象缺 required 字段拒绝。
	if _, err := output.Decode([]schema.Message{assistantOf(`{"title":"t","detail":{}}`)}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	// 嵌套对象多余字段拒绝。
	if _, err := output.Decode([]schema.Message{assistantOf(`{"title":"t","detail":{"score":1.5,"x":0}}`)}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	value, err := output.Decode([]schema.Message{assistantOf(`{"title":"t","detail":{"score":1.5}}`)})
	if err != nil || value.Detail.Score != 1.5 {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
}

func TestStructuredOutputMessageSelection(t *testing.T) {
	output := newDecisionOutput(t)
	good := `{"approved":true,"reason":"ok"}`

	// 没有 assistant 消息。
	if _, err := output.Decode([]schema.Message{schema.UserMessage("q")}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	// 最后一条 assistant 带工具调用：不回退到更早的回答。
	messages := []schema.Message{
		assistantOf(good),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("c1", "search", `{"q":"x"}`)),
	}
	if _, err := output.Decode(messages); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	// 只有 reasoning 没有文本。
	reasoningOnly := schema.NewMessage(schema.RoleAssistant, schema.NewReasoningBlock("think", ""))
	if _, err := output.Decode([]schema.Message{reasoningOnly}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	// reasoning + text 正常解码：reasoning 不阻塞。
	mixed := schema.NewMessage(schema.RoleAssistant,
		schema.NewReasoningBlock("think", ""), schema.NewTextBlock(good))
	if value, err := output.Decode([]schema.Message{mixed}); err != nil || !value.Approved {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
	// 拒绝回答。
	refusal := assistantOf("")
	refusal.Extra = map[string]any{"llm.refusal": "cannot comply"}
	if _, err := output.Decode([]schema.Message{refusal}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
	// 截断与内容过滤。
	for _, reason := range []string{"length", "content_filter"} {
		truncated := assistantOf(`{"approved":true,"reason":"ok"}`)
		truncated.ResponseMeta = &schema.ResponseMeta{FinishReason: reason}
		if _, err := output.Decode([]schema.Message{truncated}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
			t.Fatalf("finish_reason=%s err = %v", reason, err)
		}
	}
	// 无 ResponseMeta 的消息仍按文本校验。
	if value, err := output.Decode([]schema.Message{assistantOf(good)}); err != nil || !value.Approved {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
}

func TestStructuredOutputFormat(t *testing.T) {
	output, err := llm.NewStructuredOutput[decision](
		llm.WithOutputName("decision"), llm.WithOutputDescription("审批结论"))
	if err != nil {
		t.Fatal(err)
	}
	format := output.Format()
	if format.Name != "decision" || format.Description != "审批结论" || !format.Strict {
		t.Fatalf("format = %#v", format)
	}
	// 返回值是独立副本。
	format.Name = "mutated"
	format.Schema[0] = ' '
	again := output.Format()
	if again.Name != "decision" || string(again.Schema) == string(format.Schema) {
		t.Fatalf("format not isolated: %#v", again)
	}
	// schema 包含严格约束。
	var doc map[string]any
	if err := json.Unmarshal(again.Schema, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["additionalProperties"] != false {
		t.Fatalf("schema = %v", doc)
	}
	required, _ := doc["required"].([]any)
	if len(required) != 2 {
		t.Fatalf("required = %v", required)
	}
}

func TestStructuredOutputNameRules(t *testing.T) {
	// 默认名称由类型名推导。
	output := newDecisionOutput(t)
	if output.Format().Name != "decision" {
		t.Fatalf("name = %q", output.Format().Name)
	}
	// 非法显式名称构造期报错。
	for _, bad := range []string{"has space", "中文", "", strings.Repeat("a", 65)} {
		if _, err := llm.NewStructuredOutput[decision](llm.WithOutputName(bad)); err == nil {
			t.Fatalf("name %q accepted", bad)
		}
	}
	if _, err := llm.NewStructuredOutput[decision](llm.WithOutputName("ok-name_1")); err != nil {
		t.Fatal(err)
	}
}

func TestStructuredOutputConcurrentReuse(t *testing.T) {
	output := newDecisionOutput(t)
	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := output.Decode([]schema.Message{assistantOf(`{"approved":true,"reason":"ok"}`)}); err != nil {
				t.Error(err)
			}
			if _, err := output.Decode([]schema.Message{assistantOf(`{"broken":true}`)}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
				t.Errorf("err = %v", err)
			}
			_ = output.Format()
		}()
	}
	group.Wait()
}

func TestStructuredOutputInvokeRejectsOptions(t *testing.T) {
	output := newDecisionOutput(t)
	if _, err := output.Invoke(context.Background(),
		[]schema.Message{assistantOf(`{"approved":true,"reason":"ok"}`)},
		llm.WithDeltaStream(true)); err == nil {
		t.Fatal("options accepted")
	}
	if len(output.OptionTypes()) != 0 {
		t.Fatal("OptionTypes should be empty")
	}
}

func TestStructuredOutputStreamDeliversOnce(t *testing.T) {
	output := newDecisionOutput(t)
	reader, err := output.Stream(context.Background(), []schema.Message{assistantOf(`{"approved":true,"reason":"ok"}`)})
	if err != nil {
		t.Fatal(err)
	}
	value, err := reader.Recv()
	if err != nil || !value.Approved {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
	if _, err := reader.Recv(); err != io.EOF {
		t.Fatalf("second recv err = %v", err)
	}
	// 失败路径交付错误。
	bad, err := output.Stream(context.Background(), []schema.Message{assistantOf(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Recv(); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("err = %v", err)
	}
}

func TestStructuredOutputCollectWaitsForStreamEnd(t *testing.T) {
	output := newDecisionOutput(t)
	reader, writer := runnable.Pipe[[]schema.Message](context.Background())
	go func() {
		_ = writer.Send([]schema.Message{assistantOf(`{"approved":true,"reason":"ok"}`)})
		_ = writer.Close()
	}()
	value, err := output.Collect(context.Background(), reader)
	if err != nil || !value.Approved {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
}

func TestStructuredOutputCollectPropagatesTailError(t *testing.T) {
	output := newDecisionOutput(t)
	tail := errors.New("upstream tail error")
	reader, writer := runnable.Pipe[[]schema.Message](context.Background())
	go func() {
		_ = writer.Send([]schema.Message{assistantOf(`{"approved":true,"reason":"ok"}`)})
		_ = writer.CloseWithError(tail)
	}()
	if _, err := output.Collect(context.Background(), reader); !errors.Is(err, tail) {
		t.Fatalf("err = %v", err)
	}
}

func TestStructuredOutputTransformSemantics(t *testing.T) {
	output := newDecisionOutput(t)
	// 正常路径：输入结束后交付一次 T。
	input, writer := runnable.Pipe[[]schema.Message](context.Background())
	reader, err := output.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = writer.Send([]schema.Message{assistantOf(`{"approved":true,"reason":"ok"}`)})
		_ = writer.Close()
	}()
	value, err := reader.Recv()
	if err != nil || !value.Approved {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
	if _, err := reader.Recv(); err != io.EOF {
		t.Fatalf("err = %v", err)
	}

	// 输入尾部错误不被吞掉。
	input2, writer2 := runnable.Pipe[[]schema.Message](context.Background())
	reader2, err := output.Transform(context.Background(), input2)
	if err != nil {
		t.Fatal(err)
	}
	tail := errors.New("tail")
	go func() {
		_ = writer2.Send([]schema.Message{assistantOf(`{"approved":true,"reason":"ok"}`)})
		_ = writer2.CloseWithError(tail)
	}()
	if _, err := reader2.Recv(); !errors.Is(err, tail) {
		t.Fatalf("err = %v", err)
	}
}

func TestStructuredOutputTransformCloseReleasesUpstream(t *testing.T) {
	output := newDecisionOutput(t)
	input, writer := runnable.Pipe[[]schema.Message](context.Background())
	reader, err := output.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	// 上游还未产出即关闭输出读者：上游写入应被取消而不是永久阻塞。
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- writer.Send([]schema.Message{assistantOf(`{}`)}) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream not released after reader close")
	}
}

func TestStructuredOutputCollectHonorsCancellation(t *testing.T) {
	output := newDecisionOutput(t)
	// 上游 Reader 用自己的 Context：本次调用取消仍应让等待退出并释放上游。
	input, _ := runnable.Pipe[[]schema.Message](context.Background())
	defer input.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := output.Collect(ctx, input); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Collect did not respond to caller cancellation")
	}
}

func TestStructuredOutputTransformHonorsCancellation(t *testing.T) {
	output := newDecisionOutput(t)
	input, _ := runnable.Pipe[[]schema.Message](context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := output.Transform(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := reader.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestStructuredOutputRejectsUnsupportedFieldTypes(t *testing.T) {
	// 动态 map 无法展开成 strict 要求的封闭属性集。
	type withMap struct {
		Labels map[string]string `json:"labels"`
	}
	if _, err := llm.NewStructuredOutput[withMap](); err == nil {
		t.Fatal("dynamic map field accepted")
	}
	// ,string 标签让线路用字符串包裹数字，与推导出的原始类型不一致。
	type stringTag struct {
		ID int64 `json:"id,string"`
	}
	if _, err := llm.NewStructuredOutput[stringTag](); err == nil {
		t.Fatal("json ,string tag accepted")
	}
	// json.Number 按原始数字收发而推导按 string 种类处理。
	type numberField struct {
		N json.Number `json:"n"`
	}
	if _, err := llm.NewStructuredOutput[numberField](); err == nil {
		t.Fatal("json.Number field accepted")
	}
}

func TestStructuredOutputRejectsCodecBeyondFields(t *testing.T) {
	// 顶层自定义编解码。
	if _, err := llm.NewStructuredOutput[customCodec](); err == nil {
		t.Fatal("root custom codec accepted")
	}
	// 容器元素内的自定义编解码。
	type items struct {
		Items []customCodec `json:"items"`
	}
	if _, err := llm.NewStructuredOutput[items](); err == nil {
		t.Fatal("slice element custom codec accepted")
	}
	// 指针字段内的自定义编解码。
	type pointerItem struct {
		Item *customCodec `json:"item"`
	}
	if _, err := llm.NewStructuredOutput[pointerItem](); err == nil {
		t.Fatal("pointer field custom codec accepted")
	}
}

func TestStructuredOutputFromModel(t *testing.T) {
	errVerdict := errors.New("conflicting verdict")
	model, err := datamodel.New[decision](
		datamodel.WithSchema[decision](func(s *jsonschema.Schema) error {
			// 给 reason 配一个长度上限，验证约束进入严格副本的 Format。
			max := 10
			s.Properties["reason"].MaxLength = &max
			return nil
		}),
		datamodel.WithValidator(func(v decision) error {
			if v.Approved && strings.Contains(v.Reason, "deny") {
				return errVerdict
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	output, err := llm.NewStructuredOutputFromModel(model)
	if err != nil {
		t.Fatal(err)
	}
	// Format 使用严格副本：reason 为 required 且禁止额外字段。
	format := output.Format()
	var strictSchema map[string]any
	if err := json.Unmarshal(format.Schema, &strictSchema); err != nil {
		t.Fatal(err)
	}
	if !format.Strict {
		t.Fatal("format must be strict")
	}
	required := map[string]bool{}
	for _, item := range strictSchema["required"].([]any) {
		required[item.(string)] = true
	}
	if !required["approved"] || !required["reason"] || strictSchema["additionalProperties"] != false {
		t.Fatalf("strict schema = %s", format.Schema)
	}
	reasonProps, _ := strictSchema["properties"].(map[string]any)["reason"].(map[string]any)
	if reasonProps["maxLength"] != float64(10) {
		t.Fatalf("model constraint must reach strict format: %s", format.Schema)
	}
	// Decode 走严格副本并保留业务检查。
	value, err := output.Decode([]schema.Message{assistantOf(`{"approved":false,"reason":"ok"}`)})
	if err != nil || value.Approved || value.Reason != "ok" {
		t.Fatalf("value=%+v err=%v", value, err)
	}
	// 超过模型配置的上限，严格副本拒绝；原模型按同样规则也拒绝，
	// 说明约束来自同一来源。
	if _, err := output.Decode([]schema.Message{assistantOf(`{"approved":false,"reason":"far too long"}`)}); !errors.Is(err, llm.ErrInvalidStructuredOutput) {
		t.Fatalf("over-limit output must be rejected: %v", err)
	}
	// 业务检查错误穿过输出组件：errors.Is 同时命中三层。
	_, err = output.Decode([]schema.Message{assistantOf(`{"approved":true,"reason":"deny"}`)})
	if !errors.Is(err, llm.ErrInvalidStructuredOutput) ||
		!errors.Is(err, datamodel.ErrInvalidData) ||
		!errors.Is(err, errVerdict) {
		t.Fatalf("error chain must expose all three classes: %v", err)
	}
	// 原模型不受影响：业务校验仍作用于它自身。
	if _, err := model.DecodeJSON([]byte(`{"approved":true,"reason":"deny"}`)); err == nil {
		t.Fatal("source model validator must still apply to itself")
	}
	if _, err := llm.NewStructuredOutputFromModel[decision](nil); err == nil {
		t.Fatal("nil model must be rejected")
	}
}

func TestStructuredOutputFromModelRejectsUnstrictableSource(t *testing.T) {
	type withAny struct {
		Name string `json:"name"`
		Data any    `json:"data"`
	}
	// 普通模型允许 any 字段；派生严格副本时按 strict 政策拒绝。
	model, err := datamodel.New[withAny]()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := llm.NewStructuredOutputFromModel(model); err == nil || !strings.Contains(err.Error(), "data") {
		t.Fatalf("expected strict rejection, got %v", err)
	}
}
