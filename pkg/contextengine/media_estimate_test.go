package contextengine_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/schema"
)

// mediaImage 构造内嵌 Data 的媒体块（URI 与 Data 互斥；Data 须为合法 JSON，
// base64 载荷以 JSON 字符串形式携带）。
func mediaImage(t *testing.T, raw []byte) schema.ContentBlock {
	t.Helper()
	payload, err := json.Marshal(base64.StdEncoding.EncodeToString(raw))
	if err != nil {
		t.Fatal(err)
	}
	block := schema.NewImageBlock("", "image/png")
	block.Image.Data = payload
	return block
}

// 大体积内嵌 Data 不能按文本计入 token——旧实现把 base64 交给 TokenCounter，
// 一张图片会被算成数十万 token 并错误裁剪掉所有历史。
func TestMediaDataNotCountedAsText(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     2048,
		ReservedOutputTokens: 10,
		Counter:              runeCounter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 约 1 MB 的 base64 载荷：旧实现下会被计为 ~1M token。
	message := schema.NewMessage(schema.RoleUser, schema.NewTextBlock("hi"), mediaImage(t, make([]byte, 1<<20)))
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{message}})
	if err != nil {
		t.Fatal(err)
	}
	// 4 + role(4) + text(2) + 图片固定估算(1105)。
	if result.Stats.InputTokens != 1115 {
		t.Fatalf("input tokens=%d", result.Stats.InputTokens)
	}

	// Data 完全不参与估算：有/无 Data 的计数必须一致。
	empty := schema.NewMessage(schema.RoleUser, schema.NewTextBlock("hi"), schema.NewImageBlock("img", "image/png"))
	same, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{empty}})
	if err != nil {
		t.Fatal(err)
	}
	if same.Stats.InputTokens != result.Stats.InputTokens {
		t.Fatalf("data affects estimate: with=%d without=%d", result.Stats.InputTokens, same.Stats.InputTokens)
	}
}

// 自定义估算器注入后生效，URI/MIME 也不再走文本计数。
func TestMediaEstimatorOverridesDefaults(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     100,
		ReservedOutputTokens: 10,
		Counter:              runeCounter{},
		MediaEstimator:       contextengine.ApproxMediaEstimator{Image: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	message := schema.NewMessage(schema.RoleUser, schema.NewImageBlock("file:///a-very-long-image-uri.png", "image/png"))
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{message}})
	if err != nil {
		t.Fatal(err)
	}
	// 4 + role(4) + image(7)；URI 与 MIME 不计入。
	if result.Stats.InputTokens != 15 {
		t.Fatalf("input tokens=%d", result.Stats.InputTokens)
	}
}

// 媒体估算参与预算：媒体块本身无法裁剪，预算不足时仍返回 BudgetError，
// 但所需 token 反映的是估算值而非 Data 字节数。
func TestMediaEstimateParticipatesInBudget(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     50,
		ReservedOutputTokens: 5,
		Counter:              runeCounter{},
		MediaEstimator:       contextengine.ApproxMediaEstimator{Image: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	message := schema.NewMessage(schema.RoleUser, mediaImage(t, make([]byte, 1<<20)))
	_, err = assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{message}})
	budgetErr, ok := err.(*contextengine.BudgetError)
	if !ok {
		t.Fatalf("err=%v", err)
	}
	if budgetErr.Required != 108 { // 4 + role(4) + image(100)
		t.Fatalf("required=%d（应等于估算值而非 Data 字节数）", budgetErr.Required)
	}
}

// 四类媒体的默认估算按类型区分。
func TestDefaultMediaEstimatePerType(t *testing.T) {
	estimator := contextengine.ApproxMediaEstimator{}
	cases := []struct {
		block schema.ContentBlock
		want  int
	}{
		{schema.NewImageBlock("u", "m"), 1105},
		{schema.NewFileBlock("u", "m"), 256},
		{schema.NewAudioBlock("u", "m"), 300},
		{schema.NewVideoBlock("u", "m"), 1105},
	}
	for _, tc := range cases {
		if got := estimator.EstimateMedia(tc.block); got != tc.want {
			t.Fatalf("type=%s got=%d want=%d", tc.block.Type, got, tc.want)
		}
	}
	// nil 媒体载荷与未知类型不 panic，返回 0。
	if estimator.EstimateMedia(schema.ContentBlock{Type: schema.ContentBlockTypeImage}) != 0 {
		t.Fatal("nil image payload should estimate 0")
	}
	if estimator.EstimateMedia(schema.NewTextBlock("x")) != 0 {
		t.Fatal("non-media block should estimate 0")
	}
}

// 历史中的媒体消息不因其 Data 体量被错误丢弃——裁剪只取决于估算值。
func TestMediaDoesNotEvictHistory(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens:     1200,
		ReservedOutputTokens: 10,
		Counter:              runeCounter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewTextBlock("remember me")),
		schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("ok")),
		schema.NewMessage(schema.RoleUser, mediaImage(t, make([]byte, 1<<20)), schema.NewTextBlock("describe")),
	}
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: input})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.DroppedTurns != 0 || len(result.Messages) != 3 {
		t.Fatalf("dropped=%d messages=%d", result.Stats.DroppedTurns, len(result.Messages))
	}
	// 媒体消息保留且估算值计入 OutputTokens 统计。
	if result.Stats.OutputTokens != result.Stats.InputTokens {
		t.Fatalf("output tokens=%d input=%d", result.Stats.OutputTokens, result.Stats.InputTokens)
	}
}
