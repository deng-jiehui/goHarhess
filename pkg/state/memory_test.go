package state

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/schema"
)

func TestMemoryStoreVersionsClonesAndRejectsStaleWrites(t *testing.T) {
	store := NewMemoryStore(MemoryStoreConfig{})
	message := schema.UserMessage("hello")
	message = schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock("call-1", "lookup", false, schema.NewTextBlock("value"), schema.NewOpaqueBlock("provider.citation", json.RawMessage(`{"page":3}`))))
	message.ResponseMeta = &schema.ResponseMeta{ID: "resp-1", Usage: &schema.TokenUsage{CacheReadTokens: 32}}
	message.Extra = map[string]any{"nested": map[string]any{"value": "message-original"}}
	created, err := store.Save(context.Background(), Checkpoint{
		ConversationID: "conversation-1",
		Messages:       []schema.Message{message},
		Values:         map[string]any{"nested": map[string]any{"value": "original"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Version != 1 || created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatalf("created checkpoint = %#v", created)
	}
	created.Messages[0].ContentBlocks[0] = schema.NewTextBlock("changed")
	created.Messages[0].Extra["nested"].(map[string]any)["value"] = "message-changed"
	created.Values["nested"].(map[string]any)["value"] = "changed"
	loaded, found, err := store.Load(context.Background(), "conversation-1")
	if err != nil || !found {
		t.Fatalf("Load found=%t err=%v", found, err)
	}
	results := loaded.Messages[0].FunctionToolResults()
	if len(results) != 1 || results[0].CallID != "call-1" || results[0].ContentBlocks[1].Opaque == nil || string(results[0].ContentBlocks[1].Opaque.Raw) != `{"page":3}` || loaded.Messages[0].ResponseMeta.Usage.CacheReadTokens != 32 || loaded.Messages[0].Extra["nested"].(map[string]any)["value"] != "message-original" || loaded.Values["nested"].(map[string]any)["value"] != "original" {
		t.Fatalf("stored checkpoint was mutated: %#v", loaded)
	}

	stale := loaded.Clone()
	updated := loaded.Clone()
	updated.Messages = append(updated.Messages, schema.AssistantMessage("answer"))
	updated, err = store.Save(context.Background(), updated)
	if err != nil || updated.Version != 2 {
		t.Fatalf("updated checkpoint = %#v, err=%v", updated, err)
	}
	_, err = store.Save(context.Background(), stale)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Save error = %v", err)
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Expected != 1 || conflict.Actual != 2 {
		t.Fatalf("conflict = %#v", conflict)
	}
}

func TestMemoryStoreExpiresCheckpoint(t *testing.T) {
	now := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore(MemoryStoreConfig{TTL: time.Minute})
	store.now = func() time.Time { return now }
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "expiring"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if _, found, err := store.Load(context.Background(), "expiring"); err != nil || found {
		t.Fatalf("expired Load found=%t err=%v", found, err)
	}
}

func TestCheckpointContextReturnsClonedValues(t *testing.T) {
	ctx := WithCheckpoint(context.Background(), Checkpoint{
		ConversationID: "context-conversation",
		Version:        3,
		Values:         map[string]any{"nested": map[string]any{"value": "original"}},
	})
	checkpoint, ok := FromContext(ctx)
	if !ok || checkpoint.Version != 3 {
		t.Fatalf("checkpoint = %#v, ok=%t", checkpoint, ok)
	}
	checkpoint.Values["nested"].(map[string]any)["value"] = "changed"
	if ValuesFromContext(ctx)["nested"].(map[string]any)["value"] != "original" {
		t.Fatal("checkpoint context values were mutated")
	}
}

// Save 返回值与存储数据隔离：调用方修改返回的消息不影响后续 Load。
func TestMemoryStoreSaveReturnIsolation(t *testing.T) {
	store := NewMemoryStore()
	msg := schema.UserMessage("persisted")
	msg.Extra = map[string]any{"k": "v"}
	saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "iso", Messages: []schema.Message{msg}})
	if err != nil {
		t.Fatal(err)
	}
	saved.Messages[0].Extra["k"] = "mutated"
	saved.Messages[0].ContentBlocks[0].Text.Text = "mutated"
	saved.Messages[0].Extra["new"] = true
	loaded, found, err := store.Load(context.Background(), "iso")
	if err != nil || !found {
		t.Fatalf("load failed: found=%v err=%v", found, err)
	}
	if loaded.Messages[0].JoinedText("") != "persisted" {
		t.Fatalf("stored message mutated: %q", loaded.Messages[0].JoinedText(""))
	}
	if loaded.Messages[0].Extra["k"] != "v" || len(loaded.Messages[0].Extra) != 1 {
		t.Fatalf("stored extra mutated: %+v", loaded.Messages[0].Extra)
	}
}

// ---- 2.1 持久化克隆回归：Clone 优先，JSON 保底 ----

// tagList 是未实现 Clone() 的含引用字段结构体：保存时应走 JSON 保底，
// 生成独立的通用类型副本。
type tagList struct {
	Tags []string `json:"tags"`
}

// cloneableMeta 实现 Clone() any：保存时保留具体类型并隔离内部切片。
type cloneableMeta struct {
	Tags []string
}

func (m *cloneableMeta) Clone() any {
	if m == nil {
		return (*cloneableMeta)(nil)
	}
	cloned := *m
	cloned.Tags = append([]string(nil), m.Tags...)
	return &cloned
}

// panickingMeta 的 Clone() 必定 panic：属于克隆实现失败的硬错误。
type panickingMeta struct{}

func (*panickingMeta) Clone() any { panic("boom") }

// wrongTypeMeta 的 Clone() 返回不符类型：属于克隆实现失败的硬错误。
type wrongTypeMeta struct{}

func (*wrongTypeMeta) Clone() any { return 42 }

// unmarshalableMeta 结构克隆不支持且 JSON 也无法表达：保存必须报错。
type unmarshalableMeta struct {
	F func()
}

func messageWithMeta(meta map[string]any) schema.Message {
	msg := schema.UserMessage("with meta")
	msg.Extra = meta
	return msg
}

// 未实现 Clone() 的自定义结构体经 JSON 保底隔离：调用方或返回值
// 的修改不得改写已存记录，且存储值转为通用 JSON 类型。
func TestMemoryStoreJSONFallbackIsolatesCustomStruct(t *testing.T) {
	store := NewMemoryStore()
	original := tagList{Tags: []string{"a", "b"}}
	msg := messageWithMeta(map[string]any{"meta": original, "count": 3})
	saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "json-fallback", Messages: []schema.Message{msg}})
	if err != nil {
		t.Fatal(err)
	}
	// 保存后修改调用方的切片与返回值的元数据，均不得影响已存记录。
	original.Tags[0] = "mutated"
	if meta, ok := saved.Messages[0].Extra["meta"].(map[string]any); ok {
		meta["tags"] = "mutated"
	}
	saved.Messages[0].Extra["count"] = 99
	loaded, found, err := store.Load(context.Background(), "json-fallback")
	if err != nil || !found {
		t.Fatalf("load failed: found=%v err=%v", found, err)
	}
	meta, ok := loaded.Messages[0].Extra["meta"].(map[string]any)
	if !ok {
		t.Fatalf("stored meta type %T, want map[string]any (JSON generic)", loaded.Messages[0].Extra["meta"])
	}
	tags, _ := meta["tags"].([]any)
	if len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Fatalf("stored tags=%v, want [a b]", tags)
	}
	if loaded.Messages[0].Extra["count"] != 3 {
		t.Fatalf("stored count=%v, want 3 (int preserved)", loaded.Messages[0].Extra["count"])
	}
}

// 实现 Clone() 的自定义类型保留具体类型并隔离内部可变数据。
func TestMemoryStoreClonePathPreservesConcreteType(t *testing.T) {
	store := NewMemoryStore()
	original := &cloneableMeta{Tags: []string{"x"}}
	msg := messageWithMeta(map[string]any{"meta": original})
	saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "clone-path", Messages: []schema.Message{msg}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := saved.Messages[0].Extra["meta"].(*cloneableMeta); !ok {
		t.Fatalf("saved meta type %T, want *cloneableMeta", saved.Messages[0].Extra["meta"])
	}
	original.Tags[0] = "mutated"
	loaded, found, err := store.Load(context.Background(), "clone-path")
	if err != nil || !found {
		t.Fatalf("load failed: found=%v err=%v", found, err)
	}
	meta, ok := loaded.Messages[0].Extra["meta"].(*cloneableMeta)
	if !ok {
		t.Fatalf("loaded meta type %T, want *cloneableMeta", loaded.Messages[0].Extra["meta"])
	}
	if len(meta.Tags) != 1 || meta.Tags[0] != "x" {
		t.Fatalf("stored tags=%v, want [x]", meta.Tags)
	}
	// 修改读取结果不得影响存储数据。
	meta.Tags[0] = "mutated"
	reloaded, _, _ := store.Load(context.Background(), "clone-path")
	if reloaded.Messages[0].Extra["meta"].(*cloneableMeta).Tags[0] != "x" {
		t.Fatal("load-returned mutation leaked into store")
	}
}

// 嵌套容器内的自定义值随所属 Extra 项 JSON 保底，保持隔离。
func TestMemoryStoreJSONFallbackNestedContainer(t *testing.T) {
	store := NewMemoryStore()
	inner := tagList{Tags: []string{"deep"}}
	msg := messageWithMeta(map[string]any{"nested": map[string]any{"inner": inner}})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "nested", Messages: []schema.Message{msg}}); err != nil {
		t.Fatal(err)
	}
	inner.Tags[0] = "mutated"
	loaded, _, err := store.Load(context.Background(), "nested")
	if err != nil {
		t.Fatal(err)
	}
	nested, ok := loaded.Messages[0].Extra["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested type %T", loaded.Messages[0].Extra["nested"])
	}
	innerMeta, ok := nested["inner"].(map[string]any)
	if !ok {
		t.Fatalf("inner type %T, want generic map", nested["inner"])
	}
	if tags, _ := innerMeta["tags"].([]any); len(tags) != 1 || tags[0] != "deep" {
		t.Fatalf("inner tags=%v, want [deep]", innerMeta["tags"])
	}
}

// Clone() panic 是硬错误：保存失败、不产生新版本、不被 JSON 保底掩盖。
func TestMemoryStoreRejectsPanickingClone(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{"bad": &panickingMeta{}})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "panic", Messages: []schema.Message{msg}}); err == nil {
		t.Fatal("save with panicking Clone succeeded")
	} else if !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("save error=%v, want wrapped ErrInvalidCheckpoint", err)
	}
	if _, found, _ := store.Load(context.Background(), "panic"); found {
		t.Fatal("failed save produced a stored record")
	}
	// 后续正常保存不受影响。
	ok := messageWithMeta(map[string]any{"k": "v"})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "panic", Messages: []schema.Message{ok}}); err != nil {
		t.Fatalf("save after rejected attempt failed: %v", err)
	}
}

// Clone() 返回不符类型同样属于实现失败，保存必须报错。
func TestMemoryStoreRejectsWrongTypeClone(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{"bad": &wrongTypeMeta{}})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "wrong-type", Messages: []schema.Message{msg}}); err == nil {
		t.Fatal("save with wrong-type Clone succeeded")
	}
}

// JSON 也无法表达的值直接报错，不保留共享引用。
func TestMemoryStoreRejectsUnmarshalableMeta(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{"bad": unmarshalableMeta{F: func() {}}})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "unmarshalable", Messages: []schema.Message{msg}}); err == nil {
		t.Fatal("save with unmarshalable meta succeeded")
	}
	if _, found, _ := store.Load(context.Background(), "unmarshalable"); found {
		t.Fatal("failed save produced a stored record")
	}
}

// 同一条消息同时含有可 JSON 保底的值和失败的 Clone() 时，仍必须报告错误。
func TestMemoryStoreMixedMetaReportsCloneError(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{
		"good":  &cloneableMeta{Tags: []string{"ok"}},
		"unsup": tagList{Tags: []string{"json"}},
		"bad":   &panickingMeta{},
	})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "mixed", Messages: []schema.Message{msg}}); err == nil {
		t.Fatal("save with mixed meta containing panicking Clone succeeded")
	}
	if _, found, _ := store.Load(context.Background(), "mixed"); found {
		t.Fatal("failed save produced a stored record")
	}
}

// ---- 第二轮复核：后续复制、保底粒度、nil 指针、嵌套类型校验 ----

// flakyMeta 的 Clone() 在第 failAt 次调用时 panic：用于向第二次保存
// 复制或读取复制注入克隆失败。
type flakyMeta struct {
	calls  *int
	failAt int
	Note   string `json:"note"`
}

func (f *flakyMeta) Clone() any {
	*f.calls++
	if *f.calls >= f.failAt {
		panic("injected clone failure")
	}
	return &flakyMeta{calls: f.calls, failAt: f.failAt, Note: f.Note}
}

// outerMeta 未实现 Clone() 且含指针字段：只能走 JSON 保底。
// 用于验证保底前会先校验内部嵌套的 Clone() 实现。
type outerMeta struct {
	Inner *wrongTypeMeta `json:"inner"`
	Name  string         `json:"name"`
}

// 第二次保存复制注入克隆失败：保存必须报错，不产生版本与记录。
func TestMemoryStoreSaveSecondCopyFailureAborts(t *testing.T) {
	store := NewMemoryStore()
	calls := 0
	msg := messageWithMeta(map[string]any{"meta": &flakyMeta{calls: &calls, failAt: 2, Note: "x"}})
	// PersistClone 调用方副本消耗第 1 次 Clone，存储副本的第 2 次 panic。
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "flaky-save", Messages: []schema.Message{msg}}); err == nil {
		t.Fatal("save with failing second copy succeeded")
	}
	if _, found, _ := store.Load(context.Background(), "flaky-save"); found {
		t.Fatal("failed save produced a stored record")
	}
	// 版本未被污染：干净的检查点可从 0 重新保存。
	msg.Extra = map[string]any{"ok": "v"}
	if saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "flaky-save", Messages: []schema.Message{msg}}); err != nil || saved.Version != 1 {
		t.Fatalf("save after rejected attempt: version=%d err=%v", saved.Version, err)
	}
}

// 读取复制注入克隆失败：Load/Get/List 必须返回错误而不是共享引用。
func TestMemoryStoreReadCloneFailureReturnsError(t *testing.T) {
	store := NewMemoryStore()
	calls := 0
	msg := messageWithMeta(map[string]any{"meta": &flakyMeta{calls: &calls, failAt: 3, Note: "x"}})
	saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "flaky-read", Messages: []schema.Message{msg}})
	if err != nil {
		t.Fatal(err)
	}
	// Save 已消耗 2 次 Clone；第 3 次发生在读取复制时 panic。
	if _, _, err := store.Load(context.Background(), "flaky-read"); err == nil {
		t.Fatal("Load with failing clone succeeded")
	}
	if _, _, err := store.Get(context.Background(), "flaky-read", saved.ID); err == nil {
		t.Fatal("Get with failing clone succeeded")
	}
	if _, err := store.List(context.Background(), "flaky-read", ListOptions{}); err == nil {
		t.Fatal("List with failing clone succeeded")
	}
}

// 通用 map 中只需保底的值被单独转换：旁边的 int 保持 int、Clone 对象
// 保持具体类型，不再被连带转成通用 JSON 类型。
func TestMemoryStoreFallbackPreservesSiblingTypes(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{
		"mixed": map[string]any{
			"keep":   7,
			"cloner": &cloneableMeta{Tags: []string{"c"}},
			"fb":     tagList{Tags: []string{"t"}},
			"list":   []any{tagList{Tags: []string{"l"}}, 5},
		},
	})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "siblings", Messages: []schema.Message{msg}}); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(context.Background(), "siblings")
	if err != nil || !found {
		t.Fatalf("load failed: found=%v err=%v", found, err)
	}
	mixed, ok := loaded.Messages[0].Extra["mixed"].(map[string]any)
	if !ok {
		t.Fatalf("mixed type %T, want map[string]any", loaded.Messages[0].Extra["mixed"])
	}
	if keep, ok := mixed["keep"].(int); !ok || keep != 7 {
		t.Fatalf("keep=%v(%T), want int 7", mixed["keep"], mixed["keep"])
	}
	cloner, ok := mixed["cloner"].(*cloneableMeta)
	if !ok || len(cloner.Tags) != 1 || cloner.Tags[0] != "c" {
		t.Fatalf("cloner=%#v, want *cloneableMeta{[c]}", mixed["cloner"])
	}
	fb, ok := mixed["fb"].(map[string]any)
	if !ok {
		t.Fatalf("fb type %T, want generic map", mixed["fb"])
	}
	if tags, _ := fb["tags"].([]any); len(tags) != 1 || tags[0] != "t" {
		t.Fatalf("fb.tags=%v, want [t]", fb["tags"])
	}
	list, ok := mixed["list"].([]any)
	if !ok || len(list) != 2 {
		t.Fatalf("list=%#v, want []any len 2", mixed["list"])
	}
	if _, ok := list[0].(map[string]any); !ok {
		t.Fatalf("list[0] type %T, want generic map", list[0])
	}
	if n, ok := list[1].(int); !ok || n != 5 {
		t.Fatalf("list[1]=%v(%T), want int 5", list[1], list[1])
	}
}

// 强类型容器装不下保底结果时，扩大到该容器所属元数据项整体转换。
func TestMemoryStoreTypedContainerEscalatesFallback(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{
		"typed": map[string]tagList{"a": {Tags: []string{"x"}}},
		"keep":  3,
	})
	saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "typed-container", Messages: []schema.Message{msg}})
	if err != nil {
		t.Fatal(err)
	}
	typed, ok := saved.Messages[0].Extra["typed"].(map[string]any)
	if !ok {
		t.Fatalf("typed type %T, want generic map", saved.Messages[0].Extra["typed"])
	}
	entry, ok := typed["a"].(map[string]any)
	if !ok {
		t.Fatalf("typed[a] type %T, want generic map", typed["a"])
	}
	if tags, _ := entry["tags"].([]any); len(tags) != 1 || tags[0] != "x" {
		t.Fatalf("typed[a].tags=%v, want [x]", entry["tags"])
	}
	if keep, ok := saved.Messages[0].Extra["keep"].(int); !ok || keep != 3 {
		t.Fatalf("keep=%v(%T), want int 3", saved.Messages[0].Extra["keep"], saved.Messages[0].Extra["keep"])
	}
}

// 正文块元数据保底只转换需要保底的值：块正文、块内普通值与消息级
// 元数据都保持原类型，不再回退整条消息。
func TestMemoryStoreBlockExtraFallbackKeepsMessageFields(t *testing.T) {
	store := NewMemoryStore()
	msg := schema.UserMessage("body")
	msg.ContentBlocks[0].Text.Extra = map[string]any{"fb": tagList{Tags: []string{"b"}}, "keep": 4}
	msg.Extra = map[string]any{"top": 9, "meta": &cloneableMeta{Tags: []string{"m"}}}
	saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "block-extra", Messages: []schema.Message{msg}})
	if err != nil {
		t.Fatal(err)
	}
	block := saved.Messages[0].ContentBlocks[0]
	if block.Text == nil || block.Text.Text != "body" {
		t.Fatalf("text block lost: %#v", block)
	}
	if _, ok := block.Text.Extra["fb"].(map[string]any); !ok {
		t.Fatalf("block fb type %T, want generic map", block.Text.Extra["fb"])
	}
	if keep, ok := block.Text.Extra["keep"].(int); !ok || keep != 4 {
		t.Fatalf("block keep=%v(%T), want int 4", block.Text.Extra["keep"], block.Text.Extra["keep"])
	}
	if top, ok := saved.Messages[0].Extra["top"].(int); !ok || top != 9 {
		t.Fatalf("top=%v(%T), want int 9", saved.Messages[0].Extra["top"], saved.Messages[0].Extra["top"])
	}
	if _, ok := saved.Messages[0].Extra["meta"].(*cloneableMeta); !ok {
		t.Fatalf("meta type %T, want *cloneableMeta", saved.Messages[0].Extra["meta"])
	}
}

// 类型化 nil 指针实现 Clone()：与需要保底的字段同处一条消息时
// 不触发 nil 接收者调用，保存正常且类型保持。
func TestMemoryStoreTypedNilCloneableWithFallbackSibling(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{
		"opt": (*cloneableMeta)(nil),
		"fb":  tagList{Tags: []string{"n"}},
	})
	saved, err := store.Save(context.Background(), Checkpoint{ConversationID: "typed-nil", Messages: []schema.Message{msg}})
	if err != nil {
		t.Fatalf("save with typed-nil cloneable failed: %v", err)
	}
	opt, ok := saved.Messages[0].Extra["opt"].(*cloneableMeta)
	if !ok || opt != nil {
		t.Fatalf("opt=%#v, want typed nil *cloneableMeta", saved.Messages[0].Extra["opt"])
	}
	if _, ok := saved.Messages[0].Extra["fb"].(map[string]any); !ok {
		t.Fatalf("fb type %T, want generic map", saved.Messages[0].Extra["fb"])
	}
}

// 外层结构体需 JSON 保底时，内部 Clone() 返回错误类型仍必须报错，
// 不能被保底掩盖。
func TestMemoryStoreRejectsNestedWrongTypeCloneInsideFallback(t *testing.T) {
	store := NewMemoryStore()
	msg := messageWithMeta(map[string]any{"meta": outerMeta{Inner: &wrongTypeMeta{}, Name: "x"}})
	if _, err := store.Save(context.Background(), Checkpoint{ConversationID: "nested-wrong", Messages: []schema.Message{msg}}); err == nil {
		t.Fatal("save with nested wrong-type Clone inside fallback struct succeeded")
	}
	if _, found, _ := store.Load(context.Background(), "nested-wrong"); found {
		t.Fatal("failed save produced a stored record")
	}
}

// 消息日志去重：同一会话连续保存只追加增量，记录体不重复保存前缀。
func TestMemoryStoreMessageLogDeduplicates(t *testing.T) {
	store := NewMemoryStore()
	base := []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")}
	v1, err := store.Save(context.Background(), Checkpoint{ConversationID: "dedup", Messages: base})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := store.Save(context.Background(), Checkpoint{
		ConversationID: "dedup", Version: v1.Version, ID: v1.ID,
		Messages: append(append([]schema.Message{}, base...), schema.UserMessage("q2")),
	})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	entry := store.entries["dedup"]
	stored := entry.checkpoint
	logLen := len(entry.messages)
	store.mu.RUnlock()
	if stored.MessagesFrom != 2 || len(stored.Messages) != 1 {
		t.Fatalf("stored delta from=%d len=%d, want 2/1", stored.MessagesFrom, len(stored.Messages))
	}
	if logLen != 3 {
		t.Fatalf("message log len=%d, want 3", logLen)
	}
	// 历史条目同样只存增量，读取时物化完整会话。
	store.mu.RLock()
	old := entry.history[0]
	store.mu.RUnlock()
	if old.MessagesFrom != 0 || len(old.Messages) != 2 {
		t.Fatalf("v1 stored from=%d len=%d, want 0/2", old.MessagesFrom, len(old.Messages))
	}
	got, found, err := store.Get(context.Background(), "dedup", v2.ID)
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if len(got.Messages) != 3 || got.MessagesFrom != 0 {
		t.Fatalf("materialized %d messages from=%d, want 3/0", len(got.Messages), got.MessagesFrom)
	}
	prev, found, err := store.Get(context.Background(), "dedup", v1.ID)
	if err != nil || !found || len(prev.Messages) != 2 {
		t.Fatalf("Get v1 found=%v err=%v len=%d", found, err, len(prev.Messages))
	}
}

// 消息日志分叉：保存的消息序列与已存日志前缀不一致时切新代次，
// 新旧记录各自物化出正确会话。
func TestMemoryStoreMessageLogFork(t *testing.T) {
	store := NewMemoryStore()
	v1, err := store.Save(context.Background(), Checkpoint{
		ConversationID: "fork",
		Messages:       []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 改写历史后沿同一版本链保存：前缀在第 2 条分叉。
	diverged := []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("edited"), schema.UserMessage("q2")}
	v2, err := store.Save(context.Background(), Checkpoint{
		ConversationID: "fork", Version: v1.Version, ID: v1.ID, Messages: diverged,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	entry := store.entries["fork"]
	epoch := entry.epoch
	archived := len(entry.archives)
	store.mu.RUnlock()
	if epoch != 2 || archived != 1 {
		t.Fatalf("epoch=%d archives=%d, want 2/1", epoch, archived)
	}
	got, found, _ := store.Get(context.Background(), "fork", v2.ID)
	if !found || len(got.Messages) != 3 || got.Messages[1].JoinedText("") != "edited" {
		t.Fatalf("v2 materialized %#v", got.Messages)
	}
	prev, found, _ := store.Get(context.Background(), "fork", v1.ID)
	if !found || len(prev.Messages) != 2 || prev.Messages[1].JoinedText("") != "a1" {
		t.Fatalf("v1 materialized %#v", prev.Messages)
	}
	// 新代次上继续追加不触发分叉。
	v3, err := store.Save(context.Background(), Checkpoint{
		ConversationID: "fork", Version: v2.Version, ID: v2.ID,
		Messages: append(append([]schema.Message{}, diverged...), schema.AssistantMessage("a2")),
	})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	entry = store.entries["fork"]
	epoch = entry.epoch
	store.mu.RUnlock()
	if epoch != 2 {
		t.Fatalf("epoch=%d after append, want 2", epoch)
	}
	latest, _, _ := store.Load(context.Background(), "fork")
	if len(latest.Messages) != 4 || latest.ID != v3.ID {
		t.Fatalf("latest %d messages", len(latest.Messages))
	}
}

// MaxHistory 保留策略：超出上限的历史记录按保存顺序淘汰，最新记录与
// 物化不受影响。
func TestMemoryStoreMaxHistory(t *testing.T) {
	store := NewMemoryStore(MemoryStoreConfig{MaxHistory: 2})
	messages := []schema.Message{schema.UserMessage("m0")}
	var last Checkpoint
	var ids []string
	for i := 1; i <= 4; i++ {
		messages = append(append([]schema.Message{}, messages...), schema.UserMessage("m"+string(rune('0'+i))))
		saved, err := store.Save(context.Background(), Checkpoint{
			ConversationID: "retain", Version: last.Version, ID: last.ID, Messages: messages,
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, saved.ID)
		last = saved
	}
	store.mu.RLock()
	entry := store.entries["retain"]
	kept := len(entry.history)
	store.mu.RUnlock()
	if kept != 2 {
		t.Fatalf("history kept=%d, want 2", kept)
	}
	if _, found, err := store.Get(context.Background(), "retain", ids[0]); err != nil || found {
		t.Fatalf("evicted record found=%v err=%v", found, err)
	}
	got, found, err := store.Get(context.Background(), "retain", ids[3])
	if err != nil || !found || len(got.Messages) != 5 {
		t.Fatalf("latest record found=%v messages=%d", found, len(got.Messages))
	}
	// 保留记录仍能沿父链物化完整会话。
	got, found, err = store.Get(context.Background(), "retain", ids[2])
	if err != nil || !found || len(got.Messages) != 4 {
		t.Fatalf("kept record found=%v messages=%d", found, len(got.Messages))
	}
	list, err := store.List(context.Background(), "retain", ListOptions{})
	if err != nil || len(list) != 2 {
		t.Fatalf("List len=%d err=%v, want 2", len(list), err)
	}
}

// MaxHistory 淘汰运行前记录后，放弃运行仍须回退到运行开始前的会话。
// 回退依据独立于可裁剪历史保留（P1：此前 MaxHistory=2 保存三轮后
// AbandonRun 找不到运行前记录而失败）。
func TestMemoryStoreAbandonAfterHistoryTrim(t *testing.T) {
	store := NewMemoryStore(MemoryStoreConfig{MaxHistory: 2})
	ctx := context.Background()
	prev, err := store.Save(ctx, Checkpoint{
		ConversationID: "abandon", RunID: "run-a", Status: StatusCompleted,
		Messages: []schema.Message{schema.UserMessage("old")},
		Values:   map[string]any{"k": "v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	last := prev
	for i := 0; i < 3; i++ {
		last, err = store.Save(ctx, Checkpoint{
			ConversationID: "abandon", RunID: "run-b", Status: StatusInterrupted,
			Version: last.Version, ID: last.ID,
			Messages: append(append([]schema.Message{}, prev.Messages...), schema.UserMessage("new"+string(rune('0'+i)))),
			Values:   map[string]any{"k": "v2"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// 历史只剩最近两条 run-b 记录，运行前的 run-a 已被淘汰。
	store.mu.RLock()
	kept := len(store.entries["abandon"].history)
	store.mu.RUnlock()
	if kept != 2 {
		t.Fatalf("history kept=%d, want 2", kept)
	}
	abandoned, err := AbandonRun(ctx, store, "abandon")
	if err != nil {
		t.Fatalf("AbandonRun err=%v", err)
	}
	if abandoned.Status != StatusAbandoned || len(abandoned.Messages) != 1 ||
		abandoned.Messages[0].JoinedText("") != "old" || abandoned.Values["k"] != "v1" {
		t.Fatalf("abandoned=%#v", abandoned)
	}
}

// 增量记录必须持有独立尾部切片：full[shared:] 的视图会让整条底层数组
// （含前缀消息）随记录存活（P2：512KiB 载荷四十次保存后存活堆约 21MiB）。
func TestMemoryStoreDeltaDoesNotPinPrefix(t *testing.T) {
	store := NewMemoryStore()
	base := []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")}
	v1, err := store.Save(context.Background(), Checkpoint{ConversationID: "cap", Messages: base})
	if err != nil {
		t.Fatal(err)
	}
	full := append(append([]schema.Message{}, base...), schema.UserMessage("q2"))
	if _, err := store.Save(context.Background(), Checkpoint{
		ConversationID: "cap", Version: v1.Version, ID: v1.ID, Messages: full,
	}); err != nil {
		t.Fatal(err)
	}
	store.mu.RLock()
	entry := store.entries["cap"]
	stored := entry.checkpoint
	store.mu.RUnlock()
	if stored.MessagesFrom != 2 || len(stored.Messages) != 1 {
		t.Fatalf("stored from=%d len=%d, want 2/1", stored.MessagesFrom, len(stored.Messages))
	}
	if cap(stored.Messages) != len(stored.Messages) {
		t.Fatalf("delta cap=%d pins %d prefix entries", cap(stored.Messages), stored.MessagesFrom)
	}
}

// 淘汰历史记录后其引用的旧代次日志必须回收：MaxHistory=1 连续改写
// 不应堆积无引用的归档（P2：此前保留十九份孤儿日志）。
func TestMemoryStoreArchiveGC(t *testing.T) {
	store := NewMemoryStore(MemoryStoreConfig{MaxHistory: 1})
	var last Checkpoint
	for i := 0; i < 20; i++ {
		saved, err := store.Save(context.Background(), Checkpoint{
			ConversationID: "gc", Version: last.Version, ID: last.ID,
			RunID:    "run",
			Status:   StatusCompleted,
			Messages: []schema.Message{schema.UserMessage("v" + string(rune('a'+i)))},
		})
		if err != nil {
			t.Fatal(err)
		}
		last = saved
	}
	store.mu.RLock()
	entry := store.entries["gc"]
	archived := len(entry.archives)
	kept := len(entry.history)
	store.mu.RUnlock()
	if archived != 0 {
		t.Fatalf("archives=%d, want 0 (unreferenced epochs must be reclaimed)", archived)
	}
	if kept != 1 {
		t.Fatalf("history kept=%d, want 1", kept)
	}
}

// 并发下读取旧历史与新代次写入：Get 在锁外物化，不得访问被 Save
// 原地替换的归档 map（P1：archives 并发读写竞态）。
func TestMemoryStoreConcurrentForkAndRead(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	v1, err := store.Save(ctx, Checkpoint{
		ConversationID: "race", Messages: []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, _, err := store.Get(ctx, "race", v1.ID); err != nil {
					return
				}
			}
		}
	}()
	last := v1
	for i := 0; i < 50; i++ {
		msgs := []schema.Message{schema.UserMessage("q1"), schema.AssistantMessage("a" + string(rune('a'+i%26)) + string(rune('0'+i/26)))}
		last, err = store.Save(ctx, Checkpoint{
			ConversationID: "race", Version: last.Version, ID: last.ID, Messages: msgs,
		})
		if err != nil {
			close(stop)
			wg.Wait()
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}
