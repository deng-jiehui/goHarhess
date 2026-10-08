package tool_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// fakeCallRecorder 模拟会话登记器：数据落 store，批次序号按会话内顺序分配。
// replay 生成共享同一 store 的新登记器，模拟恢复后的新会话。
type fakeCallStore struct {
	mu       sync.Mutex
	stored   map[string][]byte
	recorded []string
}

type fakeCallRecorder struct {
	store   *fakeCallStore
	batches int
	mu      sync.Mutex
}

func newFakeCallRecorder() *fakeCallRecorder {
	return &fakeCallRecorder{store: &fakeCallStore{stored: map[string][]byte{}}}
}

func (f *fakeCallRecorder) replay() *fakeCallRecorder {
	return &fakeCallRecorder{store: f.store}
}

func (f *fakeCallRecorder) BeginBatch() control.CallBatch {
	f.mu.Lock()
	seq := f.batches
	f.batches++
	f.mu.Unlock()
	return fakeCallBatch{store: f.store, seq: seq}
}

func (f *fakeCallRecorder) keys() []string {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return append([]string(nil), f.store.recorded...)
}

type fakeCallBatch struct {
	store *fakeCallStore
	seq   int
}

func (b fakeCallBatch) Namespace() string { return "test-run/" + strconv.Itoa(b.seq) }

func (b fakeCallBatch) CallKey(base string) string {
	return strconv.Itoa(b.seq) + "/" + base
}

func (b fakeCallBatch) LookupPending(key string) ([]byte, bool) {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	raw, ok := b.store.stored[key]
	return append([]byte(nil), raw...), ok
}

func (b fakeCallBatch) RecordPending(key string, payload []byte) {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	b.store.stored[key] = append([]byte(nil), payload...)
	b.store.recorded = append(b.store.recorded, key)
}

// 同一批次重放命中登记结果；主动发起的新批次不复用，工具正常执行。
func TestPendingCallReplaySkipsExecution(t *testing.T) {
	var runs int
	var keys []string
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
			runs++
			keys = append(keys, input.IdempotencyKey)
			return tool.TextResult("echoed"), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := newFakeCallRecorder()
	ctx := control.WithCallRecorder(context.Background(), recorder)
	input := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewFunctionToolCallBlock("call-1", "echo", `{}`))}
	first, err := node.Invoke(ctx, input)
	if err != nil || runs != 1 {
		t.Fatalf("first invoke: runs=%d err=%v", runs, err)
	}
	if len(keys) != 1 || keys[0] != "test-run/0/0/call-1" {
		t.Fatalf("idempotency keys=%v", keys)
	}
	// 同一会话内主动再调一批：新批次不复用旧结果。
	if _, err := node.Invoke(ctx, input); err != nil {
		t.Fatal(err)
	}
	if runs != 2 {
		t.Fatalf("new batch must execute: runs=%d", runs)
	}
	// 恢复重放：新会话（批次重开）命中登记结果，不再执行。
	resumeCtx := control.WithCallRecorder(context.Background(), recorder.replay())
	second, err := node.Invoke(resumeCtx, input)
	if err != nil || runs != 2 {
		t.Fatalf("resume should replay: runs=%d err=%v", runs, err)
	}
	if toolResultText(first[0]) != "echoed" || toolResultText(second[0]) != "echoed" {
		t.Fatalf("first=%v second=%v", first, second)
	}
}

// 挂起型错误不是完成结果：不登记，恢复后调用重新执行。
func TestInterruptCallIsNotRecorded(t *testing.T) {
	var runs int
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(context.Context, tool.CallInput) (tool.Result, error) {
			runs++
			return tool.Result{}, control.RequestInterrupt("approve", nil)
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := newFakeCallRecorder()
	ctx := control.WithCallRecorder(context.Background(), recorder)
	input := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewFunctionToolCallBlock("call-1", "ask", `{}`))}
	// 挂起型错误作为节点错误上传，调度器据此挂起；不产出结果消息。
	out, err := node.Invoke(ctx, input)
	if err == nil || runs != 1 {
		t.Fatalf("runs=%d err=%v", runs, err)
	}
	if out != nil {
		t.Fatalf("out=%v", out)
	}
	if len(recorder.keys()) != 0 {
		t.Fatalf("interrupt result must not be recorded: %v", recorder.keys())
	}
}

// 无 FunctionID 的调用使用名称+参数摘要作基准键；同批重复基准键追加序号，
// 幂等键始终含运行级前缀。
func TestDedupeKeyFallbackAndOrdinal(t *testing.T) {
	var keys []string
	var mu sync.Mutex
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
			mu.Lock()
			keys = append(keys, input.IdempotencyKey)
			mu.Unlock()
			return tool.TextResult("ok"), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := control.WithCallRecorder(context.Background(), newFakeCallRecorder())
	input := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewFunctionToolCallBlock("dup", "echo", `{}`),
		schema.NewFunctionToolCallBlock("dup", "echo", `{}`),
		schema.NewFunctionToolCallBlock("", "echo", `{"x":1}`))}
	if _, err := node.Invoke(ctx, input); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("keys=%v", keys)
	}
	seen := map[string]bool{}
	for _, key := range keys {
		if key == "" || seen[key] {
			t.Fatalf("duplicate or empty key: %v", keys)
		}
		seen[key] = true
	}
}

// 错误结果（工具已运行并返回失败）也登记，保证恢复回放确定性。
func TestErrorResultIsRecorded(t *testing.T) {
	var runs int
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "pay"}, func(context.Context, tool.CallInput) (tool.Result, error) {
			runs++
			return tool.Result{}, errors.New("insufficient funds")
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := newFakeCallRecorder()
	ctx := control.WithCallRecorder(context.Background(), recorder)
	input := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewFunctionToolCallBlock("call-1", "pay", `{}`))}
	first, _ := node.Invoke(ctx, input)
	second, _ := node.Invoke(control.WithCallRecorder(context.Background(), recorder.replay()), input)
	if runs != 1 {
		t.Fatalf("runs=%d", runs)
	}
	firstText, secondText := toolResultText(first[0]), toolResultText(second[0])
	if firstText == "" || firstText != secondText {
		t.Fatalf("first=%v second=%v", first, second)
	}
}

// errProbe 是本测试用的框架哨兵：等价于子图恢复校验类错误。
var errProbe = control.Sentinel("recovery incompatible with saved progress")

// 同批挂起信号与框架错误并发时，框架错误必须优先上报——挂起可等待恢复，
// 校验失败不能被掩盖；覆盖「挂起先到」与「框架错误先到」两种时序。
func TestFrameworkErrorWinsOverSuspension(t *testing.T) {
	for _, delayFramework := range []bool{false, true} {
		t.Run(fmt.Sprintf("delayFramework=%v", delayFramework), func(t *testing.T) {
			pause := func(delay bool) {
				if delay {
					time.Sleep(20 * time.Millisecond)
				}
			}
			node, err := tool.NewToolsNode(tool.ToolNodeConfig{
				Tools: []tool.Tool{
					tool.NewFunctionTool(tool.Spec{Name: "wait"}, func(context.Context, tool.CallInput) (tool.Result, error) {
						pause(!delayFramework)
						return tool.Result{}, control.RequestInterrupt("approve", nil)
					}),
					tool.NewFunctionTool(tool.Spec{Name: "bad"}, func(context.Context, tool.CallInput) (tool.Result, error) {
						pause(delayFramework)
						return tool.Result{}, fmt.Errorf("child invoke: %w", errProbe)
					}),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			out, err := node.Invoke(context.Background(), []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-w", "wait", `{}`),
				schema.NewFunctionToolCallBlock("call-b", "bad", `{}`))})
			if !errors.Is(err, errProbe) {
				t.Fatalf("expected framework error, got err=%v out=%v", err, out)
			}
		})
	}
}
