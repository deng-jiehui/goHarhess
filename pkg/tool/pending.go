package tool

import (
	"encoding/json"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/schema"
)

// pendingCallResult 是随检查点落盘的单次调用结果投影：只保留面向模型的
// 内容块与错误标记，调用方原始 Data 结构不进入检查点。
type pendingCallResult struct {
	IsError bool                  `json:"is_error,omitempty"`
	Blocks  []schema.ContentBlock `json:"blocks,omitempty"`
}

// lookupPendingCall 查询本批次下已完成的调用结果；键为 CallBatch.CallKey
// 生成的会话内身份。
func lookupPendingCall(batch control.CallBatch, key string) (Result, bool) {
	if key == "" {
		return Result{}, false
	}
	raw, ok := batch.LookupPending(key)
	if !ok {
		return Result{}, false
	}
	var stored pendingCallResult
	if err := json.Unmarshal(raw, &stored); err != nil {
		return Result{}, false
	}
	return Result{ContentBlocks: stored.Blocks, IsError: stored.IsError}, true
}

// recordPendingCall 把一次已完成调用写入本批次；序列化失败不登记——
// 无法安全落盘的结果宁可重放执行。
func recordPendingCall(batch control.CallBatch, key string, result Result) {
	if key == "" {
		return
	}
	raw, err := json.Marshal(pendingCallResult{IsError: result.IsError, Blocks: result.ContentBlocks})
	if err != nil {
		return
	}
	batch.RecordPending(key, raw)
}

// suspendCaused 判断错误是否来自挂起传播（交互等待或子调用挂起）；
// 这类错误的提示消息不是调用完成结果，恢复时调用须重新执行以重放交互。
func suspendCaused(err error) bool {
	if _, ok := control.AsInterrupt(err); ok {
		return true
	}
	_, ok := control.AsSuspension(err)
	return ok
}
