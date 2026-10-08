package graph

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/goharness/goharness/pkg/control"
)

// pendingEntry 是随进度落盘的单次调用登记：Data 为工具运行时序列化的结果
// 投影，Journal 是该调用在本次执行中消费过的交互日志条数——恢复命中登记
// 时同步标记对应日志已消费，避免后续调用消费错位。
type pendingEntry struct {
	Data    json.RawMessage `json:"data"`
	Journal int             `json:"journal,omitempty"`
}

// runScopeContextKey 承载本次根运行的身份：启用检查点时为持久化的 RunID，
// 否则为运行入口生成的随机 ID；嵌套运行共享根身份并以子调用身份分层。
type runScopeContextKey struct{}

func runScopeFromContext(ctx context.Context) string {
	scope, _ := ctx.Value(runScopeContextKey{}).(string)
	return scope
}

func withRunScope(ctx context.Context, scope string) context.Context {
	if ctx == nil || scope == "" {
		return ctx
	}
	return context.WithValue(ctx, runScopeContextKey{}, scope)
}

// sessionCallRecorder 按批次登记会话内已完成的调用结果；批次序号在会话内
// 单调递增且恢复重放顺序一致，使「同批恢复」命中而「新批次」落空。
// scope 是运行级身份前缀（运行标识/任务或节点路径）。
type sessionCallRecorder struct {
	session *inputSession
	scope   string
}

func (r sessionCallRecorder) BeginBatch() control.CallBatch {
	s := r.session
	s.mu.Lock()
	seq := s.batches
	s.batches++
	s.mu.Unlock()
	return sessionCallBatch{session: s, seq: seq, namespace: r.scope}
}

// sessionCallBatch 是一批调用的恢复作用域：LookupPending 命中时连带把该
// 调用消费过的交互日志标记为已消费，保证后续调用的日志游标不错位。
type sessionCallBatch struct {
	session   *inputSession
	seq       int
	namespace string
}

func (b sessionCallBatch) Namespace() string {
	return b.namespace + "/" + strconv.Itoa(b.seq)
}

func (b sessionCallBatch) CallKey(base string) string {
	return strconv.Itoa(b.seq) + "/" + base
}

func (b sessionCallBatch) LookupPending(key string) ([]byte, bool) {
	s := b.session
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.pending[key]
	if !ok {
		return nil, false
	}
	var entry pendingEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, false
	}
	// 该调用已被结果复用：把它此前消费过的日志标记为已消费，
	// 使仍在重放的调用按各自归属继续消费，而不是从头错位。
	if s.consumed == nil {
		s.consumed = make(map[int]bool)
	}
	marked := 0
	for i := range s.records {
		if marked >= entry.Journal {
			break
		}
		if s.records[i].Call == key && !s.consumed[i] {
			s.consumed[i] = true
			marked++
		}
	}
	return append([]byte(nil), entry.Data...), true
}

func (b sessionCallBatch) RecordPending(key string, payload []byte) {
	s := b.session
	s.mu.Lock()
	defer s.mu.Unlock()
	journal := 0
	for i := range s.records {
		if s.records[i].Call == key {
			journal++
		}
	}
	raw, err := json.Marshal(pendingEntry{Data: payload, Journal: journal})
	if err != nil {
		return
	}
	if s.pending == nil {
		s.pending = make(map[string]json.RawMessage)
	}
	s.pending[key] = raw
}

// withCallRecorder 把会话登记器装入节点执行上下文；path 为该节点执行的
// 稳定位置（任务 ID 或节点@步数）。
func withCallRecorder(ctx context.Context, session *inputSession, path string) context.Context {
	if ctx == nil || session == nil {
		return ctx
	}
	return control.WithCallRecorder(ctx, sessionCallRecorder{session: session, scope: runScopeFromContext(ctx) + "/" + path})
}

// clonePendingCalls 深拷贝 pending 区；载荷逐条复制，读写方不共享字节。
func clonePendingCalls(in map[string]json.RawMessage) map[string]json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(in))
	for key, raw := range in {
		out[key] = append(json.RawMessage(nil), raw...)
	}
	return out
}
