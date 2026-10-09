package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/goharness/goharness/pkg/state"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/stream"
)

var (
	ErrInvalidResume        = control.Sentinel("invalid graph resume input")
	ErrInterruptMismatch    = control.Sentinel("interrupt replay does not match saved questions")
	ErrInterruptUnsupported = control.Sentinel("dynamic interrupt requires a sequential root graph node with a PreHandler")
)

// resumeInput 记录一条恢复输入；value 是原始值，data 在入口 finalize 时编码。
type resumeInput struct {
	id    string
	value any
	data  json.RawMessage
}

// WithResume 为指定中断提供 JSON 可序列化的输入，仅用于 Resume 系列入口。
// 该选项归属于 Graph 自身配置，只在当前执行入口解析，不进入节点选项。
func WithResume(id string, value any) runnable.Option {
	return runnable.WrapOption(func(c *graphRunConfig) {
		c.inputs = append(c.inputs, resumeInput{id: id, value: value})
	})
}

type inputRunKey struct{}
type inputSessionKey struct{}
type inputRun struct{ nested bool }
type inputSession struct {
	mu       sync.Mutex
	node     string
	step     int
	taskID   string // 任务级会话：分支任务的交互日志归属任务记录，非 frame
	entry    GraphStateCheckpoint
	visits   map[string]int
	records  []control.InterruptRecord
	disabled error
	terminal error
	closed   bool
	// pending 记录本次节点执行中已完成的工具调用（键为调用去重键），
	// 挂起时随进度落盘；恢复重放同批调用时命中者直接复用结果。
	pending map[string]json.RawMessage
	// batches 是本执行内已分配的调用批次序号，按序分配使恢复重放同一批
	// 时命中相同键，新批次获得新键。
	batches int
	// consumed 标记日志中已被消费的位置（仅本次执行内存态，不落盘）：
	// 恢复重放按调用归属消费，被结果复用跳过的调用由登记器代为标记。
	consumed map[int]bool
}

// inputInterrupt 保留发起节点的现场，流错误延迟到下游读取时也不会归错节点。
type inputInterrupt struct{ session *inputSession }

func (*inputInterrupt) Error() string { return "graph is waiting for external input" }
func (*inputInterrupt) Unwrap() error {
	return control.InterruptRequest{Reason: "waiting for external input"}
}

// Interrupt 首次返回中断请求；恢复后按调用顺序返回对应的 JSON 输入。
// 调用者必须向上传递 err。节点会从入口重放，不恢复 Go 调用栈。
func Interrupt(ctx context.Context, payload any) (any, error) {
	if ctx == nil {
		return nil, ErrInterruptUnsupported
	}
	s, _ := ctx.Value(inputSessionKey{}).(*inputSession)
	if s == nil {
		return nil, ErrInterruptUnsupported
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disabled != nil {
		return nil, s.disabled
	}
	if s.closed {
		return nil, ErrInterruptUnsupported
	}
	if s.terminal != nil {
		return nil, s.terminal
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		s.terminal = fmt.Errorf("interrupt payload is not JSON serializable: %w", err)
		return nil, s.terminal
	}
	// 按调用归属消费日志：优先取本调用未消费的第一条记录；无归属记录
	// （Call 为空）由调用级或节点级问题按顺序兜底消费——兼容旧日志。
	callKey, _ := control.CallKeyFromContext(ctx)
	index := -1
	for i := range s.records {
		if !s.consumed[i] && s.records[i].Call == callKey {
			index = i
			break
		}
	}
	if index < 0 && callKey != "" {
		for i := range s.records {
			if !s.consumed[i] && s.records[i].Call == "" {
				index = i
				break
			}
		}
	}
	if index >= 0 {
		record := s.records[index]
		if !bytes.Equal(data, record.Payload) {
			s.terminal = ErrInterruptMismatch
			return nil, s.terminal
		}
		if !record.Answered {
			s.terminal = &inputInterrupt{s}
			return nil, s.terminal
		}
		if s.consumed == nil {
			s.consumed = make(map[int]bool)
		}
		s.consumed[index] = true
		var value any
		decoder := json.NewDecoder(bytes.NewReader(record.Resume))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		return value, nil
	}
	s.records = append(s.records, control.InterruptRecord{ID: stream.NewID(), Node: s.node, Step: s.step, Index: len(s.records), Call: callKey, Payload: data})
	s.terminal = &inputInterrupt{s}
	return nil, s.terminal
}

func (s *inputSession) finish() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.terminal != nil {
		return s.terminal
	}
	if len(s.consumed) != len(s.records) {
		return ErrInterruptMismatch
	}
	return nil
}

func (e *InterruptedError) PendingInterrupts() []control.InterruptRecord {
	if e == nil {
		return nil
	}
	records := make([]control.InterruptRecord, len(e.interrupts))
	for i, r := range e.interrupts {
		records[i] = r.Clone()
	}
	return records
}

func pendingInterrupts(c executionProgress) []control.InterruptRecord {
	var records []control.InterruptRecord
	for _, record := range c.Interrupts {
		if !record.Answered {
			records = append(records, record.Clone())
		}
	}
	return records
}

// PendingInterrupts 从检查点读取待输入的问题，可用于页面刷新或进程重启。
func (g *CompiledGraph[I, O]) PendingInterrupts(ctx context.Context, options ...runnable.Option) ([]control.InterruptRecord, error) {
	if g == nil || g.runtime == nil {
		return nil, errors.New("compiled graph is nil")
	}
	runCtx, setup, err := g.runtime.prepareRun(ctx, options)
	if err != nil {
		return nil, err
	}
	c, err := g.runtime.loadCheckpointProgress(runCtx, &setup)
	// 纯查询路径没有运行记录：加载 span 以独立根 span 发射。
	setup.flushPendingLoad(runCtx)
	if err != nil {
		return nil, err
	}
	return pendingInterruptsTree(&c), nil
}

func validateInterruptRecords(c executionProgress) error {
	seen := map[string]bool{}
	for i, r := range c.Interrupts {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("%w: %v", state.ErrInvalidCheckpoint, err)
		}
		if seen[r.ID] || r.Node != c.ResumeNode || r.Step != c.Step || r.Index != i || (!r.Answered && i != len(c.Interrupts)-1) {
			return fmt.Errorf("%w: inconsistent interrupt journal", state.ErrInvalidCheckpoint)
		}
		seen[r.ID] = true
	}
	return nil
}
