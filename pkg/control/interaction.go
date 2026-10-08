package control

import (
	"encoding/json"
	"errors"
	"fmt"
)

// InterruptRecord 是通用交互记录；框架不解释问题和输入的业务含义。
type InterruptRecord struct {
	ID    string `json:"id"`
	Node  string `json:"node"`
	Step  int    `json:"step"`
	Index int    `json:"index"`
	// Call 标记本条问题归属的调用身份（工具调用的会话内去重键）；
	// 为空表示节点级问题。恢复重放时各调用只消费自己的记录，
	// 被 pending 结果跳过的调用不再消费日志，游标因此不错位。
	Call     string          `json:"call,omitempty"`
	Payload  json.RawMessage `json:"payload"`
	Answered bool            `json:"answered"`
	Resume   json.RawMessage `json:"resume,omitempty"`
}

func (r InterruptRecord) Clone() InterruptRecord {
	r.Payload = append(json.RawMessage(nil), r.Payload...)
	r.Resume = append(json.RawMessage(nil), r.Resume...)
	return r
}

func (r InterruptRecord) Validate() error {
	if r.ID == "" || r.Node == "" || r.Step < 0 || r.Index < 0 || !json.Valid(r.Payload) {
		return fmt.Errorf("invalid interrupt record %q", r.ID)
	}
	if (r.Answered && !json.Valid(r.Resume)) || (!r.Answered && len(r.Resume) != 0) {
		return fmt.Errorf("invalid resume value for interrupt %q", r.ID)
	}
	return nil
}

// Suspension 只由执行器在检查点保存成功后返回，事件层据此识别挂起。
type Suspension interface {
	error
	PendingInterrupts() []InterruptRecord
}

func AsSuspension(err error) (Suspension, bool) {
	var suspended Suspension
	ok := errors.As(err, &suspended)
	return suspended, ok
}
