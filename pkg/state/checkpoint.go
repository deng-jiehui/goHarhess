// Package state 统一保存会话内容、执行进度和历史检查点。
package state

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/schema"
)

var (
	ErrInvalidCheckpoint = control.Sentinel("invalid checkpoint")
	ErrNilContext        = errors.New("checkpoint context cannot be nil")
	ErrConflict          = errors.New("checkpoint version conflict")
)

const CurrentCheckpointSchemaVersion = 3

// Checkpoint 是 Graph 和 Agent 共用的持久化记录，包含会话、执行进度与结果。
// SchemaVersion 管理整份记录的格式；Version 是每次保存递增的乐观锁版本。
type Checkpoint struct {
	ID       string `json:"id"`
	ParentID string `json:"parent_id,omitempty"`
	RunID    string `json:"run_id,omitempty"`
	Status   string `json:"status,omitempty"`
	// Execution 是执行器内部编码的进度，随整份检查点保存，不单独版本化。
	Execution      json.RawMessage  `json:"execution,omitempty"`
	Result         json.RawMessage  `json:"result,omitempty"`
	SchemaVersion  int              `json:"schema_version"`
	ConversationID string           `json:"conversation_id"`
	Version        uint64           `json:"version"`
	Messages       []schema.Message `json:"messages,omitempty"`
	// MessagesFrom/MessagesEpoch 是 Store 内部的消息增量标记：
	// MessagesFrom > 0 时 Messages 只保存尾部新增，完整会话 = 该会话
	// 消息日志前 MessagesFrom 条 + Messages；MessagesEpoch 标识会话
	// 日志代次，前缀不匹配分叉时递增。调用方始终提供完整会话；
	// Load/Get/List 返回的始终是物化后的完整会话且两个字段为 0。
	MessagesFrom  int            `json:"messages_from,omitempty"`
	MessagesEpoch int            `json:"messages_epoch,omitempty"`
	Values        map[string]any `json:"values,omitempty"`
	// SubAgents 是会话级的子 Agent 集合（由执行引擎编码）：与 Execution
	// 不同，它属于会话而不属于某一次运行——新一轮运行清空 Execution 时
	// 原样保留，只在运行内随子 Agent 的创建、任务结算和关闭更新。
	SubAgents json.RawMessage `json:"sub_agents,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func (c Checkpoint) Clone() Checkpoint {
	c.Execution = append(json.RawMessage(nil), c.Execution...)
	c.Result = append(json.RawMessage(nil), c.Result...)
	c.SubAgents = append(json.RawMessage(nil), c.SubAgents...)
	c.Messages = cloneMessages(c.Messages)
	c.Values = cloneMap(c.Values)
	return c
}

func (c Checkpoint) Validate() (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", ErrInvalidCheckpoint, err)
		}
	}()
	if strings.TrimSpace(c.ConversationID) == "" {
		return errors.New("checkpoint conversation ID cannot be empty")
	}
	if c.SchemaVersion != 0 && c.SchemaVersion != CurrentCheckpointSchemaVersion {
		return fmt.Errorf("unsupported checkpoint schema version %d", c.SchemaVersion)
	}
	for index, message := range c.Messages {
		if err := message.Validate(); err != nil {
			return fmt.Errorf("checkpoint message %d: %w", index, err)
		}
	}
	if err := validateJSON("checkpoint messages", c.Messages); err != nil {
		return err
	}
	if err := validateJSON("checkpoint values", c.Values); err != nil {
		return err
	}
	if len(c.Execution) > 0 && !json.Valid(c.Execution) {
		return errors.New("invalid execution JSON")
	}
	if len(c.Result) > 0 && !json.Valid(c.Result) {
		return errors.New("invalid result JSON")
	}
	if len(c.SubAgents) > 0 && !json.Valid(c.SubAgents) {
		return errors.New("invalid sub agents JSON")
	}
	switch c.Status {
	case "", StatusRunning, StatusInterrupted, StatusCompleted, StatusAbandoned:
	default:
		return errors.New("invalid checkpoint status")
	}
	return nil
}

// JSON returns a validated, portable representation suitable for transport to
// another process or device.
func (c Checkpoint) JSON() ([]byte, error) {
	prepared := prepareCheckpoint(c)
	if err := prepared.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(prepared)
}

// DecodeCheckpoint validates a checkpoint received from an external system.
func DecodeCheckpoint(data []byte) (checkpoint Checkpoint, err error) {
	defer func() {
		if err != nil && !errors.Is(err, ErrInvalidCheckpoint) {
			err = fmt.Errorf("%w: %w", ErrInvalidCheckpoint, err)
		}
	}()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return Checkpoint{}, fmt.Errorf("decode checkpoint JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return Checkpoint{}, fmt.Errorf("decode checkpoint JSON: %w", err)
	}
	if checkpoint.SchemaVersion != CurrentCheckpointSchemaVersion {
		return Checkpoint{}, fmt.Errorf("incompatible checkpoint schema version %d", checkpoint.SchemaVersion)
	}
	checkpoint = prepareCheckpoint(checkpoint)
	if err := checkpoint.Validate(); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint.Clone(), nil
}

// ConflictError reports optimistic-lock details for a rejected Save.
type ConflictError struct {
	ConversationID string
	Expected       uint64
	Actual         uint64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("checkpoint %q version conflict: expected %d, actual %d", e.ConversationID, e.Expected, e.Actual)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

// 检查点状态同时描述执行进度和会话提交状态。
const (
	StatusRunning     = "running"
	StatusInterrupted = "interrupted"
	StatusCompleted   = "completed"
	// StatusAbandoned 表示未完成的运行已被放弃，会话内容回退到该运行开始前。
	StatusAbandoned = "abandoned"
)

// Unfinished 报告检查点是否停在未完成的运行上；此时新输入需先 Resume 或 AbandonRun。
func (c Checkpoint) Unfinished() bool {
	return c.Status == StatusRunning || c.Status == StatusInterrupted
}

// ListOptions 按新到旧分页，Before 不包含指定记录；Limit 为零时使用 100。
type ListOptions struct {
	Before string
	Limit  int
}

// ConversationProvider 由消息型 State 实现，通用 Graph 无需拥有会话字段。
// Conversation 是检查点持久化的完整会话消息序列。别名让不能直接
// 依赖 schema 的包（如 graph）也能传递会话数据。
type Conversation = []schema.Message

type ConversationProvider interface{ CheckpointMessages() []schema.Message }

// ConversationExternalizer 在根持久化边界由执行引擎调用：标记后 State
// 的序列化以会话引用代替完整会话，避免每步重复保存完整历史。内存中的
// State 不受影响，只改变随后的编码输出。
type ConversationExternalizer interface {
	MarkConversationExternal()
}

// ConversationRestorer 由解码出的外部化 State 实现：NeedsConversation
// 报告会话字段被外部化、必须由 RestoreConversation 回填后才能继续执行；
// 恢复方用检查点的完整会话回填，长度不足以覆盖引用时返回错误。
type ConversationRestorer interface {
	NeedsConversation() bool
	RestoreConversation(messages []schema.Message) error
}

// RollbackBasisReader 由实现了历史保留裁剪的 Store 提供：返回当前运行
// 开始前（RunID 不同的最近一条记录）的回退依据。该依据独立于可淘汰的
// 查询历史保留，供 AbandonRun 恢复运行开始前的会话与 Values；
// 未实现的 Store 回退到沿父链遍历。
type RollbackBasisReader interface {
	RollbackBasis(ctx context.Context, conversationID string) (Checkpoint, bool, error)
}

// NewID 生成不依赖时钟、可跨进程使用的记录标识。
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Store 是 Graph 和 Agent 共用的持久化边界。
// Save 使用传入的 Version 和 ID 校验最新记录，再原子追加历史和更新最新指针。
// 返回值携带新 ID、ParentID 和递增版本；Delete 删除整个会话及其历史。
type Store interface {
	Get(ctx context.Context, conversationID, checkpointID string) (Checkpoint, bool, error)
	List(ctx context.Context, conversationID string, options ListOptions) ([]Checkpoint, error)
	Load(ctx context.Context, conversationID string) (Checkpoint, bool, error)
	Save(ctx context.Context, checkpoint Checkpoint) (Checkpoint, error)
	Delete(ctx context.Context, conversationID string) error
}

func validateContextAndID(ctx context.Context, conversationID string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(conversationID) == "" {
		return errors.New("checkpoint conversation ID cannot be empty")
	}
	return nil
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	if data, err := json.Marshal(input); err == nil {
		var cloned map[string]any
		if err := json.Unmarshal(data, &cloned); err == nil {
			return cloned
		}
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = cloneValue(value)
	}
	return cloned
}

func cloneMessages(messages []schema.Message) []schema.Message {
	if messages == nil {
		return nil
	}
	// 持久化边界用明确的结构深拷贝；先前的 JSON 往返在每次保存都要
	// 重新编码整个会话，且会把 Extra 中的具体类型归一化为通用 JSON 类型。
	return schema.CloneMessages(messages)
}

// PersistClone 是持久化边界的深拷贝：能结构克隆的值保留具体 Go 类型；
// 结构克隆不支持且未实现 Clone() 的元数据值经 JSON 往返转换为通用
// JSON 类型副本；Clone() panic、返回类型错误、JSON 转换失败等硬错误
// 直接上报，不被保底分支掩盖。调用方与 Store 之间双向隔离。
func (c Checkpoint) PersistClone() (Checkpoint, error) {
	prepared := c
	prepared.Execution = append(json.RawMessage(nil), prepared.Execution...)
	prepared.Result = append(json.RawMessage(nil), prepared.Result...)
	prepared.SubAgents = append(json.RawMessage(nil), prepared.SubAgents...)
	prepared.Values = cloneMap(prepared.Values)
	messages, err := cloneMessagesPersist(prepared.Messages)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("%w: %w", ErrInvalidCheckpoint, err)
	}
	prepared.Messages = messages
	if prepared.SchemaVersion == 0 {
		prepared.SchemaVersion = CurrentCheckpointSchemaVersion
	}
	return prepared, nil
}

// cloneMessagesPersist 单趟严格遍历完成持久化克隆：普通消息即快路径；
// 只在实际无法结构克隆的值处（CloneUnsupportedError）交给 persistFallback
// 做 JSON 保底——通用 map/slice 就地容纳转换结果，旁边的 int、Clone 类型
// 保持原样；强类型容器装不下转换结果时才扩大到容器整体。Clone() panic、
// 返回类型错误、JSON 失败等硬错误直接上抛，错误路径携带消息下标与
// Extra 字段位置（schema 生成的 messages[i].extra["k"] 形式）。
func cloneMessagesPersist(messages []schema.Message) ([]schema.Message, error) {
	if messages == nil {
		return nil, nil
	}
	return schema.CloneMessagesFallback(messages, persistFallbackValue)
}

// persistFallbackValue 把无法结构克隆的值转换为独立的通用类型副本。
// 转换前先用 schema 的完整校验（含类型化 nil 检查、Clone() 调用与返回
// 类型校验）遍历内部所有 Clone() 实现，确保保底不掩盖克隆实现失败。
func persistFallbackValue(path string, value any) (any, error) {
	if err := verifyCloneables(value, path, 0, map[uintptr]bool{}); err != nil {
		return nil, err
	}
	generic, err := jsonGenericCopy(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return generic, nil
}

// verifyCloneables 遍历值中所有实现 Clone() 的节点并用 schema.CloneValueStrict
// 完整校验：类型化 nil 直接放行，panic 与返回类型错误都作为硬错误上报，
// 确保 JSON 保底不会掩盖克隆实现失败。返回副本被丢弃，内部隔离由实现契约负责。
func verifyCloneables(value any, path string, depth int, seen map[uintptr]bool) error {
	if value == nil || depth > maxCloneWalkDepth {
		return nil
	}
	rv := reflect.ValueOf(value)
	if clonePersistKindCanBeNil(rv.Kind()) && rv.IsNil() {
		return nil
	}
	if _, ok := value.(interface{ Clone() any }); ok {
		if _, err := schema.CloneValueStrict(value); err != nil {
			return fmt.Errorf("clone %s: %w", path, err)
		}
		return nil
	}
	switch rv.Kind() {
	case reflect.Interface:
		return verifyCloneables(rv.Elem().Interface(), path, depth+1, seen)
	case reflect.Pointer:
		ptr := rv.Pointer()
		if seen[ptr] {
			return nil
		}
		seen[ptr] = true
		return verifyCloneables(rv.Elem().Interface(), path, depth+1, seen)
	case reflect.Map:
		iterator := rv.MapRange()
		for iterator.Next() {
			if err := verifyCloneables(iterator.Key().Interface(), path, depth+1, seen); err != nil {
				return err
			}
			if err := verifyCloneables(iterator.Value().Interface(), path, depth+1, seen); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < rv.Len(); index++ {
			if err := verifyCloneables(rv.Index(index).Interface(), fmt.Sprintf("%s[%d]", path, index), depth+1, seen); err != nil {
				return err
			}
		}
	case reflect.Struct:
		typeOf := rv.Type()
		for index := 0; index < rv.NumField(); index++ {
			field := rv.Field(index)
			if !field.CanInterface() {
				continue
			}
			if err := verifyCloneables(field.Interface(), fmt.Sprintf("%s.%s", path, typeOf.Field(index).Name), depth+1, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

const maxCloneWalkDepth = 64

func clonePersistKindCanBeNil(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

// sharedMessagePrefix 返回 provided 相对 log 的最长公共前缀长度，
// 供持久化增量判定：会话按只追加约定增长，分叉（截断/改写历史）时
// 公共前缀不足覆盖任一侧。
func sharedMessagePrefix(provided, log []schema.Message) int {
	n := len(log)
	if len(provided) < n {
		n = len(provided)
	}
	shared := 0
	for shared < n && reflect.DeepEqual(provided[shared], log[shared]) {
		shared++
	}
	return shared
}

// materializeCheckpointMessages 把存储的增量视图还原为完整会话并清空
// Store 内部标记；from 超出日志范围时防御性退化为已存的尾部数据。
func materializeCheckpointMessages(log []schema.Message, c Checkpoint) Checkpoint {
	from := c.MessagesFrom
	if from <= 0 || from > len(log) {
		c.MessagesFrom = 0
		c.MessagesEpoch = 0
		return c
	}
	messages := make([]schema.Message, 0, from+len(c.Messages))
	messages = append(messages, log[:from]...)
	messages = append(messages, c.Messages...)
	c.Messages = messages
	c.MessagesFrom = 0
	c.MessagesEpoch = 0
	return c
}

// jsonGenericCopy 经 JSON 往返生成独立的通用类型副本（map[string]any、
// []any、string、float64、bool、nil），用于结构克隆不支持的元数据值。
func jsonGenericCopy(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var generic any
	if err := json.Unmarshal(data, &generic); err != nil {
		return nil, err
	}
	return generic, nil
}

func cloneValue(value any) any {
	switch current := value.(type) {
	case map[string]any:
		return cloneMap(current)
	case []any:
		cloned := make([]any, len(current))
		for index := range current {
			cloned[index] = cloneValue(current[index])
		}
		return cloned
	case []string:
		return append([]string(nil), current...)
	case []int:
		return append([]int(nil), current...)
	default:
		return value
	}
}

func validateJSON(label string, value any) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("%s is not JSON serializable: %w", label, err)
	}
	return nil
}

func prepareCheckpoint(checkpoint Checkpoint) Checkpoint {
	prepared := checkpoint.Clone()
	if prepared.SchemaVersion == 0 {
		prepared.SchemaVersion = CurrentCheckpointSchemaVersion
	}
	return prepared
}
