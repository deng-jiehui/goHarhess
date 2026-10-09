package graph

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

const (
	DefaultCheckpointTimeout = 5 * time.Second
)

var (
	ErrInterrupted           = errors.New("graph execution interrupted")
	ErrCheckpointRequired    = control.Sentinel("graph interruption requires a checkpoint")
	ErrCheckpointNotFound    = control.Sentinel("graph checkpoint not found")
	ErrGraphRevisionMismatch = control.Sentinel("graph revision mismatch")
	ErrGraphNotResumable     = control.Sentinel("graph is not resumable")
	// ErrRunAbandoned 表示检查点上的运行已通过 state.AbandonRun 放弃，不能再恢复。
	ErrRunAbandoned = control.Sentinel("graph run was abandoned")
)

type InterruptPoint string

const (
	InterruptPointBeforeNode  InterruptPoint = "before_node"
	InterruptPointDuringNode  InterruptPoint = "during_node"
	InterruptPointAfterCommit InterruptPoint = "after_commit"
)

// executionFormatTree 是 Execution 的当前格式版本：每条记录保存一个执行
// frame，子调用进度经 childExecution.Key 指向的独立命名空间记录存储，
// 不再内嵌递归。
const executionFormatTree = 1

// executionProgress 保存一个执行 frame 在提交边界上的 State、下一批任务及
// 汇合记录；它同时是执行树的节点：Tasks 保存本轮分支任务的进度与交互日志，
// Children 登记本 frame 发起的子调用（进度在各自的命名空间记录中）。
// 不保存流；有 PreHandler 的任务由 PreHandler 从 State 重建输入，无
// PreHandler 的顺序任务在调度时把边输入冻结进 StepTask.Input（fork 组
// 任务仍恒由 Pre 重建）。
type executionProgress struct {
	Format        int                       `json:"format,omitempty"`
	Schedule      *StepSchedule             `json:"schedule,omitempty"`
	Interrupts    []control.InterruptRecord `json:"interrupts,omitempty"`
	GraphName     string                    `json:"graph_name"`
	GraphRevision string                    `json:"graph_revision"`
	ResumeNode    string                    `json:"resume_node"`
	Step          int                       `json:"step"`
	Visits        map[string]int            `json:"visits,omitempty"`
	State         GraphStateCheckpoint      `json:"state"`
	CursorStack   CursorStack               `json:"cursor_stack,omitempty"`
	Point         InterruptPoint            `json:"point"`
	Reason        string                    `json:"reason,omitempty"`
	Metadata      map[string]any            `json:"metadata,omitempty"`
	Tasks         []*taskProgress           `json:"tasks,omitempty"`
	Children      []*childExecution         `json:"children,omitempty"`
	// Managed 保存本 frame 内创建的受管子任务：身份、最终配置、状态、
	// 结果、收件箱与子执行进度。受管任务的生命周期不随创建任务结束，
	// 由根运行统一驱动、收敛与恢复。
	Managed []*managedTask `json:"managed,omitempty"`
	// Waiting 是挂起时的当前阻塞位置：本层问题、子调用或并行任务组。
	// 恢复判定以它为准，已回答的旧问题不会误唤醒后来阻塞的任务。
	Waiting *waitRecord `json:"waiting,omitempty"`
	// Pending 登记挂起节点内已完成的工具调用结果（键为调用去重键）；
	// 恢复重放节点时命中登记的调用直接复用结果，不再执行。
	Pending map[string]json.RawMessage `json:"pending,omitempty"`
	// Output 仅用于"末端等待"挂起：根图已路由到 End 但仍有受管任务挂起，
	// 挂起时把进入 End 的边输出冻结在此，恢复后直接产出，不重放末节点。
	Output json.RawMessage `json:"output,omitempty"`
}

func (c executionProgress) Clone() executionProgress {
	c.Schedule = c.Schedule.clone()
	c.Interrupts = append([]control.InterruptRecord(nil), c.Interrupts...)
	for i := range c.Interrupts {
		c.Interrupts[i] = c.Interrupts[i].Clone()
	}
	c.Visits = cloneVisits(c.Visits)
	c.State = c.State.Clone()
	c.CursorStack = c.CursorStack.Clone()
	c.Metadata = clonePortableMetadata(c.Metadata)
	c.Tasks = cloneTaskProgress(c.Tasks)
	c.Children = cloneChildExecutions(c.Children)
	c.Managed = cloneManagedTasks(c.Managed)
	c.Waiting = c.Waiting.Clone()
	c.Pending = clonePendingCalls(c.Pending)
	if c.Output != nil {
		c.Output = append(json.RawMessage(nil), c.Output...)
	}
	return c
}

func (c executionProgress) Validate() error {
	if c.Format < 0 || c.Format > executionFormatTree {
		return fmt.Errorf("%w: unsupported execution format %d", state.ErrInvalidCheckpoint, c.Format)
	}
	if err := validateInterruptRecords(c); err != nil {
		return err
	}
	if err := validateTaskProgress(c.Tasks); err != nil {
		return err
	}
	if err := validateChildExecutions(c.Children); err != nil {
		return err
	}
	if err := validateManagedTasks(c.Managed); err != nil {
		return err
	}
	if strings.TrimSpace(c.GraphName) == "" || strings.TrimSpace(c.GraphRevision) == "" {
		return fmt.Errorf("%w: graph name and revision are required", state.ErrInvalidCheckpoint)
	}
	switch {
	case strings.TrimSpace(c.ResumeNode) == "" || c.ResumeNode == Start:
		return fmt.Errorf("%w: invalid resume node %q", state.ErrInvalidCheckpoint, c.ResumeNode)
	case c.ResumeNode == End:
		// 末端等待挂起只在提交边界之后成立，且必须携带已冻结的输出。
		if c.Point != InterruptPointAfterCommit || len(c.Output) == 0 {
			return fmt.Errorf("%w: invalid resume node %q", state.ErrInvalidCheckpoint, c.ResumeNode)
		}
	}
	if c.Step < 0 {
		return fmt.Errorf("%w: step cannot be negative", state.ErrInvalidCheckpoint)
	}
	if c.Schedule != nil {
		if err := c.Schedule.validate(); err != nil {
			return fmt.Errorf("%w: %v", state.ErrInvalidCheckpoint, err)
		}
		if len(c.Schedule.Tasks) > 0 && c.Schedule.Tasks[0].Node != c.ResumeNode {
			return fmt.Errorf("%w: resume node does not match next task", state.ErrInvalidCheckpoint)
		}
	}
	for node, visits := range c.Visits {
		if strings.TrimSpace(node) == "" || visits < 0 {
			return fmt.Errorf("%w: invalid visit count for %q", state.ErrInvalidCheckpoint, node)
		}
	}
	switch c.Point {
	case InterruptPointBeforeNode, InterruptPointDuringNode, InterruptPointAfterCommit:
	default:
		return fmt.Errorf("%w: unsupported interrupt point %q", state.ErrInvalidCheckpoint, c.Point)
	}
	if err := c.State.Validate(); err != nil {
		return fmt.Errorf("%w: %v", state.ErrInvalidCheckpoint, err)
	}
	if err := c.CursorStack.Validate(); err != nil {
		return fmt.Errorf("%w: %v", state.ErrInvalidCheckpoint, err)
	}
	if _, err := json.Marshal(c.Metadata); err != nil {
		return fmt.Errorf("%w: metadata is not JSON serializable: %v", state.ErrInvalidCheckpoint, err)
	}
	return nil
}

func (c executionProgress) JSON() ([]byte, error) {
	copy := c.Clone()
	if err := copy.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(copy)
}

func decodeExecutionProgress(data []byte) (executionProgress, error) {
	var progress executionProgress
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&progress); err != nil {
		return executionProgress{}, fmt.Errorf("%w: decode JSON: %v", state.ErrInvalidCheckpoint, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return executionProgress{}, fmt.Errorf("%w: trailing execution data", state.ErrInvalidCheckpoint)
	}
	if err := progress.Validate(); err != nil {
		return executionProgress{}, err
	}
	if err := validateExecutionTree(&progress, 0); err != nil {
		return executionProgress{}, err
	}
	return progress.Clone(), nil
}

// InterruptedError 只在检查点保存成功后返回，Checkpoint 是实际存储记录的独立副本。
type InterruptedError struct {
	Point      InterruptPoint
	Reason     string
	Checkpoint state.Checkpoint
	interrupts []control.InterruptRecord
}

func (e *InterruptedError) Error() string {
	if e == nil {
		return ErrInterrupted.Error()
	}
	return fmt.Sprintf("%v at %s: %s", ErrInterrupted, e.Point, e.Reason)
}

func (*InterruptedError) Unwrap() error { return ErrInterrupted }

// checkpointConfig 集中承载检查点持久化参数与共享记录指针。coord 非 nil
// 表示本运行参与执行树持久化：frame 指向树中本运行负责的 frame 槽位
// （根运行与命名空间子运行为各自 coord.root，受管运行为根树中的
// managedTask.Frame），进度提交先原地更新该 frame 再编码落盘。
// recordView 以保护性互斥持有当前检查点记录：后台写入成功后整体替换，
// 读取方在锁内取得稳定视图。记录各切片/映射字段按不可变约定共享，
// 调用方不得原地修改取回的字段。
type recordView struct {
	mu     sync.Mutex
	record state.Checkpoint
}

func (v *recordView) load() state.Checkpoint {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.record
}

func (v *recordView) store(c state.Checkpoint) {
	v.mu.Lock()
	v.record = c
	v.mu.Unlock()
}

// conversation 返回记录当前持有的完整会话视图；不可原地修改。
func (v *recordView) conversation() state.Conversation {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.record.Messages
}

type checkpointConfig struct {
	Store   state.Store
	Key     string
	Timeout time.Duration
	record  *recordView
	writer  *checkpointWriter
	coord   *runCoordinator
	frame   *executionProgress
}

// enabled 判断本次运行是否具备落盘能力：协调模式看协调器与记录槽位，
// 独立模式看存储与会话 ID。内存受管运行（storeless 协调器）不具备
// 落盘能力——挂起恢复语义受限，审批类中断会被拒绝。
func (c checkpointConfig) enabled() bool {
	if c.coord != nil {
		return !c.coord.storeless && c.writer != nil && c.record != nil
	}
	return !isNilCheckpointStore(c.Store) && strings.TrimSpace(c.Key) != ""
}

// graphRunConfig 是 Graph 自身在运行入口消费的类型化配置：检查点、持久化
// 模式与恢复输入。这些选项只在当前 Graph 入口解析，不传入节点或子图。
type graphRunConfig struct {
	checkpoint    checkpointConfig
	hasCheckpoint bool
	durability    Durability
	inputs        []resumeInput
	// managedBuilders 是受管任务构建函数注册表；只在根运行生效，
	// 嵌套运行共享根协调器的同一份注册。
	managedBuilders map[string]managedBuilderEntry
}

// graphRunConfigType 是 Graph 运行配置的归属类型；未导出，外部组件无法声明接收。
var graphRunConfigType = runnable.OptionType[graphRunConfig]()

// finalizeRunConfig 在入口解析后统一校验：不合法的执行配置在此报错。
func (c *graphRunConfig) finalize() error {
	if c.hasCheckpoint {
		if isNilCheckpointStore(c.checkpoint.Store) {
			return errors.New("graph checkpoint store cannot be nil")
		}
		c.checkpoint.Key = strings.TrimSpace(c.checkpoint.Key)
		if c.checkpoint.Key == "" {
			return errors.New("graph checkpoint key cannot be empty")
		}
		if c.checkpoint.Timeout <= 0 {
			c.checkpoint.Timeout = DefaultCheckpointTimeout
		}
	}
	if c.durability != "" {
		if err := c.durability.validate(); err != nil {
			return err
		}
	}
	seen := map[string]struct{}{}
	for index, input := range c.inputs {
		if strings.TrimSpace(input.id) == "" {
			return fmt.Errorf("%w: interrupt ID is required", ErrInvalidResume)
		}
		data, err := json.Marshal(input.value)
		if err != nil {
			return fmt.Errorf("%w: input is not JSON serializable: %v", ErrInvalidResume, err)
		}
		if _, exists := seen[input.id]; exists {
			return fmt.Errorf("%w: duplicate ID %q", ErrInvalidResume, input.id)
		}
		seen[input.id] = struct{}{}
		c.inputs[index].data = data
	}
	return nil
}

func WithCheckpoint(store state.Store, key string) runnable.Option {
	return runnable.WrapOption(func(c *graphRunConfig) {
		c.checkpoint = checkpointConfig{Store: store, Key: key, Timeout: DefaultCheckpointTimeout}
		c.hasCheckpoint = true
	})
}

// Thread 选择本次运行的检查点线程 ID。与编译期 Checkpointer(store) 配对：
// Compile(graph.Checkpointer(store)) 绑店，Invoke/Resume 传 Thread 启用；
// 未绑定存储时校验拒绝。
func Thread(key string) runnable.Option {
	return runnable.WrapOption(func(c *graphRunConfig) {
		c.checkpoint.Key = key
		c.hasCheckpoint = true
	})
}

func isNilCheckpointStore(store state.Store) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// encodeTaskInput 在调度时刻把无 Pre 节点的边输入冻结为 JSON。
// 编码发生在执行线程：节点随后可能修改这个对象，已保存的字节不受影响。
// 编码后立即解码比对，动态值（如 map[string]any 中的数字变 float64）在此拒绝。
func encodeTaskInput(input any, inputType reflect.Type) (json.RawMessage, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("task input is not JSON encodable: %w", err)
	}
	decoded, err := decodeTaskInput(data, inputType)
	if err != nil {
		return nil, err
	}
	if source := reflect.ValueOf(input); source.IsValid() {
		if err := compareDynamicTypes(source, reflect.ValueOf(decoded), typeName(inputType), map[visitPair]bool{}); err != nil {
			return nil, fmt.Errorf("task input is not JSON round-trip stable: %w; add a PreHandler to rebuild the input", err)
		}
	}
	return data, nil
}

// encodeTaskOutput 冻结任务边输出供恢复选路：与任务输入同一份往返稳定
// 校验，但提示针对输出侧——不稳定时该把选路需要的数据经 PostHandler
// 收进 State，或换用 JSON 往返稳定的输出类型，而不是去改输入 PreHandler。
func encodeTaskOutput(output any, outputType reflect.Type) (json.RawMessage, error) {
	data, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("task output is not JSON encodable: %w", err)
	}
	decoded, err := decodeTaskInput(data, outputType)
	if err != nil {
		return nil, err
	}
	if source := reflect.ValueOf(output); source.IsValid() {
		if err := compareDynamicTypes(source, reflect.ValueOf(decoded), typeName(outputType), map[visitPair]bool{}); err != nil {
			return nil, fmt.Errorf("task output is not JSON round-trip stable: %w; move routing-relevant data into State via a PostHandler or use a JSON-stable output type", err)
		}
	}
	return data, nil
}

// decodeTaskInput 用节点的静态输入类型恢复任务输入；接口类型已在编译期被拒绝。
func decodeTaskInput(data json.RawMessage, inputType reflect.Type) (any, error) {
	if inputType == nil {
		return nil, errors.New("node declares no input type")
	}
	ptr := reflect.New(inputType)
	if err := json.Unmarshal(data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("decode task input: %w", err)
	}
	return ptr.Elem().Interface(), nil
}

// validateTaskInputType 校验无 Pre 节点的输入类型能否经 JSON 往返恢复。
// 接口类型不能保留具体类型（含 io.Reader 等运行资源），与流句柄一样不可持久化。
func validateTaskInputType(inputType reflect.Type) error {
	if inputType == nil {
		return errors.New("node declares no input type")
	}
	base := inputType
	for base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.Kind() == reflect.Interface {
		return fmt.Errorf("input type %s loses its concrete type when persisted", typeName(inputType))
	}
	if err := validateDefaultJSONType(inputType, typeName(inputType), map[reflect.Type]bool{}); err != nil {
		return errors.New(strings.ReplaceAll(err.Error(), "default State Codec", "default task input encoding"))
	}
	return nil
}

func cloneVisits(source map[string]int) map[string]int {
	if source == nil {
		return nil
	}
	copy := make(map[string]int, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func clonePortableMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	data, err := json.Marshal(source)
	if err != nil {
		copy := make(map[string]any, len(source))
		for key, value := range source {
			copy[key] = value
		}
		return copy
	}
	var copy map[string]any
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil
	}
	return copy
}

// WithCheckpointTimeout 设置单次同步写入的最长等待时间。
func WithCheckpointTimeout(store state.Store, key string, timeout time.Duration) runnable.Option {
	return runnable.WrapOption(func(c *graphRunConfig) {
		c.checkpoint = checkpointConfig{Store: store, Key: key, Timeout: timeout}
		c.hasCheckpoint = true
	})
}
