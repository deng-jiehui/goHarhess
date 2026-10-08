package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/state"
)

// 执行树的 frame 进度即 executionProgress；子调用进度经 childExecution.Frame
// 递归，分支任务进度经 taskProgress 挂在所属 frame 上。根记录 Execution 只
// 保存根 frame——树内任何 frame/任务/日志变更都编码整棵树后写回。

// taskStatus 是分支任务的进度状态。
type taskStatus string

const (
	taskStatusRunning   taskStatus = "running"
	taskStatusSuspended taskStatus = "suspended"
	taskStatusCompleted taskStatus = "completed"
	taskStatusFailed    taskStatus = "failed"
)

// taskPhase 是分支任务内部的可恢复阶段：只有到达边界的阶段产出才允许保存。
type taskPhase string

const (
	taskPhaseEntry taskPhase = "entry" // 已领取，尚未跨过任何边界
	taskPhasePost  taskPhase = "post"  // PostHandler 已把输出落入局部 State
)

// waitKind 是当前阻塞位置的类型。
type waitKind string

const (
	// waitQuestion 表示阻塞在本层日志中的某个问题上；ID 指向该问题。
	waitQuestion waitKind = "question"
	// waitChild 表示阻塞在一次子调用上；TaskID+Seq 指向本 frame 的子记录。
	waitChild waitKind = "child"
	// waitGroup 表示阻塞在并行任务组上；Tasks 是挂起时仍在等待的任务集合。
	waitGroup waitKind = "group"
	// waitManaged 表示阻塞在本 frame 登记的受管子任务上；Tasks 是等待的
	// 受管任务 ID 集合，全部到达终态后等待解除。
	waitManaged waitKind = "managed"
)

// waitRecord 描述当前阻塞位置及关联对象。交互日志保存历史问题与答案，
// 等待记录说明"现在停在哪里"——恢复判定以它为准：旧答案只回答当时
// 的问题，不能唤醒后来阻塞在子调用上的任务。字段留在根 Execution 内，
// 不增加公开恢复入口。
type waitRecord struct {
	Kind   waitKind `json:"kind"`
	ID     string   `json:"id,omitempty"`      // kind=question：本层日志中的问题 ID
	TaskID string   `json:"task_id,omitempty"` // kind=child：子调用所属父任务 ID
	Seq    int      `json:"seq,omitempty"`     // kind=child：任务内调用序号
	Call   string   `json:"call,omitempty"`    // kind=child：发起子调用的调用身份
	Tasks  []string `json:"tasks,omitempty"`   // kind=group：挂起时仍在等待的任务 ID；kind=managed：等待的受管任务 ID
	Tree   bool     `json:"tree,omitempty"`    // kind=managed：根 End 收敛的全树等待——引用可跨 frame 定位到后代任务
}

func (w *waitRecord) Clone() *waitRecord {
	if w == nil {
		return nil
	}
	out := *w
	out.Tasks = append([]string(nil), w.Tasks...)
	return &out
}

// taskProgress 保存一个分支任务（并行 fan-out 分支或原生 step 任务）的进度。
// Entry 是任务领取时的局部 State 快照；Local 是任务最新产出的局部 State；
// Output 是节点边输出值（原生 step 路由判断需要，pre/post 模式分支不需要）。
// Interrupts 是本任务的交互日志；恢复时按日志重放已保存回答。
// Waiting 是挂起时的当前阻塞位置，决定恢复时能否继续执行。
type taskProgress struct {
	TaskID     string                    `json:"id"`
	Node       string                    `json:"node"`
	Branch     string                    `json:"branch,omitempty"`
	GroupID    string                    `json:"group_id,omitempty"`
	Status     taskStatus                `json:"status"`
	Phase      taskPhase                 `json:"phase,omitempty"`
	Input      json.RawMessage           `json:"input,omitempty"`
	Entry      *GraphStateCheckpoint     `json:"entry,omitempty"`
	Local      *GraphStateCheckpoint     `json:"local,omitempty"`
	Output     json.RawMessage           `json:"output,omitempty"`
	Interrupts []control.InterruptRecord `json:"interrupts,omitempty"`
	Waiting    *waitRecord               `json:"waiting,omitempty"`
	// Pending 登记任务内已完成的工具调用结果（键为调用去重键），随任务
	// 记录落盘；恢复重放时工具节点命中登记的调用直接复用结果。
	Pending map[string]json.RawMessage `json:"pending,omitempty"`
}

func (p *taskProgress) Clone() *taskProgress {
	if p == nil {
		return nil
	}
	out := *p
	if p.Input != nil {
		out.Input = append(json.RawMessage(nil), p.Input...)
	}
	if p.Output != nil {
		out.Output = append(json.RawMessage(nil), p.Output...)
	}
	if p.Entry != nil {
		entry := p.Entry.Clone()
		out.Entry = &entry
	}
	if p.Local != nil {
		local := p.Local.Clone()
		out.Local = &local
	}
	out.Interrupts = append([]control.InterruptRecord(nil), p.Interrupts...)
	for i := range out.Interrupts {
		out.Interrupts[i] = out.Interrupts[i].Clone()
	}
	out.Waiting = p.Waiting.Clone()
	out.Pending = clonePendingCalls(p.Pending)
	return &out
}

func cloneTaskProgress(tasks []*taskProgress) []*taskProgress {
	if len(tasks) == 0 {
		return nil
	}
	out := make([]*taskProgress, len(tasks))
	for i, task := range tasks {
		out[i] = task.Clone()
	}
	return out
}

func validateTaskProgress(tasks []*taskProgress) error {
	seen := make(map[string]bool, len(tasks))
	for i, task := range tasks {
		if task == nil {
			return fmt.Errorf("%w: tasks[%d] is nil", state.ErrInvalidCheckpoint, i)
		}
		if strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(task.Node) == "" {
			return fmt.Errorf("%w: tasks[%d] requires id and node", state.ErrInvalidCheckpoint, i)
		}
		if seen[task.TaskID] {
			return fmt.Errorf("%w: duplicate task id %q", state.ErrInvalidCheckpoint, task.TaskID)
		}
		seen[task.TaskID] = true
		switch task.Status {
		case taskStatusRunning, taskStatusSuspended, taskStatusCompleted, taskStatusFailed:
		default:
			return fmt.Errorf("%w: tasks[%d] has invalid status %q", state.ErrInvalidCheckpoint, i, task.Status)
		}
		switch task.Phase {
		case "", taskPhaseEntry, taskPhasePost:
		default:
			return fmt.Errorf("%w: tasks[%d] has invalid phase %q", state.ErrInvalidCheckpoint, i, task.Phase)
		}
		if err := validateTaskJournal(task.Interrupts); err != nil {
			return fmt.Errorf("%w: tasks[%d] journal: %v", state.ErrInvalidCheckpoint, i, err)
		}
		if task.Status == taskStatusCompleted && task.Local == nil {
			return fmt.Errorf("%w: tasks[%d] completed without local state", state.ErrInvalidCheckpoint, i)
		}
		if err := validateTaskData("entry", task.Entry, i); err != nil {
			return err
		}
		if err := validateTaskData("local", task.Local, i); err != nil {
			return err
		}
	}
	return nil
}

// validateTaskJournal 校验分支任务的交互日志：记录合法、ID 唯一、序号即
// 日志位置、未回答问题只允许出现在末尾。
func validateTaskJournal(records []control.InterruptRecord) error {
	seen := make(map[string]bool, len(records))
	for i, r := range records {
		if err := r.Validate(); err != nil {
			return err
		}
		if seen[r.ID] || r.Index != i || (!r.Answered && i != len(records)-1) {
			return errors.New("inconsistent interrupt journal")
		}
		seen[r.ID] = true
	}
	return nil
}

func validateTaskData(field string, checkpoint *GraphStateCheckpoint, index int) error {
	if checkpoint == nil {
		return nil
	}
	if checkpoint.CodecID == "" {
		return fmt.Errorf("%w: tasks[%d].%s requires codec metadata", state.ErrInvalidCheckpoint, index, field)
	}
	return nil
}

// childStatus 是一次子调用（嵌套图节点或子 Agent）的执行状态。
type childStatus string

const (
	childStatusRunning   childStatus = "running"
	childStatusSuspended childStatus = "suspended"
	childStatusCompleted childStatus = "completed"
	childStatusFailed    childStatus = "failed"
)

// childExecution 保存父任务发起的一次子调用的身份与进度。身份由父任务 ID
// 加任务内调用序号构成；Input 用于重放校验——同一任务重放时必须以相同
// 顺序、相同输入调用子执行。Result/Error 保存已完成的输出或可恢复的
// 业务失败；Frame 是子执行自己的进度树（含它的子调用）。
type childExecution struct {
	TaskID string `json:"task_id"`
	Seq    int    `json:"seq"`
	// Call 是发起该子调用的调用身份（会话内去重键）；为空表示节点级
	// 调用。序号 Seq 按调用身份分派，被结果复用跳过的调用不打乱序号。
	Call     string             `json:"call,omitempty"`
	Status   childStatus        `json:"status"`
	Graph    string             `json:"graph,omitempty"`
	Revision string             `json:"revision,omitempty"`
	Input    json.RawMessage    `json:"input,omitempty"`
	Result   json.RawMessage    `json:"result,omitempty"`
	Error    string             `json:"error,omitempty"`
	Frame    *executionProgress `json:"frame,omitempty"`
}

// childKey 生成子调用在父 frame 内的稳定身份：父任务、工具调用归属与
// 该调用内的序号共同构成唯一键——同批不同工具调用各自的子调用不冲突。
func childKey(taskID, callKey string, seq int) string {
	if callKey == "" {
		return taskID + "#" + strconv.Itoa(seq)
	}
	return taskID + "@" + callKey + "#" + strconv.Itoa(seq)
}

func (c *childExecution) Clone() *childExecution {
	if c == nil {
		return nil
	}
	out := *c
	if c.Input != nil {
		out.Input = append(json.RawMessage(nil), c.Input...)
	}
	if c.Result != nil {
		out.Result = append(json.RawMessage(nil), c.Result...)
	}
	if c.Frame != nil {
		frame := c.Frame.Clone()
		out.Frame = &frame
	}
	return &out
}

func cloneChildExecutions(children []*childExecution) []*childExecution {
	if len(children) == 0 {
		return nil
	}
	out := make([]*childExecution, len(children))
	for i, child := range children {
		out[i] = child.Clone()
	}
	return out
}

const maxExecutionDepth = 64

// validateChildExecutions 浅校验一层子调用记录；frame 树的递归校验由
// validateExecutionTree 在根解码后统一执行，保证深度受限。
func validateChildExecutions(children []*childExecution) error {
	seen := make(map[string]bool, len(children))
	for i, child := range children {
		if child == nil {
			return fmt.Errorf("%w: children[%d] is nil", state.ErrInvalidCheckpoint, i)
		}
		if strings.TrimSpace(child.TaskID) == "" || child.Seq < 0 {
			return fmt.Errorf("%w: children[%d] requires task id and non-negative seq", state.ErrInvalidCheckpoint, i)
		}
		key := childKey(child.TaskID, child.Call, child.Seq)
		if seen[key] {
			return fmt.Errorf("%w: duplicate child %q", state.ErrInvalidCheckpoint, key)
		}
		seen[key] = true
		switch child.Status {
		case childStatusRunning, childStatusSuspended, childStatusCompleted, childStatusFailed:
		default:
			return fmt.Errorf("%w: children[%d] has invalid status %q", state.ErrInvalidCheckpoint, i, child.Status)
		}
		if len(child.Input) > 0 && !json.Valid(child.Input) {
			return fmt.Errorf("%w: children[%d] input is not valid JSON", state.ErrInvalidCheckpoint, i)
		}
		if len(child.Result) > 0 && !json.Valid(child.Result) {
			return fmt.Errorf("%w: children[%d] result is not valid JSON", state.ErrInvalidCheckpoint, i)
		}
		// 挂起的子执行必然提交过 frame 进度（挂起路径先写 frame 再传播信号）。
		if child.Status == childStatusSuspended && (child.Frame == nil || child.Frame.GraphName == "") {
			return fmt.Errorf("%w: children[%d] suspended without frame progress", state.ErrInvalidCheckpoint, i)
		}
		if child.Status == childStatusCompleted && len(child.Result) == 0 {
			return fmt.Errorf("%w: children[%d] completed without result", state.ErrInvalidCheckpoint, i)
		}
	}
	return nil
}

// validateExecutionTree 深度优先校验整棵 frame 树：每个 frame 自身的字段
// 校验叠加深度限制，拒绝病态嵌套导致的无限递归。
func validateExecutionTree(root *executionProgress, depth int) error {
	if depth > maxExecutionDepth {
		return fmt.Errorf("%w: execution tree exceeds depth %d", state.ErrInvalidCheckpoint, maxExecutionDepth)
	}
	if root == nil {
		return errors.New("execution tree root is missing")
	}
	if err := root.Validate(); err != nil {
		return err
	}
	if err := validateWaitRef(root, root.Waiting, root.Interrupts, "frame"); err != nil {
		return err
	}
	for _, task := range root.Tasks {
		if task == nil {
			continue
		}
		if err := validateWaitRef(root, task.Waiting, task.Interrupts, "task "+task.TaskID); err != nil {
			return err
		}
		// 旧格式没有等待记录：挂起任务必须能从自身日志或子调用推断阻塞
		// 位置；两者都没有说明关联缺失，明确拒绝而不是清空现场重跑。
		if task.Status == taskStatusSuspended && task.Waiting == nil && len(task.Interrupts) == 0 {
			hasChild := false
			for _, child := range root.Children {
				if child != nil && child.TaskID == task.TaskID {
					hasChild = true
					break
				}
			}
			if !hasChild {
				return fmt.Errorf("%w: suspended task %q has no journal or child calls to infer its waiting position", ErrResumeIncompatible, task.TaskID)
			}
		}
	}
	for _, child := range root.Children {
		// 已登记但尚未提交过进度的子调用槽位为空 frame，跳过校验；
		// 首个进度提交后 GraphName 等字段才齐备。
		if child.Frame == nil || child.Frame.GraphName == "" {
			continue
		}
		if err := validateExecutionTree(child.Frame, depth+1); err != nil {
			return err
		}
	}
	for _, m := range root.Managed {
		if m == nil || m.Frame == nil || m.Frame.GraphName == "" {
			continue
		}
		if err := validateExecutionTree(m.Frame, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// validateWaitRef 校验等待记录的引用完整性：问题 ID 必须存在于所属日志，
// 子调用引用必须指向本 frame 中存在的记录，任务组引用必须指向存在的任务。
func validateWaitRef(frame *executionProgress, w *waitRecord, journal []control.InterruptRecord, owner string) error {
	if w == nil {
		return nil
	}
	switch w.Kind {
	case waitQuestion:
		if strings.TrimSpace(w.ID) == "" {
			return fmt.Errorf("%w: %s waiting record has empty question id", state.ErrInvalidCheckpoint, owner)
		}
		for _, record := range journal {
			if record.ID == w.ID {
				return nil
			}
		}
		return fmt.Errorf("%w: %s waiting record references missing question %q", state.ErrInvalidCheckpoint, owner, w.ID)
	case waitChild:
		if strings.TrimSpace(w.TaskID) == "" || w.Seq < 0 {
			return fmt.Errorf("%w: %s waiting record has invalid child reference", state.ErrInvalidCheckpoint, owner)
		}
		if frame.findChild(w.TaskID, w.Call, w.Seq) == nil {
			return fmt.Errorf("%w: %s waiting record references missing child %s", state.ErrInvalidCheckpoint, owner, childKey(w.TaskID, w.Call, w.Seq))
		}
		return nil
	case waitGroup:
		if len(w.Tasks) == 0 {
			return fmt.Errorf("%w: %s waiting record has empty task set", state.ErrInvalidCheckpoint, owner)
		}
		for _, id := range w.Tasks {
			if frame.findTask(id) == nil {
				return fmt.Errorf("%w: %s waiting record references missing task %q", state.ErrInvalidCheckpoint, owner, id)
			}
		}
		return nil
	case waitManaged:
		if len(w.Tasks) == 0 {
			return fmt.Errorf("%w: %s waiting record has empty managed set", state.ErrInvalidCheckpoint, owner)
		}
		for _, id := range w.Tasks {
			var m *managedTask
			if w.Tree {
				// 根收敛等待覆盖整棵树：后代任务登记在各自 frame 中。
				m = findManagedInTree(frame, id)
			} else {
				m = frame.findManaged(id)
			}
			if m == nil {
				return fmt.Errorf("%w: %s waiting record references missing managed task %q", state.ErrInvalidCheckpoint, owner, id)
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: %s waiting record has invalid kind %q", state.ErrInvalidCheckpoint, owner, w.Kind)
	}
}

// findTask 返回本 frame 中指定任务的进度记录。
func (p *executionProgress) findTask(taskID string) *taskProgress {
	for _, task := range p.Tasks {
		if task.TaskID == taskID {
			return task
		}
	}
	return nil
}

// findChild 返回本 frame 中指定父任务、调用身份与序号的子调用记录。
func (p *executionProgress) findChild(taskID, callKey string, seq int) *childExecution {
	for _, child := range p.Children {
		if child.TaskID == taskID && child.Call == callKey && child.Seq == seq {
			return child
		}
	}
	return nil
}

// suspendSignal 表示子执行进入挂起：携带该子树所有未回答问题，实现
// control.Suspension，由父级调度器继续向根传播；根记录保存成功后根运行
// 才把 InterruptedError 返回给用户。blocked 是发起者与其直接子调用的
// 关联——各层传播时换成本层的等待位置（finishChildRun 重写为本层子调用）。
type suspendSignal struct {
	pending []control.InterruptRecord
	blocked *waitRecord
}

func (s *suspendSignal) Error() string { return "nested execution suspended" }

func (s *suspendSignal) PendingInterrupts() []control.InterruptRecord {
	out := make([]control.InterruptRecord, len(s.pending))
	for i, record := range s.pending {
		out[i] = record.Clone()
	}
	return out
}

var _ control.Suspension = (*suspendSignal)(nil)

// suspendReasonNested 是子执行挂起向父级传播时填入的暂停理由；
// 真正的未回答问题在各子 frame 的日志中，根返回携带全树 pending。
const suspendReasonNested = "waiting for nested execution input"

// callSite 挂在任务执行 ctx 上，描述组件发起子调用时使用的父现场：
// 父 frame 指针、父任务 ID 和任务内子调用序号。重放同一任务时序号从 0
// 重新递增，相同顺序的调用命中相同子记录。序号按调用身份分派：节点级
// 调用（callKey 为空）共享一条序号空间；工具调用各自独立，被结果复用
// 跳过的调用不影响其余调用的序号。
type callSite struct {
	coord   *runCoordinator
	frame   *executionProgress
	taskID  string
	step    int
	callSeq map[string]int
	// owner 是本任务所属受管运行的任务 ID；根运行的任务为空。受管任务
	// ID 携带完整 owner 路径，保证跨层级全局唯一。
	owner string
	// managedSeq 是受管任务创建调用的序号空间：与子调用序号分开，
	// 两类调用的重放顺序互不影响。
	managedSeq map[string]int
	// managedMsgSeq 是受管消息发送的序号空间：重放同一位点不再重复
	// 追加收件箱。
	managedMsgSeq map[string]int
}

// nextSeq 分配指定调用身份下的下一个子调用序号。
func (s *callSite) nextSeq(callKey string) int {
	if s.callSeq == nil {
		s.callSeq = make(map[string]int)
	}
	seq := s.callSeq[callKey]
	s.callSeq[callKey]++
	return seq
}

// nextManagedSeq 分配指定调用身份下的下一个受管创建序号。
func (s *callSite) nextManagedSeq(callKey string) int {
	if s.managedSeq == nil {
		s.managedSeq = make(map[string]int)
	}
	seq := s.managedSeq[callKey]
	s.managedSeq[callKey]++
	return seq
}

// nextManagedMsgSeq 分配指定调用身份下的下一个受管消息序号。
func (s *callSite) nextManagedMsgSeq(callKey string) int {
	if s.managedMsgSeq == nil {
		s.managedMsgSeq = make(map[string]int)
	}
	seq := s.managedMsgSeq[callKey]
	s.managedMsgSeq[callKey]++
	return seq
}

type callSiteKey struct{}

func withCallSite(ctx context.Context, site *callSite) context.Context {
	if site == nil {
		return ctx
	}
	return context.WithValue(ctx, callSiteKey{}, site)
}

func callSiteFromContext(ctx context.Context) *callSite {
	site, _ := ctx.Value(callSiteKey{}).(*callSite)
	return site
}

// ErrResumeIncompatible 表示重放中的调用顺序或输入与原记录不匹配，
// 或根恢复输入与保存的挂起现场不一致。
var ErrResumeIncompatible = control.Sentinel("checkpoint resume is incompatible with saved progress")

// isRecoveryError 识别恢复校验类错误：恢复现场与当前代码不兼容、检查点
// 损坏或保存失败。它们是框架错误而非业务结果——不登记为子执行的可重放
// 失败，原样向上传播，由恢复方显式处理。
func isRecoveryError(err error) bool {
	var checkpoint *CheckpointError
	if errors.As(err, &checkpoint) {
		return true
	}
	return errors.Is(err, ErrResumeIncompatible) ||
		errors.Is(err, ErrGraphRevisionMismatch) ||
		errors.Is(err, ErrInterruptMismatch) ||
		errors.Is(err, ErrInterruptUnsupported) ||
		errors.Is(err, ErrCheckpointRequired) ||
		errors.Is(err, state.ErrInvalidCheckpoint)
}

// runCoordinator 是执行树的唯一持久化协调点：持有根 frame 进度、根记录指针
// 与唯一的 checkpoint writer。所有 frame/任务/日志写入在树锁下编码整棵树；
// 登记子调用、任务完成、挂起等边界经 writer 同步确认。业务代码与 Store 调用
// 不在树锁内执行。
type runCoordinator struct {
	mu      sync.Mutex
	writer  *checkpointWriter
	record  *recordView
	timeout time.Duration
	root    *executionProgress
	// runCtx 是根运行的执行上下文：受管任务协程的生命周期随它取消，
	// 不会逃逸出本运行。
	runCtx context.Context
	// handles 是各受管任务的运行期驱动句柄（不落盘）；builders 是恢复
	// 受管任务所需的构建函数注册表；slots 为受管协程并发额度，容量取
	// 根运行的并发上限；fatalErrs 保存本运行内各任务的原始框架错误
	// （完整错误链），跨次恢复由任务记录的 Fatal/Error 字段兜底重建。
	handles   map[*managedTask]*managedHandle
	builders  map[string]managedBuilderEntry
	slots     chan struct{}
	fatalErrs map[*managedTask]error
	// storeless 标记无检查点的内存受管运行：树变更照常并入内存现场，
	// 只是没有落盘队列；审批类可恢复挂起在该模式下按
	// ErrCheckpointRequired 拒绝。
	storeless bool
	// agents 是会话级子 Agent 集合（树锁保护）：运行入口从会话记录载入，
	// 每次写入随执行树一同落盘；agentsData 缓存其编码，集合变更时失效。
	agents     []*sessionAgent
	agentsData json.RawMessage
}

// encodeLocked 在树锁内把根 frame 编码为 Execution 载荷；根 frame 标记
// 当前格式版本，嵌套 frame 的 Format 字段不参与判断。
func (c *runCoordinator) encodeLocked() ([]byte, error) {
	root := c.root
	if root == nil {
		root = &executionProgress{}
	}
	// 快照后再打版本标记：受管协程会并发编码/变更树内记录，原地修改
	// 根 frame 的 Format 会与主调度的字段读取产生数据竞争。
	snapshot := root.Clone()
	snapshot.Format = executionFormatTree
	return snapshot.JSON()
}

// publishLocked 在树锁内编码整棵树并登记写入，返回完成票据。入队发生在
// 持锁期间——写入队列的顺序与树变更的顺序严格一致，后到写入不会把旧
// 内容覆盖在兄弟结果上。调用方持有树锁；等待票据须在树锁外进行。
func (c *runCoordinator) publishLocked(ctx context.Context, edit func(record *state.Checkpoint, data []byte)) (writeTicket, error) {
	if c.storeless {
		// 内存模式没有落盘队列：树变更已就地提交，返回即时完成票据。
		return resolvedTicket(), nil
	}
	data, err := c.encodeLocked()
	if err != nil {
		return writeTicket{}, err
	}
	// 会话集合与执行树在同一持锁内取快照、同一次写入落盘：任务结算与
	// 子 Agent 状态不会出现一方已保存另一方未保存的中间态。
	agents, err := c.encodeAgentsLocked()
	if err != nil {
		return writeTicket{}, err
	}
	return c.writer.enqueue(ctx, func(r *state.Checkpoint) {
		edit(r, data)
		if agents != nil {
			r.SubAgents = agents
		}
	}), nil
}

// commitFrame 在树锁内把新进度合并进本 frame 槽位（保留运行期登记的子调用
// 与任务记录）、执行收尾变更、编码整棵树并登记写入，返回完成票据。
// finalize 在 frame 替换后、编码前执行：任务清理与子记录回收必须与本次
// 进度写进入同一份快照，不能产生被并发保存出去的中间态。
func (c *runCoordinator) commitFrame(ctx context.Context, frame *executionProgress, next executionProgress, finalize func(*executionProgress), edit func(*state.Checkpoint, []byte)) (writeTicket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	next.Children = frame.Children
	next.Tasks = frame.Tasks
	next.Managed = frame.Managed
	*frame = next
	if finalize != nil {
		finalize(frame)
	}
	return c.publishLocked(ctx, edit)
}

// mutateTree 在树锁内执行变更、编码并登记写入，返回完成票据。
// 变更与入队在同一持锁内完成，保证快照内容与提交顺序一致。
func (c *runCoordinator) mutateTree(ctx context.Context, mutate func(), edit func(*state.Checkpoint, []byte)) (writeTicket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if mutate != nil {
		mutate()
	}
	return c.publishLocked(ctx, edit)
}

// updateFrame 仅就地更新本 frame（嵌套挂起路径）：保存由根调度器收敛后
// 统一执行，嵌套 frame 不直接落盘。
func (c *runCoordinator) updateFrame(frame *executionProgress, next executionProgress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	next.Children = frame.Children
	next.Tasks = frame.Tasks
	next.Managed = frame.Managed
	*frame = next
}

// findTask 读取本 frame 中指定任务的进度记录；返回克隆，读方不持有树锁。
func (c *runCoordinator) findTask(frame *executionProgress, taskID string) *taskProgress {
	c.mu.Lock()
	defer c.mu.Unlock()
	return frame.findTask(taskID).Clone()
}

// upsertTaskLocked 领取或更新本 frame 中一个任务的进度记录；调用方须持有
// 树锁或操作独占数据。任务记录按值替换——调用方应在新对象上写完字段
// 再提交，避免与正在编码的写入竞争。
func upsertTaskLocked(frame *executionProgress, task *taskProgress) {
	for i, existing := range frame.Tasks {
		if existing.TaskID == task.TaskID {
			frame.Tasks[i] = task
			return
		}
	}
	frame.Tasks = append(frame.Tasks, task)
}

func (c *runCoordinator) upsertTask(frame *executionProgress, task *taskProgress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	upsertTaskLocked(frame, task)
}

// clearTasksLocked 在本 frame 中移除已被本轮合并消费的任务记录；
// 挂起待答与尚未消费的任务保留，供下一次恢复继续。须持有树锁。
func clearTasksLocked(frame *executionProgress, consumed map[string]bool) {
	kept := frame.Tasks[:0]
	for _, task := range frame.Tasks {
		if !consumed[task.TaskID] {
			kept = append(kept, task)
		}
	}
	frame.Tasks = kept
}

// releaseTasksByPrefixLocked 移除固定并行分支任务记录（键为 父任务ID/分支名）。
// 须持有树锁。
func releaseTasksByPrefixLocked(frame *executionProgress, prefix string) {
	kept := frame.Tasks[:0]
	for _, task := range frame.Tasks {
		if !strings.HasPrefix(task.TaskID, prefix) {
			kept = append(kept, task)
		}
	}
	frame.Tasks = kept
}

// acquireChild 登记或复用一次子调用：按 (父任务, 序号) 定位记录。新调用
// 在树锁内登记、编码并排队后同步落盘再返回——崩溃后恢复能重放到同一
// 调用点；已存在记录校验输入一致后由调用方按状态分流。
func (c *runCoordinator) acquireChild(ctx context.Context, site *callSite, callKey string, encoded json.RawMessage) (*childExecution, bool, error) {
	if site == nil || site.frame == nil || site.taskID == "" {
		return nil, false, errors.New("child call requires a task context")
	}
	// 登记、编码与入队在同一持锁内：登记快照严格按变更顺序落盘；
	// 命中已存在记录时不产生冗余写入。序号按调用身份分派：被结果复用
	// 跳过的调用不消耗序号，其余调用的登记位置不受其影响。
	c.mu.Lock()
	seq := site.nextSeq(callKey)
	child := site.frame.findChild(site.taskID, callKey, seq)
	if child == nil && callKey != "" {
		// 旧格式记录没有调用归属：按 (任务, 序号) 兜底认领并补记归属，
		// 使升级前的挂起子调用仍能恢复。
		if legacy := site.frame.findChild(site.taskID, "", seq); legacy != nil {
			legacy.Call = callKey
			child = legacy
		}
	}
	if child != nil {
		c.mu.Unlock()
		if !inputBytesMatch(child.Input, encoded) {
			return nil, false, fmt.Errorf("%w: child call %s input changed between replays", ErrResumeIncompatible, childKey(child.TaskID, child.Call, child.Seq))
		}
		return child, true, nil
	}
	child = &childExecution{
		TaskID: site.taskID,
		Call:   callKey,
		Seq:    seq,
		Status: childStatusRunning,
		Input:  append(json.RawMessage(nil), encoded...),
		Frame:  &executionProgress{},
	}
	site.frame.Children = append(site.frame.Children, child)
	ticket, err := c.publishLocked(ctx, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	c.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	// 登记先落盘：崩溃后恢复能重放到同一调用点。
	if err := ticket.wait(); err != nil {
		return nil, false, err
	}
	return child, false, nil
}

// completeChild 保存子执行成功结果；状态更新、编码与入队在同一树锁内完成，
// 写入按登记顺序落盘，保证任务重放时输出可直接复用。
func (c *runCoordinator) completeChild(ctx context.Context, child *childExecution, result json.RawMessage) error {
	ticket, err := c.mutateTree(ctx, func() {
		child.Status = childStatusCompleted
		child.Result = append(json.RawMessage(nil), result...)
		child.Error = ""
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err != nil {
		return err
	}
	return ticket.wait()
}

// failChild 保存可恢复的业务失败结果：父任务重放时返回相同错误。
func (c *runCoordinator) failChild(ctx context.Context, child *childExecution, err error) error {
	ticket, werr := c.mutateTree(ctx, func() {
		child.Status = childStatusFailed
		child.Error = err.Error()
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if werr != nil {
		return werr
	}
	return ticket.wait()
}

// suspendChild 仅更新子调用状态（frame 内容已由其自身挂起路径写入槽位）；
// 根保存由调度器在收敛后统一执行。
func (c *runCoordinator) suspendChild(child *childExecution) {
	c.mu.Lock()
	child.Status = childStatusSuspended
	c.mu.Unlock()
}

// retryChild 把失败子执行重新置为可运行：父任务重放到该调用点时从其最近
// 保存的 frame 边界继续；旧错误保留在 Error 字段供诊断。崩溃后重入仍按
// 原 failed 记录再次重试，语义一致。
func (c *runCoordinator) retryChild(child *childExecution) {
	c.mu.Lock()
	child.Status = childStatusRunning
	c.mu.Unlock()
}

// claimChildIdentity 登记或校验子调用绑定的子图身份：首次执行写入
// Graph/Revision；重放时图名必须一致，版本一致性由调用方按子执行状态
// 校验——带子 frame 的恢复走条目化契约闭包比较，无 frame 的记录仍要求
// 记录的版本与当前一致。
func (c *runCoordinator) claimChildIdentity(child *childExecution, name, revision string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if child.Graph == "" && child.Revision == "" {
		child.Graph = name
		child.Revision = revision
		return nil
	}
	if child.Graph != name {
		return fmt.Errorf("%w: child call %s bound to graph %q/%q, now %q/%q",
			ErrResumeIncompatible, childKey(child.TaskID, child.Call, child.Seq), child.Graph, child.Revision, name, revision)
	}
	return nil
}

// adoptPreparedChild 发布子调用恢复时准备好的现场：迁移后的 State 快照、
// 新版本标记与契约在同一次树变更内落位并落盘，随后任何保存都同时携带
// 两者——再次恢复不会重复进入旧版迁移。
func (c *runCoordinator) adoptPreparedChild(ctx context.Context, child *childExecution, frame *executionProgress, revision string) error {
	ticket, err := c.mutateTree(ctx, func() {
		if frame != child.Frame {
			*child.Frame = *frame
		}
		child.Revision = revision
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err != nil {
		return err
	}
	return ticket.wait()
}

// frameRunnable 判断挂起子 frame 的子树是否获得了可推进的输入。
// 判定以 frame 的等待记录为准；旧检查点没有等待记录时按日志尾部、
// 挂起任务与子调用状态推断当前阻塞位置。
func (c *runCoordinator) frameRunnable(frame *executionProgress) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	runnable, _ := frameWaitResolved(frame)
	return runnable
}

// frameRunnable 是不持锁版本：调用方须持有协调器树锁或操作克隆数据。
func frameRunnable(f *executionProgress) bool {
	runnable, _ := frameWaitResolved(f)
	return runnable
}

// frameWaitResolved 判断 frame 当前阻塞位置是否解除，返回仍待答的问题。
// 已登记但从未提交进度的槽位视为可推进（继续原子执行）。
func frameWaitResolved(f *executionProgress) (bool, []control.InterruptRecord) {
	if f == nil || f.GraphName == "" {
		return true, nil
	}
	if f.Waiting != nil {
		return resolveWait(f, f.Waiting, f.Interrupts)
	}
	return inferFrameWait(f)
}

// inferFrameWait 为没有等待记录的旧检查点推断当前阻塞位置：日志尾部未答
// 表示阻塞在该问题上（其它工作在它之后不可达）；存在挂起任务按组判定；
// 否则按序找首个未完成子调用；都没有则说明没有阻塞点（如外部暂停）。
func inferFrameWait(f *executionProgress) (bool, []control.InterruptRecord) {
	if n := len(f.Interrupts); n > 0 && !f.Interrupts[n-1].Answered {
		return false, []control.InterruptRecord{f.Interrupts[n-1].Clone()}
	}
	for _, task := range f.Tasks {
		if task != nil && task.Status == taskStatusSuspended {
			return groupWaitResolved(f)
		}
	}
	for _, child := range f.Children {
		if child == nil {
			continue
		}
		switch child.Status {
		case childStatusCompleted:
			continue
		case childStatusRunning, childStatusFailed:
			return true, nil
		case childStatusSuspended:
			if frameRunnable(child.Frame) {
				return true, nil
			}
			return false, pendingInterruptsTree(child.Frame)
		}
	}
	return true, nil
}

// resolveWait 按等待记录判定阻塞是否解除：journal 是所属层的交互日志
// （frame 为自身日志，任务为任务日志）。
func resolveWait(frame *executionProgress, w *waitRecord, journal []control.InterruptRecord) (bool, []control.InterruptRecord) {
	switch w.Kind {
	case waitQuestion:
		for i := range journal {
			if journal[i].ID == w.ID {
				if journal[i].Answered {
					return true, nil
				}
				return false, []control.InterruptRecord{journal[i].Clone()}
			}
		}
		// 引用在载入时已校验存在；缺失按可推进处理（不会误停住）。
		return true, nil
	case waitChild:
		return childWaitResolved(frame, w.TaskID, w.Call, w.Seq)
	case waitGroup:
		return groupWaitResolved(frame)
	case waitManaged:
		return managedWaitResolved(frame, w.Tasks, w.Tree)
	}
	return true, nil
}

// childWaitResolved 判断阻塞在子调用上的等待是否解除：子执行完成、可继续
// 或可重试即为可推进；仍挂起时看其子树是否获得可推进输入。
func childWaitResolved(frame *executionProgress, taskID, callKey string, seq int) (bool, []control.InterruptRecord) {
	child := frame.findChild(taskID, callKey, seq)
	if child == nil {
		return true, nil
	}
	switch child.Status {
	case childStatusCompleted, childStatusRunning, childStatusFailed:
		return true, nil
	case childStatusSuspended:
		if frameRunnable(child.Frame) {
			return true, nil
		}
		return false, pendingInterruptsTree(child.Frame)
	}
	return true, nil
}

// groupWaitResolved 判断并行任务组是否可推进：任一任务可推进、存在尚未
// 领取的任务，或全部任务完成可合并时为 true；其余情况返回各阻塞任务
// 仍待答的问题。已完成任务只是可复用结果，本身不推动执行。
func groupWaitResolved(frame *executionProgress) (bool, []control.InterruptRecord) {
	var pending []control.InterruptRecord
	incomplete := false
	for _, task := range frame.Tasks {
		if task == nil || task.Status == taskStatusCompleted {
			continue
		}
		incomplete = true
		if task.Status != taskStatusSuspended {
			return true, nil
		}
		if ok, waiting := taskWaitResolved(frame, task); ok {
			return true, nil
		} else {
			pending = append(pending, waiting...)
		}
	}
	if !incomplete {
		// 全部任务完成：阻塞点在合并提交，允许继续推进。
		return true, nil
	}
	// 调度清单中尚未领取的任务仍属于可推进工作——它们可能因并发上限
	// 或此前失败尚未登记进 frame.Tasks。
	if frame.Schedule != nil {
		for _, st := range frame.Schedule.Tasks {
			if !scheduledTaskClaimed(frame, st.ID) {
				return true, nil
			}
		}
	}
	return false, pending
}

// scheduledTaskClaimed 判断调度任务是否已开始执行。原生任务按调度任务 ID
// 查找自己的记录；固定并行节点没有父任务级独立记录——它的分支记录以
// 「父任务 ID/分支名」登记，任一分支记录存在即视为该调度任务已启动。
// 是否可推进仍由这些分支记录自身的状态决定：有分支获得答案、可以重试
// 或尚未执行时允许推进，全部等待时保持挂起，全部完成时允许合并。
func scheduledTaskClaimed(frame *executionProgress, taskID string) bool {
	if frame.findTask(taskID) != nil {
		return true
	}
	prefix := taskID + "/"
	for _, task := range frame.Tasks {
		if task != nil && strings.HasPrefix(task.TaskID, prefix) {
			return true
		}
	}
	return false
}

// taskWaitResolved 判断挂起任务的当前阻塞是否解除：优先使用挂起时保存的
// 等待记录；旧格式按日志尾部未答、首个未完成子调用推断。父任务过去获得
// 的答案不能唤醒后来阻塞在子调用上的任务。
func taskWaitResolved(frame *executionProgress, task *taskProgress) (bool, []control.InterruptRecord) {
	if task == nil || task.Status != taskStatusSuspended {
		return true, nil
	}
	if task.Waiting != nil {
		return resolveWait(frame, task.Waiting, task.Interrupts)
	}
	if n := len(task.Interrupts); n > 0 && !task.Interrupts[n-1].Answered {
		return false, []control.InterruptRecord{task.Interrupts[n-1].Clone()}
	}
	// 尾部已答或无日志：按重放顺序看首个未完成的子调用。
	for _, child := range frame.Children {
		if child == nil || child.TaskID != task.TaskID {
			continue
		}
		switch child.Status {
		case childStatusCompleted:
			continue
		case childStatusRunning, childStatusFailed:
			return true, nil
		case childStatusSuspended:
			if frameRunnable(child.Frame) {
				return true, nil
			}
			return false, pendingInterruptsTree(child.Frame)
		}
	}
	return true, nil
}

// suspendedTaskState 在树锁内判断挂起任务是否可推进，返回其仍待答的问题。
// 判定以任务等待记录为准——只回答兄弟任务时，未获新输入的任务保持等待，
// 不重新执行 PreHandler 与节点代码。
func (c *runCoordinator) suspendedTaskState(frame *executionProgress, taskID string) (bool, []control.InterruptRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return taskWaitResolved(frame, frame.findTask(taskID))
}

// groupWaitRecord 收集本 frame 当前仍挂起的任务集合，作为并行任务组的
// 等待记录；组内各任务的阻塞细节保存在各自的任务记录中。
func groupWaitRecord(coord *runCoordinator, frame *executionProgress) *waitRecord {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	var tasks []string
	for _, task := range frame.Tasks {
		if task != nil && task.Status == taskStatusSuspended {
			tasks = append(tasks, task.TaskID)
		}
	}
	if len(tasks) == 0 {
		return nil
	}
	return &waitRecord{Kind: waitGroup, Tasks: tasks}
}

// pendingOf 返回以该 frame 为根的子树中全部未回答问题；树锁内克隆后遍历，
// 不与并发编码共享指针。
func (c *runCoordinator) pendingOf(frame *executionProgress) []control.InterruptRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	clone := frame.Clone()
	return pendingInterruptsTree(&clone)
}

// releaseChildrenLocked 在父任务提交后回收其临时子记录：提交后该任务不再
// 重放，子记录可以释放；回收与进度提交必须进入同一份根快照（经
// commitFrame 的 finalize）。固定并行节点的分支子调用以「任务 ID/分支名」
// 派生键，随父任务一并回收。须持有树锁。
func releaseChildrenLocked(frame *executionProgress, taskID string) {
	prefix := taskID + "/"
	kept := frame.Children[:0]
	for _, child := range frame.Children {
		if child.TaskID != taskID && !strings.HasPrefix(child.TaskID, prefix) {
			kept = append(kept, child)
		}
	}
	frame.Children = kept
}

// cloneJournal 复制一段交互日志。
func cloneJournal(records []control.InterruptRecord) []control.InterruptRecord {
	out := append([]control.InterruptRecord(nil), records...)
	for i := range out {
		out[i] = out[i].Clone()
	}
	return out
}

// suspendTask 把挂起任务的现场写入进度记录：入口 State、已接受日志、当前
// 阻塞位置与未消费序号供恢复重放；记录整体替换，避免与正在编码的写入
// 共享指针。cause 是本次挂起的原因：自身问题记为问题等待，子调用挂起
// 记为子调用等待；进入新的等待位置时整体替换旧记录。
func (g *Graph[I, O]) suspendTask(coord *runCoordinator, frame *executionProgress, existing *taskProgress, session *inputSession, node, branch, groupID string, cause error) {
	if coord == nil || session == nil || existing == nil {
		return
	}
	record := &taskProgress{
		TaskID: existing.TaskID, Node: node, Branch: branch, GroupID: groupID,
		Status: taskStatusSuspended, Phase: taskPhaseEntry,
	}
	if session.entry.CodecID != "" {
		entry := session.entry.Clone()
		record.Entry = &entry
	}
	session.mu.Lock()
	record.Interrupts = cloneJournal(session.records)
	record.Pending = clonePendingCalls(session.pending)
	session.mu.Unlock()
	record.Waiting = waitForCause(cause, session)
	coord.upsertTask(frame, record)
}

// waitForCause 从挂起原因推导任务的当前阻塞位置：自身问题取会话日志中
// 最后一个未答问题；子调用挂起取信号携带的阻塞子引用。无法确定时返回
// nil，恢复按旧格式推断。
func waitForCause(cause error, session *inputSession) *waitRecord {
	var input *inputInterrupt
	if errors.As(cause, &input) {
		return questionWait(session)
	}
	var managedWait *managedWaitSignal
	if errors.As(cause, &managedWait) {
		return &waitRecord{Kind: waitManaged, Tasks: append([]string(nil), managedWait.ids...), Tree: managedWait.tree}
	}
	var signal *suspendSignal
	if errors.As(cause, &signal) && signal.blocked != nil {
		return signal.blocked.Clone()
	}
	return nil
}

// questionWait 返回会话日志中最后一个未答问题的等待记录；全部已答时
// 返回 nil（恢复后重放会停在新的未答问题上）。
func questionWait(session *inputSession) *waitRecord {
	session.mu.Lock()
	defer session.mu.Unlock()
	return questionWaitFromRecords(session.records)
}

func questionWaitFromRecords(records []control.InterruptRecord) *waitRecord {
	for i := len(records) - 1; i >= 0; i-- {
		if !records[i].Answered {
			return &waitRecord{Kind: waitQuestion, ID: records[i].ID}
		}
	}
	return nil
}

// completeTask 保存分支任务完成结果：局部 State 与边输出编码进任务记录，
// 经根 writer 同步确认，供恢复时直接复用。
func (g *Graph[I, O]) completeTask(ctx context.Context, coord *runCoordinator, frame *executionProgress, record *taskProgress, local any, output any, outputType reflect.Type) error {
	if coord == nil || record == nil {
		return nil
	}
	localCheckpoint, err := g.snapshotRuntimeState(ctx, local)
	if err != nil {
		return fmt.Errorf("save task %q result: %w", record.TaskID, err)
	}
	completed := &taskProgress{
		TaskID: record.TaskID, Node: record.Node, Branch: record.Branch, GroupID: record.GroupID,
		Status: taskStatusCompleted, Phase: taskPhasePost, Local: &localCheckpoint,
		Interrupts: record.Interrupts, Entry: record.Entry,
	}
	if outputType != nil {
		encoded, encErr := encodeTaskOutput(output, outputType)
		if encErr != nil {
			return fmt.Errorf("save task %q output: %w", record.TaskID, encErr)
		}
		completed.Output = encoded
	}
	ticket, err := coord.mutateTree(ctx, func() {
		upsertTaskLocked(frame, completed)
		// 收件箱消费进度与任务提交同边界：本任务确认过的消息批次随
		// 完成记录一起提升为已消费，挂起重放仍会重新投递。
		coord.promoteInboxLocked(frame, record.TaskID)
	}, func(r *state.Checkpoint, data []byte) { r.Execution = data })
	if err != nil {
		return err
	}
	return ticket.wait()
}

// 树级遍历：pending 汇集根 frame 与各子 frame 的未回答问题；
// applyResumeInputTree 先整体验证再写入回答，保证不会只写一半。
func pendingInterruptsTree(root *executionProgress) []control.InterruptRecord {
	var pending []control.InterruptRecord
	var walk func(frame *executionProgress)
	walk = func(frame *executionProgress) {
		if frame == nil {
			return
		}
		pending = append(pending, pendingInterrupts(*frame)...)
		for _, task := range frame.Tasks {
			for _, record := range task.Interrupts {
				if !record.Answered {
					pending = append(pending, record.Clone())
				}
			}
		}
		for _, child := range frame.Children {
			walk(child.Frame)
		}
		for _, m := range frame.Managed {
			if m != nil {
				walk(m.Frame)
			}
		}
	}
	walk(root)
	return pending
}

// applyResumeInputTree 将一批回答写入树中对应任务的日志。先验证所有 ID
// 存在且未回答，再逐帧写入；允许分批回答——回答后仍有未答问题时返回
// 剩余 pending，由调用方决定立即挂起还是继续可运行任务。
func applyResumeInputTree(root *executionProgress, inputs []resumeInput) error {
	pending := pendingInterruptsTree(root)
	if len(inputs) > 0 && len(pending) == 0 {
		return fmt.Errorf("%w: run is not waiting for input", ErrInvalidResume)
	}
	// 不带输入不再报错：是否存在可推进任务由调度侧决定——有可运行的继续
	// 执行，全部仍等待则返回当前的 InterruptedError 与原问题 ID。
	byID := make(map[string]bool, len(pending))
	for _, record := range pending {
		byID[record.ID] = true
	}
	for _, input := range inputs {
		if !byID[input.id] {
			return fmt.Errorf("%w: no pending question %q", ErrInvalidResume, input.id)
		}
	}
	var walk func(frame *executionProgress)
	walk = func(frame *executionProgress) {
		if frame == nil {
			return
		}
		applyAnswerSet(frame.Interrupts, inputs)
		for _, task := range frame.Tasks {
			applyAnswerSet(task.Interrupts, inputs)
		}
		for _, child := range frame.Children {
			walk(child.Frame)
		}
		for _, m := range frame.Managed {
			if m != nil {
				walk(m.Frame)
			}
		}
	}
	walk(root)
	return nil
}

func applyAnswerSet(records []control.InterruptRecord, inputs []resumeInput) {
	for i := range records {
		record := &records[i]
		if record.Answered {
			continue
		}
		for _, input := range inputs {
			if record.ID == input.id {
				record.Answered = true
				record.Resume = append(json.RawMessage(nil), input.data...)
			}
		}
	}
}

// inputBytesMatch 比较两次编码的任务输入是否逐字节一致：任务输入由确定性
// JSON 编码产生（map 键序固定），字节比较即重放校验。
func inputBytesMatch(a, b json.RawMessage) bool {
	return bytes.Equal(a, b)
}
