package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// ManagedStatus 是受管子任务的生命周期状态。cancelled 是显式取消终态，
// 根 Resume 不重新启动；running 任务在根运行停止后保持原状，恢复时重新驱动。
type ManagedStatus string

const (
	ManagedStatusRunning   ManagedStatus = "running"
	ManagedStatusSuspended ManagedStatus = "suspended"
	ManagedStatusCompleted ManagedStatus = "completed"
	ManagedStatusFailed    ManagedStatus = "failed"
	ManagedStatusCancelled ManagedStatus = "cancelled"
)

func (s ManagedStatus) terminal() bool {
	return s == ManagedStatusCompleted || s == ManagedStatusFailed || s == ManagedStatusCancelled
}

// ManagedSpec 是受管子任务的最终创建配置：构建函数身份与版本、解析后的
// 组件配置和冻结输入。登记与恢复都以它为准——模型原始请求已在调用方
// 解析成该结构，恢复时不再重新应用默认值或让模型重新选择。
type ManagedSpec struct {
	Builder string          `json:"builder"`
	Version string          `json:"version,omitempty"`
	Config  json.RawMessage `json:"config,omitempty"`
	Input   json.RawMessage `json:"input,omitempty"`
}

// ManagedResult 是受管子任务的对外视图：状态、最终输出或业务错误。
type ManagedResult struct {
	TaskID string `json:"task_id"`
	// AgentID 非空表示该任务属于会话内子 Agent（见 SpawnAgent）。
	AgentID string          `json:"agent_id,omitempty"`
	Status  ManagedStatus   `json:"status"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// ManagedRunner 是按已解析配置构建出的受管执行器：输入为冻结的 JSON，
// 输出为可持久化的 JSON 结果。执行返回 control.Suspension 表示任务挂起。
type ManagedRunner func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

// ManagedBuilder 把解析后的任务描述装配成可执行实例。模型、工具等实际
// 组件由构建函数关联的资源配置提供；构建在任务登记之前完成，构建失败
// 的任务不会被登记。
type ManagedBuilder func(ctx context.Context, spec ManagedSpec) (ManagedRunner, error)

// ManagedMessage 是受管任务收件箱中的一条消息：稳定 ID + 冻结载荷，
// 消费游标记录在所属任务上。
type ManagedMessage struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

func (m ManagedMessage) Clone() ManagedMessage {
	out := m
	out.Payload = append(json.RawMessage(nil), m.Payload...)
	return out
}

// managedTask 是本 frame 登记的一个受管子任务。身份由创建任务、调用身份
// 与调用内序号构成——同一任务重放相同顺序的创建调用命中同一记录，不会
// 重复创建。Frame 是子执行自己的进度树，由根协调器统一落盘。
type managedTask struct {
	ID     string        `json:"id"`
	TaskID string        `json:"task_id"`
	Call   string        `json:"call,omitempty"`
	Seq    int           `json:"seq"`
	Spec   ManagedSpec   `json:"spec"`
	Status ManagedStatus `json:"status"`
	// Graph/Revision 是子执行绑定的图身份：首次驱动时登记，恢复时校验。
	Graph    string          `json:"graph,omitempty"`
	Revision string          `json:"revision,omitempty"`
	Result   json.RawMessage `json:"result,omitempty"`
	Error    string          `json:"error,omitempty"`
	Reason   string          `json:"reason,omitempty"` // 取消/停止原因
	// Fatal 标记框架级错误（持久化失败、恢复不兼容等）：任务保持
	// running 供恢复重试，错误经协调器向根传播——不折叠为业务失败。
	Fatal bool               `json:"fatal,omitempty"`
	Frame *executionProgress `json:"frame,omitempty"`
	Inbox []ManagedMessage   `json:"inbox,omitempty"`
	// Consumed 是收件箱消费游标：前 Consumed 条已被执行侧提交消费。
	// TaskInbox 保存各消费任务已确认但尚未提交的批次（任务提交边界
	// 才提升为 Consumed）——挂起重放会把未提交批次重新投递。
	Consumed  int                 `json:"consumed,omitempty"`
	TaskInbox map[string][]string `json:"task_inbox,omitempty"`
	// AgentID 非空表示本任务是会话内某个子 Agent 的一次派发；子 Agent
	// 的稳定身份、配置与上下文由会话级集合持有，任务只引用它。
	AgentID string `json:"agent_id,omitempty"`
	// AgentContext 是执行器在本任务内已导出的子 Agent 上下文，任务以
	// 完成状态结算时才提交进会话集合。它是任务恢复现场的一部分，随执行
	// 树落盘：审批挂起、重建实例后重新驱动时，已完成节点产生的导出结果
	// 不会丢失（这些节点不会重跑，也就不会再次导出）。失败与取消在结算
	// 时丢弃。
	AgentContext json.RawMessage `json:"agent_context,omitempty"`
}

func (m *managedTask) Clone() *managedTask {
	if m == nil {
		return nil
	}
	out := *m
	if m.AgentContext != nil {
		out.AgentContext = append(json.RawMessage(nil), m.AgentContext...)
	}
	if m.Spec.Config != nil {
		out.Spec.Config = append(json.RawMessage(nil), m.Spec.Config...)
	}
	if m.Spec.Input != nil {
		out.Spec.Input = append(json.RawMessage(nil), m.Spec.Input...)
	}
	if m.Result != nil {
		out.Result = append(json.RawMessage(nil), m.Result...)
	}
	if m.Frame != nil {
		frame := m.Frame.Clone()
		out.Frame = &frame
	}
	out.Inbox = append([]ManagedMessage(nil), m.Inbox...)
	for i := range out.Inbox {
		out.Inbox[i] = out.Inbox[i].Clone()
	}
	if m.TaskInbox != nil {
		out.TaskInbox = make(map[string][]string, len(m.TaskInbox))
		for taskID, ids := range m.TaskInbox {
			out.TaskInbox[taskID] = append([]string(nil), ids...)
		}
	}
	return &out
}

func cloneManagedTasks(tasks []*managedTask) []*managedTask {
	if len(tasks) == 0 {
		return nil
	}
	out := make([]*managedTask, len(tasks))
	for i, task := range tasks {
		out[i] = task.Clone()
	}
	return out
}

// managedID 生成受管任务的稳定身份：携带完整 owner 路径，不同层级、
// 不同父任务下的同序创建不会撞号，重放仍按同一路径命中同一记录。
func managedID(owner, taskID, callKey string, seq int) string {
	key := childKey(taskID, callKey, seq)
	if owner == "" {
		return "mt/" + key
	}
	return "mt/" + owner + "/" + key
}

// specEqual 校验重放登记的请求与保存的最终配置完全一致。
func specEqual(a, b ManagedSpec) bool {
	return a.Builder == b.Builder && a.Version == b.Version &&
		inputBytesMatch(a.Config, b.Config) && inputBytesMatch(a.Input, b.Input)
}

func (m *managedTask) result() ManagedResult {
	return ManagedResult{TaskID: m.ID, AgentID: m.AgentID, Status: m.Status, Result: m.Result, Error: m.Error}
}

// findManaged 按任务 ID 查找本 frame 的受管任务。
func (p *executionProgress) findManaged(id string) *managedTask {
	for _, m := range p.Managed {
		if m.ID == id {
			return m
		}
	}
	return nil
}

// findManagedInTree 按任务 ID 在 frame 子树中定位受管任务——根收敛的
// 等待记录引用可能指向任意深度的后代任务。
func findManagedInTree(frame *executionProgress, id string) *managedTask {
	var hit *managedTask
	walkManaged(frame, func(m *managedTask) {
		if hit == nil && m.ID == id {
			hit = m
		}
	})
	return hit
}

// appendUniqueInterrupts 按问题 ID 去重追加：多层任务遍历可能重复收集
// 同一问题记录。
func appendUniqueInterrupts(dst []control.InterruptRecord, seen map[string]bool, src []control.InterruptRecord) []control.InterruptRecord {
	for _, record := range src {
		if record.ID != "" && seen[record.ID] {
			continue
		}
		seen[record.ID] = true
		dst = append(dst, record)
	}
	return dst
}

// findManagedByOwner 按创建归属（父任务、调用身份、序号）查找受管任务。
func (p *executionProgress) findManagedByOwner(taskID, callKey string, seq int) *managedTask {
	for _, m := range p.Managed {
		if m.TaskID == taskID && m.Call == callKey && m.Seq == seq {
			return m
		}
	}
	return nil
}

// walkManaged 深度优先遍历整棵 frame 树中的受管任务。
func walkManaged(frame *executionProgress, fn func(*managedTask)) {
	if frame == nil {
		return
	}
	for _, m := range frame.Managed {
		if m == nil {
			continue
		}
		fn(m)
		walkManaged(m.Frame, fn)
	}
	for _, child := range frame.Children {
		if child == nil {
			continue
		}
		walkManaged(child.Frame, fn)
	}
}

func validateManagedTasks(tasks []*managedTask) error {
	seen := make(map[string]bool, len(tasks))
	for i, m := range tasks {
		if m == nil {
			return fmt.Errorf("%w: managed[%d] is nil", state.ErrInvalidCheckpoint, i)
		}
		if strings.TrimSpace(m.ID) == "" || strings.TrimSpace(m.TaskID) == "" || m.Seq < 0 {
			return fmt.Errorf("%w: managed[%d] requires id, owner task and non-negative seq", state.ErrInvalidCheckpoint, i)
		}
		if seen[m.ID] {
			return fmt.Errorf("%w: duplicate managed task %q", state.ErrInvalidCheckpoint, m.ID)
		}
		seen[m.ID] = true
		if strings.TrimSpace(m.Spec.Builder) == "" {
			return fmt.Errorf("%w: managed[%d] requires builder identity", state.ErrInvalidCheckpoint, i)
		}
		switch m.Status {
		case ManagedStatusRunning, ManagedStatusSuspended, ManagedStatusCompleted, ManagedStatusFailed, ManagedStatusCancelled:
		default:
			return fmt.Errorf("%w: managed[%d] has invalid status %q", state.ErrInvalidCheckpoint, i, m.Status)
		}
		for _, field := range []struct {
			name string
			data json.RawMessage
		}{{"config", m.Spec.Config}, {"input", m.Spec.Input}, {"result", m.Result}, {"agent context", m.AgentContext}} {
			if len(field.data) > 0 && !json.Valid(field.data) {
				return fmt.Errorf("%w: managed[%d] %s is not valid JSON", state.ErrInvalidCheckpoint, i, field.name)
			}
		}
		if m.Consumed < 0 || m.Consumed > len(m.Inbox) {
			return fmt.Errorf("%w: managed[%d] consumed cursor out of range", state.ErrInvalidCheckpoint, i)
		}
		msgIDs := make(map[string]bool, len(m.Inbox))
		for j, msg := range m.Inbox {
			if strings.TrimSpace(msg.ID) == "" || msgIDs[msg.ID] || !json.Valid(msg.Payload) {
				return fmt.Errorf("%w: managed[%d] inbox[%d] invalid", state.ErrInvalidCheckpoint, i, j)
			}
			msgIDs[msg.ID] = true
		}
		// 批次键为消费任务 ID；空键是裸 runner（非图任务）的批次，合法。
		for _, ids := range m.TaskInbox {
			for _, id := range ids {
				if !msgIDs[id] {
					return fmt.Errorf("%w: managed[%d] pending ack %q not in inbox", state.ErrInvalidCheckpoint, i, id)
				}
			}
		}
	}
	return nil
}

// inboxView 组装任务视角的未消费消息：本任务已确认未提交的批次重放在前，
// 其后是其余未确认新消息。持有树锁期间调用。
func (m *managedTask) inboxView(taskID string) []ManagedMessage {
	pendingIDs := m.TaskInbox[taskID]
	pendingSet := make(map[string]bool, len(pendingIDs))
	for _, id := range pendingIDs {
		pendingSet[id] = true
	}
	var out []ManagedMessage
	for _, id := range pendingIDs {
		for _, msg := range m.Inbox {
			if msg.ID == id {
				out = append(out, msg)
				break
			}
		}
	}
	for _, msg := range m.Inbox[m.Consumed:] {
		if !pendingSet[msg.ID] {
			out = append(out, msg)
		}
	}
	return out
}

// managedSite 挂在受管执行 goroutine 的 ctx 上：持有根协调器与本次任务的
// 记录槽位。子执行经它把进度写入整棵树，收件箱读取也经它定位记录。
type managedSite struct {
	coord   *runCoordinator
	managed *managedTask
}

type managedSiteKey struct{}

func withManagedSite(ctx context.Context, site *managedSite) context.Context {
	if site == nil {
		return ctx
	}
	return context.WithValue(ctx, managedSiteKey{}, site)
}

func managedSiteFromContext(ctx context.Context) *managedSite {
	site, _ := ctx.Value(managedSiteKey{}).(*managedSite)
	return site
}

// managedHandle 是运行期（不落盘）的任务驱动句柄：取消函数与完成信号。
type managedHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// managedBuilderEntry 是注册进运行配置的构建函数及其版本。
type managedBuilderEntry struct {
	version string
	build   ManagedBuilder
}

// managedWaitSignal 由 WaitManaged 返回：本调用者等待的受管任务尚未全部
// 结束。它实现 control.Suspension——任务调度按挂起处理并保存等待记录，
// 任务槽位随之释放，不占用并发额度空等。
type managedWaitSignal struct {
	ids     []string
	pending []control.InterruptRecord
	// tree 标记根 End 收敛产生的全树等待：等待记录允许跨 frame 引用
	// 后代任务；WaitManaged 的直属等待不带此标记。
	tree bool
}

func (s *managedWaitSignal) Error() string {
	return "waiting for managed tasks: " + strings.Join(s.ids, ",")
}

func (s *managedWaitSignal) PendingInterrupts() []control.InterruptRecord {
	out := make([]control.InterruptRecord, len(s.pending))
	for i, record := range s.pending {
		out[i] = record.Clone()
	}
	return out
}

var _ control.Suspension = (*managedWaitSignal)(nil)

var (
	// ErrManagedUnavailable 表示当前调用上下文没有受管任务运行环境：
	// 受管 API 只能由运行中的任务（节点处理函数）调用。
	ErrManagedUnavailable = control.Sentinel("managed tasks require a running task context")
	// ErrManagedNotFound 表示目标任务不在本调用者可见的执行树中。
	ErrManagedNotFound = control.Sentinel("managed task not found")
	// ErrManagedScope 表示目标不是本调用者直接创建的任务；等待、消息与
	// 取消都限定为直属子任务，从结构上排除互相等待成环。
	ErrManagedScope = control.Sentinel("managed task is not a direct child of this caller")
	// ErrManagedTerminal 表示目标任务已结束，不再接受消息或取消。
	ErrManagedTerminal = control.Sentinel("managed task already reached a terminal state")
	// ErrManagedBuilder 表示规范引用的构建函数未注册或版本不符。
	ErrManagedBuilder = control.Sentinel("managed task builder is unknown or version mismatch")
)

// WithManagedBuilder 注册受管任务的构建函数。恢复受管任务前必须完成
// 相同身份与版本的绑定——记录中的 BuilderID/Version 与注册项不符时
// 恢复被拒绝，不能悄悄换一份实现继续运行。
func WithManagedBuilder(id, version string, builder ManagedBuilder) runnable.Option {
	return runnable.WrapOption(func(c *graphRunConfig) {
		if c.managedBuilders == nil {
			c.managedBuilders = map[string]managedBuilderEntry{}
		}
		c.managedBuilders[id] = managedBuilderEntry{version: version, build: builder}
	})
}

// peekManaged 按 (父任务, 调用身份, 序号) 查询受管创建调用是否已登记：
// 序号在树锁内消耗，命中既有记录时校验规范一致后直接复用——重放不再
// 重新构建执行器。未命中返回已分配的序号供 registerManaged 登记。
func (c *runCoordinator) peekManaged(site *callSite, callKey string, spec ManagedSpec) (*managedTask, int, error) {
	if site == nil || site.frame == nil || site.taskID == "" {
		return nil, 0, errors.New("managed spawn requires a task context")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	seq := site.nextManagedSeq(callKey)
	if existing := site.frame.findManagedByOwner(site.taskID, callKey, seq); existing != nil {
		if !specEqual(existing.Spec, spec) {
			return nil, 0, fmt.Errorf("%w: managed spawn %s spec changed between replays", ErrResumeIncompatible, existing.ID)
		}
		return existing, seq, nil
	}
	return nil, seq, nil
}

// registerManaged 把构建完成的新任务登记进调用方 frame：确认保存后才向
// 调用方返回任务 ID 并启动执行；树锁内再次查重，并发同位点调用不会
// 重复登记。
func (c *runCoordinator) registerManaged(ctx context.Context, site *callSite, callKey string, seq int, spec ManagedSpec) (*managedTask, bool, error) {
	return c.registerManagedWith(ctx, site, callKey, seq, spec, nil)
}

// registerManagedWith 是 registerManaged 的通用形态：bind 在树锁内、记录
// 入树前执行，子 Agent 的创建与追加任务借它把任务身份与会话集合的变更
// 并入同一次登记；bind 返回错误则整次登记放弃，树与集合都不改变。
func (c *runCoordinator) registerManagedWith(ctx context.Context, site *callSite, callKey string, seq int, spec ManagedSpec, bind func(*managedTask) error) (*managedTask, bool, error) {
	c.mu.Lock()
	if existing := site.frame.findManagedByOwner(site.taskID, callKey, seq); existing != nil {
		c.mu.Unlock()
		if !specEqual(existing.Spec, spec) {
			return nil, false, fmt.Errorf("%w: managed spawn %s spec changed between replays", ErrResumeIncompatible, existing.ID)
		}
		return existing, true, nil
	}
	// 登记即冻结规范：输入与配置拷贝进记录，调用方持有的切片此后
	// 再修改也不会影响执行中的任务。
	frozen := spec
	frozen.Config = append(json.RawMessage(nil), spec.Config...)
	frozen.Input = append(json.RawMessage(nil), spec.Input...)
	m := &managedTask{
		ID:     managedID(site.owner, site.taskID, callKey, seq),
		TaskID: site.taskID,
		Call:   callKey,
		Seq:    seq,
		Spec:   frozen,
		Status: ManagedStatusRunning,
		Frame:  &executionProgress{},
	}
	if bind != nil {
		if err := bind(m); err != nil {
			c.mu.Unlock()
			return nil, false, err
		}
	}
	site.frame.Managed = append(site.frame.Managed, m)
	ticket, err := c.publishLocked(ctx, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	c.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	if err := ticket.wait(); err != nil {
		return nil, false, err
	}
	return m, false, nil
}

// launchManaged 启动受管任务的驱动协程：协程属于根协调器而非某个节点，
// 创建任务的父节点结束后子任务继续存在。并发额度沿用根运行上限。
func (c *runCoordinator) launchManaged(ctx context.Context, m *managedTask, runner ManagedRunner) {
	handle := &managedHandle{done: make(chan struct{})}
	mctx, cancel := context.WithCancel(ctx)
	handle.cancel = cancel
	c.mu.Lock()
	if c.handles == nil {
		c.handles = map[*managedTask]*managedHandle{}
	}
	c.handles[m] = handle
	c.mu.Unlock()
	go func() {
		defer cancel()
		defer close(handle.done)
		defer func() {
			c.mu.Lock()
			delete(c.handles, m)
			c.mu.Unlock()
		}()
		// 驱动协程为 runner 领取共享额度：叶子 runner 的整段执行按一个
		// 工作单元计；runner 内部跑子图时由 run 让出令牌、图内节点各自
		// 领取——额度不被持有着包围整图。被根运行停止（ctx 取消）时不改
		// 状态——已提交进度保留，恢复时重新驱动。
		lease, leaseErr := acquireSlotLease(mctx, c.slots)
		if leaseErr != nil {
			return
		}
		mctx = withSlotLease(mctx, lease)
		defer lease.release()
		out, runErr := runner(withManagedSite(mctx, &managedSite{coord: c, managed: m}), m.Spec.Input)
		if runErr == nil {
			if err := c.completeManaged(mctx, m, out); err != nil {
				c.markManagedFatal(mctx, m, err)
			}
			return
		}
		var suspended control.Suspension
		switch {
		case errors.As(runErr, &suspended):
			if c.storeless {
				// 内存运行没有跨调用恢复能力：只有未答问题的挂起按
				// ErrCheckpointRequired 上报；纯受管等待可在运行内重放推进。
				c.mu.Lock()
				hasPending := m.Frame != nil && len(pendingInterruptsTree(m.Frame)) > 0
				c.mu.Unlock()
				if hasPending {
					c.markManagedFatal(mctx, m, ErrCheckpointRequired)
					return
				}
			}
			// 子执行内部挂起：frame 进度已由其挂起路径写入槽位，此处只
			// 更新任务状态并统一落盘。
			if err := c.suspendManaged(mctx, m); err != nil {
				c.markManagedFatal(mctx, m, err)
			}
		case errors.Is(mctx.Err(), context.Canceled) || errors.Is(mctx.Err(), context.DeadlineExceeded):
			// 根运行停止或显式取消打断执行：不做业务态变更；显式取消的
			// 状态在 cancelManaged 中已先落盘。
		case isRecoveryError(runErr):
			// 框架错误不是业务失败：任务保持 running 供修正后重试，
			// 错误随 Fatal 标记持久化并经协调器向根传播。
			c.markManagedFatal(mctx, m, runErr)
		default:
			if err := c.failManaged(mctx, m, runErr); err != nil {
				c.markManagedFatal(mctx, m, err)
			}
		}
	}()
}

// managedTaskRestartable 判定受管任务是否可立即重驱：running 残迹
// （驱动协程已随上次运行退出）或等待已解除的挂起任务。恢复入口的
// 分批推进判定与 resumeManaged 的重启选择共用本谓词，口径一致。
func managedTaskRestartable(m *managedTask) bool {
	switch m.Status {
	case ManagedStatusRunning:
		return true
	case ManagedStatusSuspended:
		return frameRunnable(m.Frame)
	}
	return false
}

// managedRestartable 判断执行树中是否还有可推进的受管任务——含已结束
// 子任务名下的孙任务。根 frame 自身阻塞在全树收敛等待上时，已获答案
// 的子任务仍可由它先行恢复。
func managedRestartable(root *executionProgress) bool {
	restartable := false
	walkManaged(root, func(m *managedTask) {
		if !restartable && managedTaskRestartable(m) {
			restartable = true
		}
	})
	return restartable
}

// resumeManaged 在恢复现场后重新驱动整棵树中未终态的受管任务：running
// 与"等待已解除"的挂起任务重建执行器；仍在等待输入的任务保持挂起。
func (c *runCoordinator) resumeManaged(ctx context.Context, root *executionProgress) error {
	var relaunch []*managedTask
	c.mu.Lock()
	walkManaged(root, func(m *managedTask) {
		if managedTaskRestartable(m) {
			relaunch = append(relaunch, m)
		}
	})
	c.mu.Unlock()
	for _, m := range relaunch {
		runner, err := c.buildManaged(ctx, m.Spec)
		if err != nil {
			return err
		}
		c.launchManaged(ctx, m, runner)
	}
	return nil
}

// buildManaged 按登记的构建身份与版本解析执行器；缺失或不符即恢复不兼容。
func (c *runCoordinator) buildManaged(ctx context.Context, spec ManagedSpec) (ManagedRunner, error) {
	c.mu.Lock()
	entry, ok := c.builders[spec.Builder]
	c.mu.Unlock()
	if !ok || entry.build == nil || entry.version != spec.Version {
		return nil, fmt.Errorf("%w: builder %q version %q", ErrManagedBuilder, spec.Builder, spec.Version)
	}
	runner, err := entry.build(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("build managed task: %w", err)
	}
	if runner == nil {
		return nil, fmt.Errorf("%w: builder %q returned nil runner", ErrManagedBuilder, spec.Builder)
	}
	return runner, nil
}

// managedActive 判断是否存在仍在推进的受管任务协程。
func (c *runCoordinator) managedActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.handles) > 0
}

// managedFrameworkErrorLocked 返回首个确定的受管框架错误：Fatal 标记
// 随任务记录持久化（跨次恢复可见）；仍有驱动协程的任务视为重试进行中，
// 暂不判为失败。调用方须持有树锁。
func (c *runCoordinator) managedFrameworkErrorLocked() error {
	var ferr error
	walkManaged(c.root, func(m *managedTask) {
		if ferr == nil && m.Fatal && !m.Status.terminal() && c.handles[m] == nil {
			if original := c.fatalErrs[m]; original != nil {
				ferr = original
			} else {
				ferr = fmt.Errorf("managed task %s failed at framework level: %s", m.ID, m.Error)
			}
		}
	})
	return ferr
}

// managedFrameworkError 是 managedFrameworkErrorLocked 的取锁版本。
func (c *runCoordinator) managedFrameworkError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.managedFrameworkErrorLocked()
}

// managedUnfinished 判断树上是否还有未到达终态的受管任务——到达 End 的
// 根运行必须等它们收敛后才能写完成状态。
func (c *runCoordinator) managedUnfinished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	unfinished := false
	walkManaged(c.root, func(m *managedTask) {
		if !m.Status.terminal() {
			unfinished = true
		}
	})
	return unfinished
}

// markManagedFatal 记录框架级错误：Fatal 标记与错误文本随任务记录持久化，
// 任务保持 running 供恢复重试；框架错误经树扫描向根收敛路径上报。
func (c *runCoordinator) markManagedFatal(ctx context.Context, m *managedTask, cause error) {
	ticket, err := c.mutateTree(ctx, func() {
		if m.Status.terminal() {
			return
		}
		m.Fatal = true
		m.Error = cause.Error()
		if c.fatalErrs == nil {
			c.fatalErrs = map[*managedTask]error{}
		}
		c.fatalErrs[m] = cause
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err == nil {
		_ = ticket.wait()
	}
}

// promoteInboxLocked 把任务已确认的收件箱批次提升为已消费游标，与任务
// 提交处于同一树变更内。frame 属于哪个受管任务按指针归属查找；批次内
// 消息 ID 按收件箱位置推进游标，重叠批次不重复计入。须持有树锁。
func (c *runCoordinator) promoteInboxLocked(frame *executionProgress, taskID string) {
	var owner *managedTask
	walkManaged(c.root, func(m *managedTask) {
		if m.Frame == frame {
			owner = m
		}
	})
	if owner == nil || len(owner.TaskInbox[taskID]) == 0 {
		return
	}
	for _, id := range owner.TaskInbox[taskID] {
		for idx, msg := range owner.Inbox {
			if msg.ID == id && idx >= owner.Consumed {
				owner.Consumed = idx + 1
			}
		}
	}
	delete(owner.TaskInbox, taskID)
}

// completeRun 在受管任务全部收敛后写根运行的最终完成记录：状态、结果
// 与编码后的执行树随同一版本提交；内存模式只更新内存现场。
func (c *runCoordinator) completeRun(ctx context.Context, result json.RawMessage) error {
	ticket, err := c.mutateTree(ctx, nil, func(r *state.Checkpoint, data []byte) {
		r.Execution = data
		r.Status = state.StatusCompleted
		r.Result = append(json.RawMessage(nil), result...)
	})
	if err != nil {
		return err
	}
	return ticket.wait()
}

// managedPendingWork 判定受管等待型挂起是否还有推进可能：活跃协程、
// 已可推进的挂起任务、或失去协程的 running 残迹（内部重入时重新驱动）。
// 全部非终态任务都卡在不可推进挂起时返回 false——重入恢复只会原地
// 再次挂起，挂起结果应交还调用方。
func (c *runCoordinator) managedPendingWork() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.handles) > 0 {
		return true
	}
	// 全部终态时重入一次即可让等待经重放收敛为结果；running 残迹与
	// 可推进挂起由重入重新驱动。只有非终态任务全部卡在不可推进挂起
	// 时才确认无推进可能——重入只会原地再次挂起。
	any := false
	deadlocked := true
	walkManaged(c.root, func(m *managedTask) {
		any = true
		switch m.Status {
		case ManagedStatusRunning, ManagedStatusSuspended:
			if m.Status == ManagedStatusRunning || frameRunnable(m.Frame) {
				deadlocked = false
			}
		default:
			deadlocked = false
		}
	})
	return any && !deadlocked
}

// awaitManagedIdle 等待全部受管驱动协程退出（完成、失败、挂起或被停止）。
// 根运行挂起、结束或内部重入恢复前调用，避免协程逃逸出本运行的生命周期。
func (c *runCoordinator) awaitManagedIdle(ctx context.Context) error {
	for {
		c.mu.Lock()
		var dones []chan struct{}
		for _, handle := range c.handles {
			dones = append(dones, handle.done)
		}
		c.mu.Unlock()
		if len(dones) == 0 {
			return nil
		}
		for _, done := range dones {
			select {
			case <-done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// stopManaged 收敛未结束协程：取消上下文并等待退出。不修改任务状态——
// running 任务保持可恢复现场，显式取消已由 cancelManaged 落盘。
func (c *runCoordinator) stopManaged() {
	c.mu.Lock()
	var handles []*managedHandle
	for _, h := range c.handles {
		handles = append(handles, h)
	}
	c.mu.Unlock()
	for _, h := range handles {
		h.cancel()
	}
	for _, h := range handles {
		<-h.done
	}
}

// drainManaged 是根图到达结束点时的收敛：等待受管任务全部落位，返回
// 仍挂起任务的 ID 集合与其子树中的未回答问题；框架错误优先于挂起向
// 上传播。
func (c *runCoordinator) drainManaged(ctx context.Context) ([]string, []control.InterruptRecord, error) {
	if err := c.awaitManagedIdle(ctx); err != nil {
		return nil, nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ferr := c.managedFrameworkErrorLocked(); ferr != nil {
		return nil, nil, ferr
	}
	var ids []string
	var pending []control.InterruptRecord
	seen := map[string]bool{}
	walkManaged(c.root, func(m *managedTask) {
		// 未终态任务都在等待集合内：失去协程的 running 任务经内部重入
		// 恢复重新驱动；挂起任务贡献其子树的未回答问题。
		if m.Status.terminal() {
			return
		}
		ids = append(ids, m.ID)
		if m.Status == ManagedStatusSuspended {
			pending = appendUniqueInterrupts(pending, seen, pendingInterruptsTree(m.Frame))
		}
	})
	return ids, pending, nil
}

// managedSlots 构造受管协程的并发额度：上限取根运行的 maxConcurrency，
// 未设置（0）时不限制——与批内任务同一量级口径。
func managedSlots(limit int) chan struct{} {
	if limit <= 0 {
		return nil
	}
	return make(chan struct{}, limit)
}

// slotLease 是一个实际工作单元持有的并发额度令牌。额度按工作领取、按
// 工作结束归还；工作单元进入嵌套图运行时先让出（yield），由图内部的
// 节点各自向同一额度池领取——包装层不持额度包围整图，上限 1 时不会
// 形成「外层持额度等内层、内层等外层释放」的等待环。运行结束后 resume
// 取回，外层工作单元恢复记账。
type slotLease struct {
	slots chan struct{}
	mu    sync.Mutex
	held  bool
}

type slotLeaseKey struct{}

func withSlotLease(ctx context.Context, lease *slotLease) context.Context {
	return context.WithValue(ctx, slotLeaseKey{}, lease)
}

func slotLeaseFromContext(ctx context.Context) *slotLease {
	lease, _ := ctx.Value(slotLeaseKey{}).(*slotLease)
	return lease
}

// acquireSlotLease 领取一个额度令牌；slots 为 nil（不限并发）时返回空
// 令牌。等待额度期间响应 ctx 取消。
func acquireSlotLease(ctx context.Context, slots chan struct{}) (*slotLease, error) {
	lease := &slotLease{slots: slots}
	if slots == nil {
		return lease, nil
	}
	select {
	case slots <- struct{}{}:
		lease.held = true
		return lease, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// yield 把持有的令牌归还额度池；重复调用安全。
func (l *slotLease) yield() {
	if l == nil || l.slots == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held {
		<-l.slots
		l.held = false
	}
}

// resume 在嵌套运行结束后取回令牌：额度被其他工作占用时等待其释放或
// ctx 取消。
func (l *slotLease) resume(ctx context.Context) {
	if l == nil || l.slots == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held {
		return
	}
	select {
	case l.slots <- struct{}{}:
		l.held = true
	case <-ctx.Done():
	}
}

// release 归还令牌并结束本工作单元的额度记账；已让出的令牌幂等返回。
func (l *slotLease) release() {
	l.yield()
}

// sharedSlots 返回本轮批任务的并发额度：协调器存在且根运行设了上限时
// 共享同一额度池（受管协程也从该池取额度，父子合计不超上限）；没有
// 协调器但祖先工作单元持有额度时沿用其额度池（未登记嵌套图同样受根
// 上限约束）；否则按本图上限独立建池。
func sharedSlots(ctx context.Context, coord *runCoordinator, limit int) chan struct{} {
	if coord != nil && coord.slots != nil {
		return coord.slots
	}
	if lease := slotLeaseFromContext(ctx); lease != nil && lease.slots != nil {
		return lease.slots
	}
	return managedSlots(limit)
}

func (c *runCoordinator) completeManaged(ctx context.Context, m *managedTask, result json.RawMessage) error {
	ticket, err := c.mutateTree(ctx, func() {
		if m.Status.terminal() || m.Status == ManagedStatusCancelled {
			return
		}
		m.Status = ManagedStatusCompleted
		m.Result = append(json.RawMessage(nil), result...)
		m.Error = ""
		m.Fatal = false
		delete(c.fatalErrs, m)
		// 运行结束：全部待提交批次提升为已消费——未提交的确认随任务
		// 完成边界一次提交，此后不再重放。
		for taskID := range m.TaskInbox {
			for _, id := range m.TaskInbox[taskID] {
				for idx, msg := range m.Inbox {
					if msg.ID == id && idx >= m.Consumed {
						m.Consumed = idx + 1
					}
				}
			}
		}
		m.TaskInbox = nil
		c.settleAgentTaskLocked(m)
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err != nil {
		return err
	}
	return ticket.wait()
}

func (c *runCoordinator) failManaged(ctx context.Context, m *managedTask, cause error) error {
	ticket, err := c.mutateTree(ctx, func() {
		if m.Status.terminal() || m.Status == ManagedStatusCancelled {
			return
		}
		m.Status = ManagedStatusFailed
		m.Error = cause.Error()
		c.settleAgentTaskLocked(m)
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err != nil {
		return err
	}
	return ticket.wait()
}

func (c *runCoordinator) suspendManaged(ctx context.Context, m *managedTask) error {
	ticket, err := c.mutateTree(ctx, func() {
		if m.Status == ManagedStatusRunning {
			m.Status = ManagedStatusSuspended
		}
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err != nil {
		return err
	}
	return ticket.wait()
}

// cancelManagedTree 级联取消任务及全部未结束后代：标记终态、记录原因、
// 取消各自驱动协程。须持有树锁；落盘由调用方统一执行。
func (c *runCoordinator) cancelManagedTree(m *managedTask, reason string) {
	if !m.Status.terminal() {
		m.Status = ManagedStatusCancelled
		m.Reason = reason
		c.settleAgentTaskLocked(m)
	}
	if h := c.handles[m]; h != nil {
		h.cancel()
	}
	walkManaged(m.Frame, func(descendant *managedTask) {
		if !descendant.Status.terminal() {
			descendant.Status = ManagedStatusCancelled
			descendant.Reason = reason
			c.settleAgentTaskLocked(descendant)
		}
		if h := c.handles[descendant]; h != nil {
			h.cancel()
		}
	})
}

// claimManagedIdentity 登记或校验受管任务绑定的子图身份，语义与
// claimChildIdentity 一致：图名必须一致，版本一致性由调用方按契约比较。
func (c *runCoordinator) claimManagedIdentity(m *managedTask, name, revision string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m.Graph == "" && m.Revision == "" {
		m.Graph = name
		m.Revision = revision
		return nil
	}
	if m.Graph != name {
		return fmt.Errorf("%w: managed task %s bound to graph %q/%q, now %q/%q",
			ErrResumeIncompatible, m.ID, m.Graph, m.Revision, name, revision)
	}
	return nil
}

// adoptPreparedManaged 发布受管任务恢复时准备好的现场：迁移后的 State
// 快照与新版本标记在同一次树变更内落位并落盘。
func (c *runCoordinator) adoptPreparedManaged(ctx context.Context, m *managedTask, frame *executionProgress, revision string) error {
	ticket, err := c.mutateTree(ctx, func() {
		if frame != m.Frame {
			*m.Frame = *frame
		}
		m.Revision = revision
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err != nil {
		return err
	}
	return ticket.wait()
}

// managedWaitResolved 判定受管等待是否解除：全部目标到达终态即可推进；
// 否则收集仍挂起目标子树中的未回答问题作为挂起原因。tree 标记时按根
// 收敛语义跨 frame 查找目标——引用与 validateWaitRef 使用同一范围。
func managedWaitResolved(frame *executionProgress, ids []string, tree bool) (bool, []control.InterruptRecord) {
	var pending []control.InterruptRecord
	seen := map[string]bool{}
	allTerminal := true
	for _, id := range ids {
		var m *managedTask
		if tree {
			m = findManagedInTree(frame, id)
		} else {
			m = frame.findManaged(id)
		}
		if m == nil || m.Status.terminal() {
			continue
		}
		allTerminal = false
		// running 任务仍在推进，无待答问题但保持等待；挂起任务贡献其
		// 子树的未回答问题，供根挂起结果对外呈现。
		if m.Status == ManagedStatusSuspended {
			pending = appendUniqueInterrupts(pending, seen, pendingInterruptsTree(m.Frame))
		}
	}
	if allTerminal {
		return true, nil
	}
	return false, pending
}

// --- 引擎级受管任务 API：供任务内代码与工具层调用。 ---

// normalizeManagedSpec 校验创建配置并补全冻结输入：构建函数身份必填，
// 配置与输入必须是合法 JSON，缺省输入记为 null。
func normalizeManagedSpec(spec ManagedSpec) (ManagedSpec, error) {
	if strings.TrimSpace(spec.Builder) == "" {
		return spec, fmt.Errorf("%w: empty builder identity", ErrManagedBuilder)
	}
	if len(spec.Config) > 0 && !json.Valid(spec.Config) {
		return spec, fmt.Errorf("managed spec config is not valid JSON")
	}
	if len(spec.Input) == 0 {
		spec.Input = json.RawMessage("null")
	} else if !json.Valid(spec.Input) {
		return spec, fmt.Errorf("managed spec input is not valid JSON")
	}
	return spec, nil
}

// SpawnManaged 创建并启动一个受管子任务。任务 ID 由根运行、创建任务身份
// 与调用内序号稳定关联：重放同一次创建调用复用原任务，相同内容的新请求
// 创建新任务。登记在确认保存后才返回。
func SpawnManaged(ctx context.Context, spec ManagedSpec) (string, error) {
	site := callSiteFromContext(ctx)
	if site == nil || site.coord == nil {
		return "", ErrManagedUnavailable
	}
	spec, err := normalizeManagedSpec(spec)
	if err != nil {
		return "", err
	}
	callKey, _ := control.CallKeyFromContext(ctx)
	existing, seq, err := site.coord.peekManaged(site, callKey, spec)
	if err != nil {
		return "", err
	}
	if existing != nil {
		if existing.AgentID != "" {
			return "", fmt.Errorf("%w: managed spawn %s was registered as an agent task", ErrResumeIncompatible, existing.ID)
		}
		return existing.ID, nil
	}
	// 命中既有记录即复用，不重新构建；新登记的任务才构建执行器，
	// 构建失败的任务不进入任务清单。
	runner, err := site.coord.buildManaged(ctx, spec)
	if err != nil {
		return "", err
	}
	m, exists, err := site.coord.registerManaged(ctx, site, callKey, seq, spec)
	if err != nil {
		return "", err
	}
	if !exists {
		site.coord.launchManaged(site.coord.runCtx, m, runner)
	}
	return m.ID, nil
}

// resolveManagedTargets 校验等待/通信的目标集合：任务必须存在且为本调用
// 者直接创建。自等待、等待祖先或兄弟等越权访问在登记前被拒绝。
// resolveManagedTargets 校验等待/通信的目标集合：任务必须登记在本调用者
// 所属的 frame 中（同一次父执行的任意任务，可跨节点重复等待）。受管任务
// 的内部 frame 看不到兄弟、祖先与自身记录，结构上排除互相等待成环；
// 目标存在于树中但不在本 frame 时按越权拒绝。
func resolveManagedTargets(site *callSite, ids []string) ([]*managedTask, error) {
	if len(ids) == 0 {
		return nil, errors.New("managed task id list is empty")
	}
	targets := make([]*managedTask, 0, len(ids))
	for _, id := range ids {
		m := site.frame.findManaged(id)
		if m == nil {
			var elsewhere *managedTask
			walkManaged(site.coord.root, func(candidate *managedTask) {
				if candidate.ID == id {
					elsewhere = candidate
				}
			})
			if elsewhere != nil {
				return nil, fmt.Errorf("%w: %q", ErrManagedScope, id)
			}
			return nil, fmt.Errorf("%w: %q", ErrManagedNotFound, id)
		}
		targets = append(targets, m)
	}
	return targets, nil
}

// WaitManaged 等待本调用者直接创建的受管任务全部结束，按请求顺序返回
// 结果；已完成任务立即复用结果。未全部结束时返回 managedWaitSignal——
// 调用任务按挂起处理并释放执行槽位，恢复后重新求值。
func WaitManaged(ctx context.Context, taskIDs []string) ([]ManagedResult, error) {
	site := callSiteFromContext(ctx)
	if site == nil || site.coord == nil {
		return nil, ErrManagedUnavailable
	}
	coord := site.coord
	coord.mu.Lock()
	targets, err := resolveManagedTargets(site, taskIDs)
	if err != nil {
		coord.mu.Unlock()
		return nil, err
	}
	var results []ManagedResult
	var pending []control.InterruptRecord
	for _, m := range targets {
		if m.Fatal && !m.Status.terminal() && coord.handles[m] == nil {
			// 框架错误不作为业务结果返回：优先本运行记录的原始错误
			// （完整错误链），跨次恢复按持久化标记重建文本。
			ferr := fmt.Errorf("managed task %s failed at framework level: %s", m.ID, m.Error)
			if original := coord.fatalErrs[m]; original != nil {
				ferr = original
			}
			coord.mu.Unlock()
			return nil, ferr
		}
		if m.Status.terminal() {
			results = append(results, m.result())
			continue
		}
		if m.Status == ManagedStatusSuspended && m.Frame != nil {
			clone := m.Frame.Clone()
			pending = append(pending, pendingInterruptsTree(&clone)...)
		}
	}
	coord.mu.Unlock()
	if len(results) == len(targets) {
		return results, nil
	}
	return nil, &managedWaitSignal{ids: append([]string(nil), taskIDs...), pending: pending}
}

// SendManagedMessage 向未结束的直属受管任务追加一条消息。登记在确认
// 保存后才返回消息 ID；已结束任务返回明确状态，不因消息自动重跑。
func SendManagedMessage(ctx context.Context, taskID string, payload any) (string, error) {
	site := callSiteFromContext(ctx)
	if site == nil || site.coord == nil {
		return "", ErrManagedUnavailable
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("managed message is not JSON serializable: %w", err)
	}
	coord := site.coord
	coord.mu.Lock()
	targets, err := resolveManagedTargets(site, []string{taskID})
	if err != nil {
		coord.mu.Unlock()
		return "", err
	}
	m := targets[0]
	callKey, _ := control.CallKeyFromContext(ctx)
	seq := site.nextManagedMsgSeq(callKey + "/" + m.ID)
	msgID := fmt.Sprintf("msg/%s", childKey(site.taskID, callKey, seq))
	// 重放同一位点的发送命中已登记消息：幂等返回原 ID，不再重复追加；
	// 载荷不同说明同一调用位点换用了新载荷，按恢复不兼容拒绝。
	for _, existing := range m.Inbox {
		if existing.ID == msgID {
			coord.mu.Unlock()
			if !inputBytesMatch(existing.Payload, data) {
				return "", fmt.Errorf("%w: managed message %s payload changed between replays", ErrResumeIncompatible, msgID)
			}
			return msgID, nil
		}
	}
	if m.Status.terminal() {
		status := m.Status
		coord.mu.Unlock()
		return "", fmt.Errorf("%w: %s", ErrManagedTerminal, status)
	}
	m.Inbox = append(m.Inbox, ManagedMessage{ID: msgID, Payload: data})
	ticket, werr := coord.publishLocked(ctx, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	coord.mu.Unlock()
	if werr != nil {
		return "", werr
	}
	if err := ticket.wait(); err != nil {
		return "", err
	}
	return msgID, nil
}

// CancelManaged 显式取消本调用者直接创建的受管任务及其未结束后代。
// 取消是终态：重复取消不重复产生副作用，根 Resume 不再启动这些任务；
// 已完成结果保持可读。
func CancelManaged(ctx context.Context, taskID string) (ManagedResult, error) {
	site := callSiteFromContext(ctx)
	if site == nil || site.coord == nil {
		return ManagedResult{}, ErrManagedUnavailable
	}
	coord := site.coord
	coord.mu.Lock()
	targets, err := resolveManagedTargets(site, []string{taskID})
	if err != nil {
		coord.mu.Unlock()
		return ManagedResult{}, err
	}
	m := targets[0]
	if m.Status.terminal() {
		result := m.result()
		coord.mu.Unlock()
		return result, nil
	}
	coord.cancelManagedTree(m, "cancelled by owner")
	ticket, werr := coord.publishLocked(ctx, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	coord.mu.Unlock()
	if werr != nil {
		return ManagedResult{}, werr
	}
	if err := ticket.wait(); err != nil {
		return ManagedResult{}, err
	}
	return m.result(), nil
}

// ManagedInbox 返回当前受管执行上下文中的未消费消息（供执行器在
// 模型调用边界读取）：本任务已确认未提交的批次重放在前，其后是
// 其余未确认消息。只读视图，不推进消费游标。
func ManagedInbox(ctx context.Context) ([]ManagedMessage, error) {
	site := managedSiteFromContext(ctx)
	task := callSiteFromContext(ctx)
	if site == nil || site.managed == nil {
		return nil, ErrManagedUnavailable
	}
	taskID := ""
	if task != nil {
		taskID = task.taskID
	}
	site.coord.mu.Lock()
	defer site.coord.mu.Unlock()
	out := site.managed.inboxView(taskID)
	for i := range out {
		out[i] = out[i].Clone()
	}
	return out, nil
}

// AckManagedInbox 确认当前任务已处理收件箱视图中的前 count 条消息。
// 确认只记入本任务的待提交批次——消费游标在任务提交边界（completeTask，
// 裸 runner 在任务完成边界）才提升；任务挂起重放会把未提交批次重新投递，
// 审批恢复不会丢已确认消息，也不会把未完成处理的消息误记为已消费。
func AckManagedInbox(ctx context.Context, count int) error {
	site := managedSiteFromContext(ctx)
	if site == nil || site.managed == nil {
		return ErrManagedUnavailable
	}
	// 图任务内确认按任务 ID 记账；裸 runner（非图任务上下文）共用空键
	// 批次——其进度随受管任务完成边界整体提升。
	taskID := ""
	if task := callSiteFromContext(ctx); task != nil {
		taskID = task.taskID
	}
	coord := site.coord
	m := site.managed
	var ackErr error
	ticket, err := coord.mutateTree(ctx, func() {
		view := m.inboxView(taskID)
		if count < 0 || count > len(view) {
			ackErr = fmt.Errorf("managed inbox ack %d out of range", count)
			return
		}
		if m.TaskInbox == nil {
			m.TaskInbox = map[string][]string{}
		}
		ids := make([]string, 0, count)
		for _, msg := range view[:count] {
			ids = append(ids, msg.ID)
		}
		m.TaskInbox[taskID] = ids
	}, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	if err != nil {
		return err
	}
	if ackErr != nil {
		return ackErr
	}
	return ticket.wait()
}
