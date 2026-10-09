package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/state"
)

// AgentStatus 是会话内子 Agent 的生命周期状态。active 表示有一项未结束的
// 任务（运行或挂起）；idle 表示本轮任务已结算、等待下一次派发；closed 表示
// 已显式关闭，不再接受新任务，历史结果仍可读取。
type AgentStatus string

const (
	AgentStatusActive AgentStatus = "active"
	AgentStatusIdle   AgentStatus = "idle"
	AgentStatusClosed AgentStatus = "closed"
)

var (
	// ErrAgentNotFound 表示目标子 Agent 不存在，或不是本调用者创建的——
	// 可见范围限定为调用者直接创建的子 Agent，且只在所属会话内。
	// 以下三个错误面向模型可修正的调用，不带框架错误标记。
	ErrAgentNotFound = errors.New("session agent not found")
	// ErrAgentBusy 表示子 Agent 仍有未结束的任务：同一子 Agent 同时只有
	// 一项活动任务，避免并发改写其上下文；审批挂起仍属于原任务。
	ErrAgentBusy = errors.New("session agent has an unfinished task")
	// ErrAgentClosed 表示子 Agent 已关闭，不再接受新任务。
	ErrAgentClosed = errors.New("session agent is closed")
)

// sessionAgentsFormat 是会话级子 Agent 集合的编码格式版本。
const sessionAgentsFormat = 1

// summaryLimit 是结果摘要的最大字符数；详细结果按需读取。
const summaryLimit = 200

// agentTask 是子 Agent 任务结果索引中的一项：一次派发的冻结输入与结算
// 结果。TaskID 在会话内唯一，同一子 Agent 的任务按 Seq 递增。未结算的
// 任务在索引里恒记为 running——运行或挂起的精确状态以执行树为准，
// 索引只在任务到达终态的同一次树变更内结算。
type agentTask struct {
	ID     string          `json:"id"`
	Seq    int             `json:"seq"`
	Status ManagedStatus   `json:"status"`
	Input  json.RawMessage `json:"input,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// sessionAgent 是会话级的子 Agent 记录：稳定身份、创建关系、最终配置与
// 版本、独立上下文和任务结果索引。它与某一轮运行的执行树分离，新一轮
// 清空执行树时仍然保留。
type sessionAgent struct {
	ID      string `json:"id"`
	Creator string `json:"creator,omitempty"`
	// Description 是创建时给出的职责说明，供父 Agent 辨认。
	Description string `json:"description,omitempty"`
	// Spec 保存最终配置（构建函数身份、版本、解析后的配置），不含任何
	// 任务输入；追加任务按它重新构建执行器，不再重新应用默认值。
	Spec   ManagedSpec `json:"spec"`
	Status AgentStatus `json:"status"`
	// Active 是活动任务的 TaskID；idle 与 closed 时为空。
	Active string `json:"active,omitempty"`
	// Context 是执行器导出的独立上下文，任务以完成状态结算时更新；
	// 失败或取消不改写它。
	Context json.RawMessage `json:"context,omitempty"`
	Tasks   []agentTask     `json:"tasks,omitempty"`
}

type sessionAgentBook struct {
	Format int             `json:"format"`
	Agents []*sessionAgent `json:"agents"`
}

func (t agentTask) clone() agentTask {
	t.Input = cloneRawJSON(t.Input)
	t.Result = cloneRawJSON(t.Result)
	return t
}

func (a *sessionAgent) clone() *sessionAgent {
	out := *a
	out.Spec.Config = cloneRawJSON(a.Spec.Config)
	out.Spec.Input = cloneRawJSON(a.Spec.Input)
	out.Context = cloneRawJSON(a.Context)
	out.Tasks = make([]agentTask, len(a.Tasks))
	for i, task := range a.Tasks {
		out.Tasks[i] = task.clone()
	}
	return &out
}

func cloneRawJSON(data json.RawMessage) json.RawMessage {
	if data == nil {
		return nil
	}
	return append(json.RawMessage(nil), data...)
}

// agentTaskID 生成子 Agent 任务的会话级唯一身份：AgentID 加该子 Agent 内
// 的任务序号。它不依赖运行或调用位置，下一轮的追加任务不会与上一轮撞号。
func agentTaskID(agentID string, seq int) string {
	return fmt.Sprintf("at/%s/%d", agentID, seq)
}

// decodeSessionAgents 严格解码会话记录中的子 Agent 集合；空载荷即空集合。
func decodeSessionAgents(raw json.RawMessage) ([]*sessionAgent, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var book sessionAgentBook
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&book); err != nil {
		return nil, fmt.Errorf("%w: decode session agents: %v", state.ErrInvalidCheckpoint, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing session agent data", state.ErrInvalidCheckpoint)
	}
	if book.Format != sessionAgentsFormat {
		return nil, fmt.Errorf("%w: unsupported session agents format %d", state.ErrInvalidCheckpoint, book.Format)
	}
	if err := validateSessionAgents(book.Agents); err != nil {
		return nil, err
	}
	return book.Agents, nil
}

func validateSessionAgents(agents []*sessionAgent) error {
	seen := make(map[string]bool, len(agents))
	taskSeen := map[string]bool{}
	for i, a := range agents {
		if a == nil || strings.TrimSpace(a.ID) == "" {
			return fmt.Errorf("%w: session agent[%d] requires an id", state.ErrInvalidCheckpoint, i)
		}
		if seen[a.ID] {
			return fmt.Errorf("%w: duplicate session agent %q", state.ErrInvalidCheckpoint, a.ID)
		}
		seen[a.ID] = true
		if strings.TrimSpace(a.Spec.Builder) == "" {
			return fmt.Errorf("%w: session agent %q requires builder identity", state.ErrInvalidCheckpoint, a.ID)
		}
		switch a.Status {
		case AgentStatusActive, AgentStatusIdle, AgentStatusClosed:
		default:
			return fmt.Errorf("%w: session agent %q has invalid status %q", state.ErrInvalidCheckpoint, a.ID, a.Status)
		}
		for _, field := range []struct {
			name string
			data json.RawMessage
		}{{"config", a.Spec.Config}, {"context", a.Context}} {
			if len(field.data) > 0 && !json.Valid(field.data) {
				return fmt.Errorf("%w: session agent %q %s is not valid JSON", state.ErrInvalidCheckpoint, a.ID, field.name)
			}
		}
		activeFound := false
		for j, task := range a.Tasks {
			if task.ID != agentTaskID(a.ID, j+1) || task.Seq != j+1 {
				return fmt.Errorf("%w: session agent %q task[%d] has unexpected identity %q", state.ErrInvalidCheckpoint, a.ID, j, task.ID)
			}
			if taskSeen[task.ID] {
				return fmt.Errorf("%w: duplicate agent task %q", state.ErrInvalidCheckpoint, task.ID)
			}
			taskSeen[task.ID] = true
			switch task.Status {
			case ManagedStatusRunning, ManagedStatusSuspended, ManagedStatusCompleted, ManagedStatusFailed, ManagedStatusCancelled:
			default:
				return fmt.Errorf("%w: agent task %q has invalid status %q", state.ErrInvalidCheckpoint, task.ID, task.Status)
			}
			for _, field := range []struct {
				name string
				data json.RawMessage
			}{{"input", task.Input}, {"result", task.Result}} {
				if len(field.data) > 0 && !json.Valid(field.data) {
					return fmt.Errorf("%w: agent task %q %s is not valid JSON", state.ErrInvalidCheckpoint, task.ID, field.name)
				}
			}
			if task.ID == a.Active {
				activeFound = true
				if task.Status.terminal() {
					return fmt.Errorf("%w: session agent %q active task %q already settled", state.ErrInvalidCheckpoint, a.ID, task.ID)
				}
			} else if !task.Status.terminal() {
				return fmt.Errorf("%w: session agent %q task %q is unfinished but not the active task", state.ErrInvalidCheckpoint, a.ID, task.ID)
			}
		}
		if (a.Status == AgentStatusActive) != (a.Active != "") || (a.Active != "" && !activeFound) {
			return fmt.Errorf("%w: session agent %q status %q is inconsistent with active task %q", state.ErrInvalidCheckpoint, a.ID, a.Status, a.Active)
		}
	}
	return nil
}

// findAgentLocked 按 AgentID 查找会话集合中的子 Agent；须持有树锁。
func (c *runCoordinator) findAgentLocked(id string) *sessionAgent {
	for _, a := range c.agents {
		if a.ID == id {
			return a
		}
	}
	return nil
}

// encodeAgentsLocked 编码会话集合；空集合返回 nil（保持记录原样）。
// 编码结果按集合变更缓存，持锁调用。
func (c *runCoordinator) encodeAgentsLocked() (json.RawMessage, error) {
	if len(c.agents) == 0 {
		return nil, nil
	}
	if c.agentsData != nil {
		return c.agentsData, nil
	}
	data, err := json.Marshal(sessionAgentBook{Format: sessionAgentsFormat, Agents: c.agents})
	if err != nil {
		return nil, fmt.Errorf("encode session agents: %w", err)
	}
	c.agentsData = data
	return data, nil
}

// attachAgents 在运行入口把会话记录中的子 Agent 集合载入协调器。
// 恢复运行时校验集合与执行树互相一致——引用缺失说明记录被改写或回退
// 不完整，明确拒绝；新一轮运行没有执行树，遗留的活动状态（记录之外的
// 异常退出所致）按已取消结算，使子 Agent 回到可追加任务的空闲态。
func (c *runCoordinator) attachAgents(raw json.RawMessage, resumed bool) error {
	agents, err := decodeSessionAgents(raw)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agents, c.agentsData = agents, nil
	if resumed {
		return c.validateAgentLinksLocked()
	}
	for _, a := range c.agents {
		if a.Status != AgentStatusActive {
			continue
		}
		for i := range a.Tasks {
			if a.Tasks[i].ID == a.Active {
				a.Tasks[i].Status = ManagedStatusCancelled
				a.Tasks[i].Error = "task did not finish before its run ended"
			}
		}
		a.Status, a.Active = AgentStatusIdle, ""
	}
	return nil
}

// validateAgentLinksLocked 校验执行树中带 AgentID 的任务与会话集合一致。
func (c *runCoordinator) validateAgentLinksLocked() error {
	var linkErr error
	live := map[string]bool{}
	walkManaged(c.root, func(m *managedTask) {
		if linkErr != nil || m.AgentID == "" {
			return
		}
		a := c.findAgentLocked(m.AgentID)
		switch {
		case a == nil:
			linkErr = fmt.Errorf("%w: managed task %s references unknown session agent %q", state.ErrInvalidCheckpoint, m.ID, m.AgentID)
		case !m.Status.terminal() && a.Active != m.ID:
			linkErr = fmt.Errorf("%w: managed task %s is unfinished but session agent %q tracks %q", state.ErrInvalidCheckpoint, m.ID, a.ID, a.Active)
		case !m.Status.terminal():
			live[a.ID] = true
		}
	})
	if linkErr != nil {
		return linkErr
	}
	for _, a := range c.agents {
		if a.Active != "" && !live[a.ID] {
			return fmt.Errorf("%w: session agent %q tracks task %q missing from the execution tree", state.ErrInvalidCheckpoint, a.ID, a.Active)
		}
	}
	return nil
}

// settleAgentTaskLocked 在受管任务到达终态的同一次树变更内结算其子 Agent：
// 任务索引记录结果，活动任务清空使子 Agent 回到空闲；任务完成时提交
// 执行器导出的上下文，失败与取消保持原上下文不变。须持有树锁。
func (c *runCoordinator) settleAgentTaskLocked(m *managedTask) {
	if m.AgentID == "" {
		return
	}
	a := c.findAgentLocked(m.AgentID)
	if a == nil {
		return
	}
	for i := range a.Tasks {
		if a.Tasks[i].ID != m.ID {
			continue
		}
		a.Tasks[i].Status = m.Status
		a.Tasks[i].Result = cloneRawJSON(m.Result)
		a.Tasks[i].Error = m.Error
		if m.Status == ManagedStatusCancelled && m.Error == "" {
			a.Tasks[i].Error = m.Reason
		}
	}
	if a.Active == m.ID {
		a.Active = ""
		if a.Status == AgentStatusActive {
			a.Status = AgentStatusIdle
		}
	}
	if m.Status == ManagedStatusCompleted && m.AgentContext != nil {
		a.Context = cloneRawJSON(m.AgentContext)
	}
	m.AgentContext = nil
	c.agentsData = nil
}

// --- 子 Agent 视图 ---

// AgentRef 标识一次派发：子 Agent 的稳定身份与本次任务的身份。
type AgentRef struct {
	AgentID string `json:"agent_id"`
	TaskID  string `json:"task_id"`
}

// AgentSummary 是父 Agent 每轮可见的子 Agent 概览：身份、职责、最近一次
// 任务的状态与结果摘要。完整结果经 GetAgent 按需读取。
type AgentSummary struct {
	AgentID     string        `json:"agent_id"`
	Description string        `json:"description,omitempty"`
	Status      AgentStatus   `json:"status"`
	TaskCount   int           `json:"task_count"`
	LastTask    string        `json:"last_task,omitempty"`
	LastStatus  ManagedStatus `json:"last_status,omitempty"`
	Summary     string        `json:"summary,omitempty"`
	Error       string        `json:"error,omitempty"`
}

// AgentTaskInfo 是结果索引中的一项任务：冻结输入、状态与完整结果。
type AgentTaskInfo struct {
	TaskID string          `json:"task_id"`
	Seq    int             `json:"seq"`
	Status ManagedStatus   `json:"status"`
	Input  json.RawMessage `json:"input,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// AgentInfo 是子 Agent 的完整记录视图（不含其独立上下文——上下文只由
// 子 Agent 自己的任务读取，父 Agent 的输入不自动包含它）。
type AgentInfo struct {
	AgentID     string          `json:"agent_id"`
	Creator     string          `json:"creator,omitempty"`
	Description string          `json:"description,omitempty"`
	Status      AgentStatus     `json:"status"`
	Builder     string          `json:"builder"`
	Version     string          `json:"version,omitempty"`
	Config      json.RawMessage `json:"config,omitempty"`
	ActiveTask  string          `json:"active_task,omitempty"`
	Tasks       []AgentTaskInfo `json:"tasks,omitempty"`
}

func (a *sessionAgent) summary() AgentSummary {
	out := AgentSummary{AgentID: a.ID, Description: a.Description, Status: a.Status, TaskCount: len(a.Tasks)}
	if n := len(a.Tasks); n > 0 {
		last := a.Tasks[n-1]
		out.LastTask, out.LastStatus, out.Error = last.ID, last.Status, last.Error
		out.Summary = summarizeResult(last.Result, summaryLimit)
	}
	return out
}

func (a *sessionAgent) info() AgentInfo {
	out := AgentInfo{
		AgentID: a.ID, Creator: a.Creator, Description: a.Description, Status: a.Status,
		Builder: a.Spec.Builder, Version: a.Spec.Version, Config: cloneRawJSON(a.Spec.Config), ActiveTask: a.Active,
	}
	for _, task := range a.Tasks {
		out.Tasks = append(out.Tasks, AgentTaskInfo{
			TaskID: task.ID, Seq: task.Seq, Status: task.Status,
			Input: cloneRawJSON(task.Input), Result: cloneRawJSON(task.Result), Error: task.Error,
		})
	}
	return out
}

// summarizeResult 把结果压缩为定长摘要：字符串结果取其文本，其余取
// 紧凑 JSON，超长按字符截断。
func summarizeResult(result json.RawMessage, limit int) string {
	if len(result) == 0 {
		return ""
	}
	text := string(result)
	var s string
	if err := json.Unmarshal(result, &s); err == nil {
		text = s
	}
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	return string([]rune(text)[:limit]) + "…"
}

// SessionAgentsOf 读取会话记录中保存的子 Agent，供运行之外的调用方
// （界面、下一轮提示词组装、测试）查看；它们只含已提交的记录。
func SessionAgentsOf(record state.Checkpoint) ([]AgentInfo, error) {
	agents, err := decodeSessionAgents(record.SubAgents)
	if err != nil {
		return nil, err
	}
	out := make([]AgentInfo, len(agents))
	for i, a := range agents {
		out[i] = a.info()
	}
	return out, nil
}

// --- 引擎级子 Agent API：供任务内代码与工具层调用。 ---

// callerAgentID 返回调用者所属的子 Agent 身份：根运行与非子 Agent 的
// 受管任务为空。创建关系与可见范围都以它为准。
func callerAgentID(ctx context.Context) string {
	if site := managedSiteFromContext(ctx); site != nil && site.managed != nil {
		return site.managed.AgentID
	}
	return ""
}

func agentSite(ctx context.Context) (*callSite, error) {
	site := callSiteFromContext(ctx)
	if site == nil || site.coord == nil {
		return nil, ErrManagedUnavailable
	}
	return site, nil
}

// visibleAgentLocked 查找调用者可见的子 Agent：只有调用者直接创建的才
// 可见，其余（含其他子 Agent 创建的）按不存在处理。须持有树锁。
func (c *runCoordinator) visibleAgentLocked(caller, id string) (*sessionAgent, error) {
	a := c.findAgentLocked(id)
	if a == nil || a.Creator != caller {
		return nil, fmt.Errorf("%w: %q", ErrAgentNotFound, id)
	}
	return a, nil
}

// SpawnAgent 创建会话内的子 Agent 并启动它的首个任务。AgentID 在会话内
// 保持稳定，TaskID 标识本次派发；两者都在确认保存后才返回。重放同一次
// 创建调用复用原记录，相同内容的新请求创建新的子 Agent。
func SpawnAgent(ctx context.Context, spec ManagedSpec, description string) (AgentRef, error) {
	site, err := agentSite(ctx)
	if err != nil {
		return AgentRef{}, err
	}
	spec, err = normalizeManagedSpec(spec)
	if err != nil {
		return AgentRef{}, err
	}
	coord := site.coord
	callKey, _ := control.CallKeyFromContext(ctx)
	existing, seq, err := coord.peekManaged(site, callKey, spec)
	if err != nil {
		return AgentRef{}, err
	}
	if existing != nil {
		if existing.AgentID == "" {
			return AgentRef{}, fmt.Errorf("%w: agent spawn %s was registered as a plain managed task", ErrResumeIncompatible, existing.ID)
		}
		return AgentRef{AgentID: existing.AgentID, TaskID: existing.ID}, nil
	}
	runner, err := coord.buildManaged(ctx, spec)
	if err != nil {
		return AgentRef{}, err
	}
	creator := callerAgentID(ctx)
	m, exists, err := coord.registerManagedWith(ctx, site, callKey, seq, spec, func(m *managedTask) error {
		// 会话集合归集到根协调器：属主不同（命名空间子运行内创建）时
		// 需要加根锁；属主相同说明外层注册锁已覆盖，不重复加锁。
		agentCoord := coord.agentScope()
		if agentCoord != coord {
			agentCoord.mu.Lock()
			defer agentCoord.mu.Unlock()
		}
		id := agentCoord.newAgentIDLocked()
		// 会话级配置取自任务已冻结的 Spec 并独立复制：调用方原切片此后
		// 的修改既不影响任务记录，也不影响子 Agent 的登记配置。
		agentSpec := ManagedSpec{Builder: m.Spec.Builder, Version: m.Spec.Version, Config: cloneRawJSON(m.Spec.Config)}
		m.AgentID = id
		m.ID = agentTaskID(id, 1)
		agentCoord.agents = append(agentCoord.agents, &sessionAgent{
			ID: id, Creator: creator, Description: description, Spec: agentSpec,
			Status: AgentStatusActive, Active: m.ID,
			Tasks: []agentTask{{ID: m.ID, Seq: 1, Status: ManagedStatusRunning, Input: cloneRawJSON(spec.Input)}},
		})
		agentCoord.agentsData = nil
		return nil
	})
	if err != nil {
		return AgentRef{}, err
	}
	if !exists {
		coord.launchManaged(coord.runCtx, m, runner)
	}
	return AgentRef{AgentID: m.AgentID, TaskID: m.ID}, nil
}

// newAgentIDLocked 分配会话内未使用的 AgentID；集合只增不减，新 ID
// 不会复用已存在（含已关闭）的子 Agent。须持有树锁。
func (c *runCoordinator) newAgentIDLocked() string {
	for n := len(c.agents) + 1; ; n++ {
		id := fmt.Sprintf("agent-%d", n)
		if c.findAgentLocked(id) == nil {
			return id
		}
	}
}

// FollowupAgent 给调用者创建的空闲子 Agent 追加一项新任务：AgentID 保持
// 不变，TaskID 更新，沿用该子 Agent 登记的配置与独立上下文。已有任务
// 未结束（含审批挂起）返回 ErrAgentBusy，已关闭返回 ErrAgentClosed；
// 重放同一次追加调用复用原任务，相同文本的新请求再次执行。
func FollowupAgent(ctx context.Context, agentID string, input json.RawMessage) (AgentRef, error) {
	site, err := agentSite(ctx)
	if err != nil {
		return AgentRef{}, err
	}
	coord := site.coord
	agentCoord := coord.agentScope()
	creator := callerAgentID(ctx)
	// 追加任务的配置固定取自子 Agent 的登记记录，不接受调用方改写。
	agentCoord.mu.Lock()
	agent, verr := agentCoord.visibleAgentLocked(creator, agentID)
	var spec ManagedSpec
	if verr == nil {
		spec = agent.Spec
		spec.Config = cloneRawJSON(agent.Spec.Config)
		spec.Input = cloneRawJSON(input)
	}
	agentCoord.mu.Unlock()
	if verr != nil {
		return AgentRef{}, verr
	}
	spec, err = normalizeManagedSpec(spec)
	if err != nil {
		return AgentRef{}, err
	}
	callKey, _ := control.CallKeyFromContext(ctx)
	existing, seq, err := coord.peekManaged(site, callKey, spec)
	if err != nil {
		return AgentRef{}, err
	}
	if existing != nil {
		if existing.AgentID != agentID {
			return AgentRef{}, fmt.Errorf("%w: followup %s targeted a different agent in the saved run", ErrResumeIncompatible, existing.ID)
		}
		return AgentRef{AgentID: agentID, TaskID: existing.ID}, nil
	}
	runner, err := coord.buildManaged(ctx, spec)
	if err != nil {
		return AgentRef{}, err
	}
	m, exists, err := coord.registerManagedWith(ctx, site, callKey, seq, spec, func(m *managedTask) error {
		if agentCoord != coord {
			agentCoord.mu.Lock()
			defer agentCoord.mu.Unlock()
		}
		a, err := agentCoord.visibleAgentLocked(creator, agentID)
		if err != nil {
			return err
		}
		switch a.Status {
		case AgentStatusClosed:
			return fmt.Errorf("%w: %q", ErrAgentClosed, agentID)
		case AgentStatusActive:
			return fmt.Errorf("%w: %q is running %s", ErrAgentBusy, agentID, a.Active)
		}
		taskSeq := len(a.Tasks) + 1
		m.AgentID = agentID
		m.ID = agentTaskID(agentID, taskSeq)
		a.Tasks = append(a.Tasks, agentTask{ID: m.ID, Seq: taskSeq, Status: ManagedStatusRunning, Input: cloneRawJSON(spec.Input)})
		a.Status, a.Active = AgentStatusActive, m.ID
		agentCoord.agentsData = nil
		return nil
	})
	if err != nil {
		return AgentRef{}, err
	}
	if !exists {
		coord.launchManaged(coord.runCtx, m, runner)
	}
	return AgentRef{AgentID: agentID, TaskID: m.ID}, nil
}

// ListAgents 返回调用者创建的全部子 Agent 概览（含已关闭的），按创建
// 顺序排列。它读取会话集合，跨轮次可见；详细结果用 GetAgent。
func ListAgents(ctx context.Context) ([]AgentSummary, error) {
	site, err := agentSite(ctx)
	if err != nil {
		return nil, err
	}
	creator := callerAgentID(ctx)
	coord := site.coord.agentScope()
	coord.mu.Lock()
	defer coord.mu.Unlock()
	var out []AgentSummary
	for _, a := range coord.agents {
		if a.Creator == creator {
			out = append(out, a.summary())
		}
	}
	return out, nil
}

// GetAgent 读取调用者创建的子 Agent 的完整记录与历史任务结果。
func GetAgent(ctx context.Context, agentID string) (AgentInfo, error) {
	site, err := agentSite(ctx)
	if err != nil {
		return AgentInfo{}, err
	}
	coord := site.coord.agentScope()
	coord.mu.Lock()
	defer coord.mu.Unlock()
	a, err := coord.visibleAgentLocked(callerAgentID(ctx), agentID)
	if err != nil {
		return AgentInfo{}, err
	}
	return a.info(), nil
}

// CloseAgent 关闭调用者创建的空闲子 Agent：此后拒绝追加任务，已保存的
// 结果仍可读取。仍有未结束任务时返回 ErrAgentBusy，需先取消；重复关闭
// 幂等。关闭在确认保存后才返回。
func CloseAgent(ctx context.Context, agentID string) (AgentInfo, error) {
	site, err := agentSite(ctx)
	if err != nil {
		return AgentInfo{}, err
	}
	coord := site.coord.agentScope()
	coord.mu.Lock()
	a, err := coord.visibleAgentLocked(callerAgentID(ctx), agentID)
	if err != nil {
		coord.mu.Unlock()
		return AgentInfo{}, err
	}
	if a.Status == AgentStatusActive {
		coord.mu.Unlock()
		return AgentInfo{}, fmt.Errorf("%w: %q is running %s", ErrAgentBusy, agentID, a.Active)
	}
	if a.Status == AgentStatusClosed {
		info := a.info()
		coord.mu.Unlock()
		return info, nil
	}
	a.Status = AgentStatusClosed
	coord.agentsData = nil
	info := a.info()
	ticket, werr := coord.publishLocked(ctx, func(record *state.Checkpoint, data []byte) { record.Execution = data })
	coord.mu.Unlock()
	if werr != nil {
		return AgentInfo{}, werr
	}
	if err := ticket.wait(); err != nil {
		return AgentInfo{}, err
	}
	return info, nil
}

// ManagedAgentID 返回当前受管执行所属的子 Agent 身份；非子 Agent 任务
// 返回空串。
func ManagedAgentID(ctx context.Context) string {
	return callerAgentID(ctx)
}

// ManagedAgentContext 返回当前子 Agent 任务开始时可用的独立上下文：它
// 由该子 Agent 之前完成的任务导出，首个任务或从未导出时为 nil。运行
// 期间只有本任务活动，读取到的内容保持稳定，重驱恢复也读到同一份。
func ManagedAgentContext(ctx context.Context) (json.RawMessage, error) {
	site := managedSiteFromContext(ctx)
	if site == nil || site.managed == nil || site.managed.AgentID == "" {
		return nil, ErrManagedUnavailable
	}
	coord := site.coord.agentScope()
	coord.mu.Lock()
	defer coord.mu.Unlock()
	a := coord.findAgentLocked(site.managed.AgentID)
	if a == nil {
		return nil, fmt.Errorf("%w: %q", ErrAgentNotFound, site.managed.AgentID)
	}
	return cloneRawJSON(a.Context), nil
}

// SetManagedAgentContext 导出当前子 Agent 任务产生的独立上下文，任务以
// 完成状态结算时并入会话集合并成为下一次任务的起点；任务失败或取消则
// 丢弃，子 Agent 保持原上下文。可多次调用，以最后一次为准。
func SetManagedAgentContext(ctx context.Context, data json.RawMessage) error {
	site := managedSiteFromContext(ctx)
	if site == nil || site.managed == nil || site.managed.AgentID == "" {
		return ErrManagedUnavailable
	}
	if !json.Valid(data) {
		return errors.New("managed agent context is not valid JSON")
	}
	site.coord.mu.Lock()
	site.managed.AgentContext = cloneRawJSON(data)
	site.coord.mu.Unlock()
	return nil
}
