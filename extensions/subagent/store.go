package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

var (
	// ErrAgentNotFound 表示给定 ID 的子 Agent 不存在或对当前创建者不可见。
	ErrAgentNotFound = errors.New("sub-agent not found")
	// ErrAgentBusy 表示子 Agent 已有进行中的任务：同步模型下同一时刻
	// 只能有一个任务，重复调用是重放或其他调用的并发冲突。
	ErrAgentBusy = errors.New("sub-agent has a task in progress")
	// ErrAgentClosed 表示子 Agent 已关闭，不再接受新任务。
	ErrAgentClosed = errors.New("sub-agent is closed")
	// ErrAgentCorrupt 表示持久化的子 Agent 记录损坏或与其登记实现不符。
	ErrAgentCorrupt = errors.New("sub-agent record is corrupt")
	// ErrNoSession 表示会话子 Agent 功能需要带检查点的运行与配置的 Store。
	ErrNoSession = errors.New("sub-agent session requires a checkpoint-enabled run and a configured store")
)

// 会话子 Agent 记录在调用方 Store 中按命名空间键存放：
// 索引为 "<会话ID>/agents"，各 Agent 记录为 "<会话ID>/agents/<ID>"；
// 每条记录是一次普通的检查点保存，历史随 Store 保留，Cleanup 按 RunID 回退。
const (
	agentIndexSuffix    = "/agents"
	agentStatusIdle     = "idle"
	agentStatusRunning  = "running"
	agentStatusClosed   = "closed"
	taskStatusRunning   = "running"
	taskStatusCompleted = "completed"
	taskStatusFailed    = "failed"
)

// indexEntry 是会话内子 Agent 的列表项。
type indexEntry struct {
	AgentID     string `json:"agent_id"`
	Description string `json:"description"`
	Creator     string `json:"creator"`
	Status      string `json:"status"`
	Result      string `json:"result,omitempty"`
	Updated     string `json:"updated_task,omitempty"`
}

type agentIndex struct {
	Agents []indexEntry `json:"agents"`
}

// taskRecord 是子 Agent 的一次任务记录；RunID 标识发起它的运行。
// Input 是登记时冻结的任务输入（编码后的消息列表）：同一调用重放必须
// 用逐字节一致的输入进入嵌套子执行，登记后上下文变化不再影响输入。
type taskRecord struct {
	TaskID string          `json:"task_id"`
	Task   string          `json:"task"`
	RunID  string          `json:"run_id"`
	Status string          `json:"status"`
	Input  json.RawMessage `json:"input,omitempty"`
	Result string          `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// agentRecord 是会话子 Agent 的持久化记录本体。
type agentRecord struct {
	AgentID        string          `json:"agent_id"`
	Description    string          `json:"description"`
	Creator        string          `json:"creator"`
	OwnerRunID     string          `json:"owner_run_id"`
	BuilderID      string          `json:"builder_id"`
	BuilderVersion string          `json:"builder_version"`
	Config         savedConfig     `json:"config"`
	Status         string          `json:"status"`
	Context        json.RawMessage `json:"context,omitempty"`
	Tasks          []taskRecord    `json:"tasks"`
}

// AgentSummary 是 list_agents 返回的单项视图。
type AgentSummary struct {
	AgentID     string `json:"agent_id"`
	Description string `json:"description"`
	Status      string `json:"status"`
	Result      string `json:"result,omitempty"`
	Tasks       int    `json:"tasks"`
}

// AgentDetail 是 get_agent 返回的完整视图。
type AgentDetail struct {
	AgentID     string       `json:"agent_id"`
	Description string       `json:"description"`
	Status      string       `json:"status"`
	Creator     string       `json:"creator,omitempty"`
	Tasks       []taskRecord `json:"tasks"`
}

type callerKey struct{}

// withCaller 记录当前调用者（发起方子 Agent）的 ID；根调用为空。
func withCaller(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, callerKey{}, id)
}

func callerID(ctx context.Context) string {
	id, _ := ctx.Value(callerKey{}).(string)
	return id
}

// sessionScope 从调用上下文取会话身份：会话子 Agent 需要运行处于
// 检查点会话中（state.FromContext 可解析），否则返回 ErrNoSession。
// runID 取本轮运行身份（ownerRunID），不是快照里上一轮的历史 RunID。
func (m *Manager) sessionScope(ctx context.Context, in tool.CallInput) (conversationID, runID string, err error) {
	if m.cfg.Store == nil {
		return "", "", ErrNoSession
	}
	checkpoint, ok := state.FromContext(ctx)
	if !ok || checkpoint.ConversationID == "" {
		return "", "", ErrNoSession
	}
	return checkpoint.ConversationID, ownerRunID(in), nil
}

// ownerRunID 从调用幂等键取本轮根运行身份：工具运行时把
// 「运行范围/节点路径/批次序号/调用键」写进 IdempotencyKey，运行范围的
// 首段即持久化 RunID（嵌套调用共享根身份）；无批次来源的裸调用返回空串。
func ownerRunID(in tool.CallInput) string {
	key := in.IdempotencyKey
	if strings.Count(key, "/") < 3 {
		return ""
	}
	runID, _, _ := strings.Cut(key, "/")
	return runID
}

func indexKey(conversationID string) string { return conversationID + agentIndexSuffix }

func agentKey(conversationID, agentID string) string {
	return indexKey(conversationID) + "/" + agentID
}

// loadIndex 读会话内的子 Agent 索引；缺失返回空索引，损坏返回 ErrAgentCorrupt。
func (m *Manager) loadIndex(ctx context.Context, conversationID string) (agentIndex, state.Checkpoint, error) {
	record, found, err := m.cfg.Store.Load(ctx, indexKey(conversationID))
	if err != nil {
		return agentIndex{}, state.Checkpoint{}, err
	}
	if !found {
		return agentIndex{}, state.Checkpoint{}, nil
	}
	var index agentIndex
	if err := json.Unmarshal(record.Result, &index); err != nil {
		return agentIndex{}, state.Checkpoint{}, fmt.Errorf("%w: decode agent index: %v", ErrAgentCorrupt, err)
	}
	return index, record, nil
}

func (m *Manager) saveIndex(ctx context.Context, conversationID string, index agentIndex, latest state.Checkpoint) error {
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	_, err = m.cfg.Store.Save(ctx, state.Checkpoint{
		ConversationID: indexKey(conversationID),
		ID:             latest.ID,
		Version:        latest.Version,
		Result:         data,
	})
	return err
}

// loadAgent 读单条子 Agent 记录并做创建者可见性过滤。
func (m *Manager) loadAgent(ctx context.Context, conversationID, agentID, caller string) (agentRecord, state.Checkpoint, error) {
	agent, latest, found, err := m.loadAgentRaw(ctx, conversationID, agentID)
	if err != nil {
		return agentRecord{}, state.Checkpoint{}, err
	}
	if !found || agent.Creator != caller {
		return agentRecord{}, state.Checkpoint{}, ErrAgentNotFound
	}
	return agent, latest, nil
}

// loadAgentRaw 读记录不做可见性过滤：updateAgent 内部用它，
// 新建记录的场景下记录尚不存在也属合法。
func (m *Manager) loadAgentRaw(ctx context.Context, conversationID, agentID string) (agentRecord, state.Checkpoint, bool, error) {
	record, found, err := m.cfg.Store.Load(ctx, agentKey(conversationID, agentID))
	if err != nil {
		return agentRecord{}, state.Checkpoint{}, false, err
	}
	if !found {
		return agentRecord{}, state.Checkpoint{}, false, nil
	}
	var agent agentRecord
	if err := json.Unmarshal(record.Result, &agent); err != nil || agent.AgentID == "" {
		return agentRecord{}, state.Checkpoint{}, false, fmt.Errorf("%w: decode agent %q: %v", ErrAgentCorrupt, agentID, err)
	}
	return agent, record, true, nil
}

func (m *Manager) saveAgent(ctx context.Context, conversationID string, agent agentRecord, latest state.Checkpoint, runID string) error {
	data, err := json.Marshal(agent)
	if err != nil {
		return err
	}
	_, err = m.cfg.Store.Save(ctx, state.Checkpoint{
		ConversationID: agentKey(conversationID, agent.AgentID),
		ID:             latest.ID,
		Version:        latest.Version,
		RunID:          runID,
		Result:         data,
	})
	return err
}

// indexStatus 把记录的最新状态同步进索引项。
func indexStatus(agent agentRecord) indexEntry {
	entry := indexEntry{AgentID: agent.AgentID, Description: agent.Description, Creator: agent.Creator, Status: agent.Status}
	if n := len(agent.Tasks); n > 0 {
		last := agent.Tasks[n-1]
		entry.Result = last.Result
		if last.Error != "" {
			entry.Result = "error: " + last.Error
		}
		entry.Updated = last.TaskID
		if last.Status == taskStatusRunning {
			entry.Status = agentStatusRunning
		}
	}
	return entry
}

func upsertIndex(index *agentIndex, entry indexEntry) {
	for i := range index.Agents {
		if index.Agents[i].AgentID == entry.AgentID {
			index.Agents[i] = entry
			return
		}
	}
	index.Agents = append(index.Agents, entry)
}

// updateAgent 读改写「索引 + 单条 Agent 记录」，乐观并发冲突时整体重试。
// exists 报告记录此前是否已存在；mutate 内可抛 ErrAgentNotFound 等中止写入。
// agentID 为空时只改索引。
func (m *Manager) updateAgent(ctx context.Context, conversationID, agentID, runID string,
	mutate func(index *agentIndex, agent *agentRecord, exists bool) error) error {
	for attempts := 0; attempts < 5; attempts++ {
		index, indexHead, err := m.loadIndex(ctx, conversationID)
		if err != nil {
			return err
		}
		var agent agentRecord
		var agentHead state.Checkpoint
		var exists bool
		if agentID != "" {
			agent, agentHead, exists, err = m.loadAgentRaw(ctx, conversationID, agentID)
			if err != nil {
				return err
			}
		}
		if err := mutate(&index, &agent, exists); err != nil {
			return err
		}
		if agentID != "" {
			if err := m.saveAgent(ctx, conversationID, agent, agentHead, runID); err != nil {
				if errors.Is(err, state.ErrConflict) {
					continue
				}
				return err
			}
			upsertIndex(&index, indexStatus(agent))
		}
		if err := m.saveIndex(ctx, conversationID, index, indexHead); err != nil {
			if errors.Is(err, state.ErrConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("update sub-agent records for %q: persistent version conflicts", agentID)
}

// listForCaller 返回当前创建者可见的子 Agent 摘要。
func (m *Manager) listForCaller(ctx context.Context, conversationID, caller string) ([]AgentSummary, error) {
	index, _, err := m.loadIndex(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	out := make([]AgentSummary, 0, len(index.Agents))
	for _, entry := range index.Agents {
		if entry.Creator != caller {
			continue
		}
		agent, _, err := m.loadAgent(ctx, conversationID, entry.AgentID, caller)
		if err != nil {
			if errors.Is(err, ErrAgentNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, AgentSummary{
			AgentID:     agent.AgentID,
			Description: agent.Description,
			Status:      agent.Status,
			Result:      entry.Result,
			Tasks:       len(agent.Tasks),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AgentID < out[j].AgentID })
	return out, nil
}

// Cleanup 清理属于已放弃运行的子 Agent 记录：父运行 abandon 不会自动回滚
// 独立保存的子 Agent 记录（契约清单 K 节第 7 项）。调用方传入被放弃运行的
// RunID——可由 state.Store 加载会话记录读取——本函数删除该运行创建的子
// Agent，并把其余子 Agent 回退到该运行之前的最后一条记录。返回清理数。
func (m *Manager) Cleanup(ctx context.Context, conversationID, abandonedRunID string) (int, error) {
	if m.cfg.Store == nil || conversationID == "" || abandonedRunID == "" {
		return 0, errors.New("cleanup requires a store, conversation id and run id")
	}
	index, indexHead, err := m.loadIndex(ctx, conversationID)
	if err != nil {
		return 0, err
	}
	var kept []indexEntry
	removed := 0
	for _, entry := range index.Agents {
		key := agentKey(conversationID, entry.AgentID)
		latest, found, lerr := m.cfg.Store.Load(ctx, key)
		if lerr != nil {
			return 0, lerr
		}
		if !found {
			continue
		}
		if latest.RunID != abandonedRunID {
			kept = append(kept, entry)
			continue
		}
		// 沿历史找该运行之前的最后一条记录：有则恢复其内容，无则删除。
		restored, rerr := m.restoreBeforeRun(ctx, conversationID, key, abandonedRunID, latest)
		if rerr != nil {
			return 0, rerr
		}
		removed++
		if restored != nil {
			entry.Status = restored.Status
			entry.Result = ""
			if n := len(restored.Tasks); n > 0 {
				entry.Result = restored.Tasks[n-1].Result
				entry.Updated = restored.Tasks[n-1].TaskID
			}
			kept = append(kept, entry)
		}
	}
	if removed == 0 {
		return 0, nil
	}
	index.Agents = kept
	if err := m.saveIndex(ctx, conversationID, index, indexHead); err != nil {
		return removed, err
	}
	return removed, nil
}

// restoreBeforeRun 把子 Agent 记录回退到指定运行之前的最后一条保存；
// 该运行前没有任何记录时返回 nil（记录应整体删除）。
func (m *Manager) restoreBeforeRun(ctx context.Context, conversationID, key, runID string, latest state.Checkpoint) (*agentRecord, error) {
	records, err := m.cfg.Store.List(ctx, key, state.ListOptions{Limit: 100})
	if err != nil {
		return nil, err
	}
	var base *state.Checkpoint
	for i := range records {
		if records[i].RunID != runID {
			base = &records[i]
			break
		}
	}
	if base == nil {
		if err := m.cfg.Store.Delete(ctx, key); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var agent agentRecord
	if err := json.Unmarshal(base.Result, &agent); err != nil {
		return nil, fmt.Errorf("%w: decode agent history: %v", ErrAgentCorrupt, err)
	}
	if err := m.saveAgent(ctx, conversationID, agent, latest, base.RunID); err != nil {
		return nil, err
	}
	return &agent, nil
}
