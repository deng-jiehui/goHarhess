package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// Tools 返回模型可调用的子 Agent 管理工具，放进父 Agent 的 Tools 即可。
// run_subagent 一次性运行不落记录；spawn_agent/followup_task/list/get/close
// 管会话子 Agent，需要带检查点的运行（state.FromContext 可解析会话）。
func (m *Manager) Tools() []tool.Tool {
	return []tool.Tool{
		tool.NewFunctionTool(runSpec, m.runOnce),
		tool.NewFunctionTool(spawnSpec, m.spawn),
		tool.NewFunctionTool(followupSpec, m.followup),
		tool.NewFunctionTool(listSpec, m.list),
		tool.NewFunctionTool(getSpec, m.get),
		tool.NewFunctionTool(closeSpec, m.close),
	}
}

func objectSchema(required []string, properties map[string]any) map[string]any {
	if required == nil {
		required = []string{}
	}
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}

func strProp(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

var (
	runSpec = tool.Spec{
		Name:        ToolRun,
		Description: "Run a self-contained task with a sub-agent and return its result when it finishes. The sub-agent is ephemeral: it leaves no session record and cannot receive follow-up tasks. Use spawn_agent for an agent you want to reuse.",
		Parameters: objectSchema([]string{"task"}, map[string]any{
			"task":         strProp("The concrete task and the expected deliverable."),
			"instructions": strProp("Optional extra responsibilities for this sub-agent."),
			"model":        strProp("Optional model name; omit to use the default."),
			"tools":        map[string]any{"type": []string{"array", "null"}, "items": map[string]any{"type": "string"}, "description": "Optional tool names; omit for the default set, an empty array means no tools."},
			"role":         strProp("Optional role template name."),
		}),
	}
	spawnSpec = tool.Spec{
		Name:        ToolSpawn,
		Description: "Create a session sub-agent, run its first task and return the result when it finishes. The agent keeps its context across runs; use followup_task for more work, list_agents/get_agent to inspect it, close_agent to retire it.",
		Parameters: objectSchema([]string{"task"}, map[string]any{
			"task":         strProp("The first task and the expected deliverable."),
			"instructions": strProp("Optional extra responsibilities for this sub-agent."),
			"name":         strProp("Optional short label that helps you recognize this sub-agent later."),
			"model":        strProp("Optional model name; omit to use the default."),
			"tools":        map[string]any{"type": []string{"array", "null"}, "items": map[string]any{"type": "string"}, "description": "Optional tool names; omit for the default set, an empty array means no tools."},
			"role":         strProp("Optional role template name."),
		}),
	}
	followupSpec = tool.Spec{
		Name:        ToolFollowup,
		Description: "Give an idle session sub-agent a new task and return its result when it finishes. The agent keeps its identity and remembers its earlier work. Fails while it still has an unfinished task or after it was closed.",
		Parameters: objectSchema([]string{"agent_id", "task"}, map[string]any{
			"agent_id": strProp("Agent id returned by spawn_agent or list_agents."),
			"task":     strProp("The new task and expected deliverable."),
		}),
	}
	listSpec = tool.Spec{
		Name:        ToolList,
		Description: "List the session sub-agents you created in this conversation with their status and latest result.",
		Parameters:  objectSchema(nil, map[string]any{}),
		IsReadOnly:  true,
	}
	getSpec = tool.Spec{
		Name:        ToolGet,
		Description: "Read one session sub-agent's full task history, including earlier results.",
		Parameters: objectSchema([]string{"agent_id"}, map[string]any{
			"agent_id": strProp("Agent id."),
		}),
		IsReadOnly: true,
	}
	closeSpec = tool.Spec{
		Name:        ToolClose,
		Description: "Close an idle session sub-agent so it no longer accepts new tasks. Its saved results stay readable.",
		Parameters: objectSchema([]string{"agent_id"}, map[string]any{
			"agent_id": strProp("Agent id."),
		}),
	}
)

func stringArg(args map[string]any, key string) (string, bool) {
	value, ok := args[key].(string)
	return value, ok
}

func requiredString(args map[string]any, key string) (string, error) {
	value, _ := stringArg(args, key)
	if strings.TrimSpace(value) == "" {
		return "", configErrorf("%s is required and cannot be empty", key)
	}
	return value, nil
}

// optionalTools 区分"省略"与"明确的空集合"：缺省或 null 返回 nil，空数组返回非 nil 空切片。
func optionalTools(args map[string]any) ([]string, error) {
	raw, present := args["tools"]
	if !present || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, configErrorf("tools must be an array of tool names")
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		name, ok := item.(string)
		if !ok {
			return nil, configErrorf("tools must contain tool names")
		}
		out = append(out, name)
	}
	return out, nil
}

func resolveArgs(m *Manager, args map[string]any) (task string, resolved ResolvedSpec, err error) {
	task, err = requiredString(args, "task")
	if err != nil {
		return "", ResolvedSpec{}, err
	}
	tools, err := optionalTools(args)
	if err != nil {
		return "", ResolvedSpec{}, err
	}
	req := Request{Task: task, Tools: tools}
	req.Instructions, _ = stringArg(args, "instructions")
	req.Model, _ = stringArg(args, "model")
	req.Role, _ = stringArg(args, "role")
	resolved, err = m.Resolve(req)
	if err != nil {
		return "", ResolvedSpec{}, err
	}
	return strings.TrimSpace(task), resolved, nil
}

// taskInput 取登记时冻结的任务输入；找不到任务或旧记录缺失 Input 时
// 按当前上下文经 Codec 重建（尽力而为，不保证跨重放一致）。
func (m *Manager) taskInput(agent agentRecord, taskID, task string) ([]schema.Message, error) {
	for _, t := range agent.Tasks {
		if t.TaskID == taskID && len(t.Input) > 0 {
			var messages []schema.Message
			if err := json.Unmarshal(t.Input, &messages); err != nil {
				return nil, fmt.Errorf("%w: decode task input: %v", ErrAgentCorrupt, err)
			}
			return messages, nil
		}
	}
	return m.cfg.Codec.Input(agent.Context, task)
}

// runOnce 一次性子 Agent：不落会话记录，结果直接返回。
func (m *Manager) runOnce(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	task, resolved, err := resolveArgs(m, in.Args)
	if err != nil {
		return modelError(err)
	}
	input, err := m.cfg.Codec.Input(nil, task)
	if err != nil {
		return tool.Result{}, err
	}
	result, _, err := m.execute(ctx, resolved, "", input)
	if err != nil {
		if errors.As(err, new(*ConfigError)) {
			return modelError(err)
		}
		return tool.Result{}, err
	}
	return tool.DataResult(map[string]any{"status": taskStatusCompleted, "result": result}), nil
}

// spawn 建会话记录并同步跑首个任务。AgentID/TaskID 由调用幂等键派生：
// 挂起恢复后重放本调用时记录与任务项已存在，不重复建 Agent、不重复登记，
// 嵌套运行从其命名空间子记录继续。
func (m *Manager) spawn(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	conversationID, runID, err := m.sessionScope(ctx, in)
	if err != nil {
		return modelError(err)
	}
	task, resolved, err := resolveArgs(m, in.Args)
	if err != nil {
		return modelError(err)
	}
	if _, err := m.build(ctx, resolved); err != nil {
		return buildFailure(ctx, err)
	}
	name, _ := stringArg(in.Args, "name")
	agentID := callID("sa-", in.IdempotencyKey)
	if in.IdempotencyKey == "" {
		// 无幂等键的裸调用（测试直接 Invoke 等）：用任务内容兜底派生。
		agentID = callID("sa-", conversationID+"/"+runID+"/"+task)
	}
	taskID := callID("t-", agentID+"/first")
	caller := callerID(ctx)
	// 首任务输入随登记冻结：新记录无上下文；重放本调用时输入逐字节一致。
	input, err := m.freezeInput(nil, task)
	if err != nil {
		return tool.Result{}, err
	}

	err = m.updateAgent(ctx, conversationID, agentID, runID, func(index *agentIndex, rec *agentRecord, exists bool) error {
		if exists {
			if rec.Creator != caller {
				return ErrAgentNotFound
			}
			return nil // 重放：记录与任务项已登记，直接续跑
		}
		*rec = agentRecord{
			AgentID: agentID, Description: describe(name, resolved.Role, task), Creator: caller,
			OwnerRunID: runID, BuilderID: resolved.BuilderID, BuilderVersion: resolved.BuilderVersion,
			Config: savedConfigOf(resolved), Status: agentStatusRunning,
			Tasks: []taskRecord{{TaskID: taskID, Task: task, RunID: runID, Input: input, Status: taskStatusRunning}},
		}
		return nil
	})
	if err != nil {
		return modelError(err)
	}

	return m.settle(ctx, conversationID, agentID, caller, runID, taskID, resolved, task)
}

// settle 运行登记的任务并结算记录；spawn 与 followup 共用。
// 输入取登记时冻结的字节：同一调用重放必须用逐字节一致的输入进入嵌套
// 子执行（acquireChild 按输入一致性认领记录），登记后的上下文更新不再
// 影响本次调用。挂起、取消等控制信号原样上交。
func (m *Manager) settle(ctx context.Context, conversationID, agentID, caller, runID, taskID string,
	resolved ResolvedSpec, task string) (tool.Result, error) {
	agent, _, err := m.loadAgent(ctx, conversationID, agentID, caller)
	if err != nil {
		return tool.Result{}, err
	}
	input, err := m.taskInput(agent, taskID, task)
	if err != nil {
		return tool.Result{}, err
	}
	result, exported, runErr := m.execute(ctx, resolved, agentID, input)
	failed := runErr != nil && !hasControlSignal(runErr)
	if runErr != nil && hasControlSignal(runErr) {
		return tool.Result{}, runErr
	}
	status := taskStatusCompleted
	if failed {
		status = taskStatusFailed
	}
	err = m.updateAgent(ctx, conversationID, agentID, runID, func(index *agentIndex, rec *agentRecord, exists bool) error {
		if !exists {
			return ErrAgentNotFound
		}
		for i := range rec.Tasks {
			if rec.Tasks[i].TaskID == taskID {
				rec.Tasks[i].Status = status
				if failed {
					rec.Tasks[i].Error = runErr.Error()
				} else {
					rec.Tasks[i].Result = result
				}
			}
		}
		if !failed {
			rec.Context = exported
		}
		rec.Status = agentStatusIdle
		return nil
	})
	if err != nil {
		return tool.Result{}, err
	}
	if failed {
		return tool.Result{}, runErr
	}
	return tool.DataResult(map[string]any{
		"agent_id": agentID, "task_id": taskID, "status": taskStatusCompleted, "result": result,
	}), nil
}

// followup 给空闲的会话子 Agent 追加任务：沿用记录配置与上下文重建执行器。
func (m *Manager) followup(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	conversationID, runID, err := m.sessionScope(ctx, in)
	if err != nil {
		return modelError(err)
	}
	agentID, err := requiredString(in.Args, "agent_id")
	if err != nil {
		return modelError(err)
	}
	task, err := requiredString(in.Args, "task")
	if err != nil {
		return modelError(err)
	}
	if m.cfg.MaxTaskRunes > 0 && len([]rune(task)) > m.cfg.MaxTaskRunes {
		return modelError(configErrorf("task is too long, the limit is %d characters", m.cfg.MaxTaskRunes))
	}
	caller := callerID(ctx)
	taskID := callID("t-", in.IdempotencyKey+"/task")
	if in.IdempotencyKey == "" {
		taskID = callID("t-", conversationID+"/"+runID+"/"+agentID+"/"+task)
	}

	// 先校验保存配置并完成构建，再登记任务：登记即把 Agent 置为
	// running，构建/校验在登记后失败会让记录停留在 running、后续任务
	// 被 ErrAgentBusy 永久拒绝。重放本调用时记录已含该任务，登记分支
	// 直接放行，settle 从命名空间子记录续跑。
	var agent agentRecord
	agent, _, err = m.loadAgent(ctx, conversationID, agentID, caller)
	if err != nil {
		return modelError(err)
	}
	resolved, err := m.specFromSaved(agent.Config, agent.BuilderID, agent.BuilderVersion)
	if err != nil {
		return tool.Result{}, err
	}
	if _, err := m.build(ctx, resolved); err != nil {
		return buildFailure(ctx, err)
	}
	err = m.updateAgent(ctx, conversationID, agentID, runID, func(index *agentIndex, rec *agentRecord, exists bool) error {
		if !exists || rec.Creator != caller {
			return ErrAgentNotFound
		}
		if rec.Status == agentStatusClosed {
			return ErrAgentClosed
		}
		for _, t := range rec.Tasks {
			if t.Status == taskStatusRunning && t.TaskID != taskID {
				return ErrAgentBusy
			}
		}
		for i := range rec.Tasks {
			if rec.Tasks[i].TaskID == taskID {
				return nil // 重放：任务已登记，直接续跑
			}
		}
		// 任务输入随登记冻结：基于本任务之前的上下文，重放间逐字节一致。
		input, ierr := m.freezeInput(rec.Context, task)
		if ierr != nil {
			return ierr
		}
		rec.Tasks = append(rec.Tasks, taskRecord{TaskID: taskID, Task: task, RunID: runID, Input: input, Status: taskStatusRunning})
		rec.Status = agentStatusRunning
		return nil
	})
	if err != nil {
		return modelError(err)
	}
	return m.settle(ctx, conversationID, agentID, caller, runID, taskID, resolved, task)
}

func (m *Manager) list(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	conversationID, _, err := m.sessionScope(ctx, in)
	if err != nil {
		return modelError(err)
	}
	agents, err := m.listForCaller(ctx, conversationID, callerID(ctx))
	if err != nil {
		return modelError(err)
	}
	if agents == nil {
		agents = []AgentSummary{}
	}
	return tool.DataResult(map[string]any{"agents": agents}), nil
}

func (m *Manager) get(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	conversationID, _, err := m.sessionScope(ctx, in)
	if err != nil {
		return modelError(err)
	}
	agentID, err := requiredString(in.Args, "agent_id")
	if err != nil {
		return modelError(err)
	}
	agent, _, err := m.loadAgent(ctx, conversationID, agentID, callerID(ctx))
	if err != nil {
		return modelError(err)
	}
	return tool.DataResult(map[string]any{
		"agent_id": agent.AgentID, "description": agent.Description, "status": agent.Status,
		"creator": agent.Creator, "tasks": agent.Tasks,
	}), nil
}

func (m *Manager) close(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	conversationID, runID, err := m.sessionScope(ctx, in)
	if err != nil {
		return modelError(err)
	}
	agentID, err := requiredString(in.Args, "agent_id")
	if err != nil {
		return modelError(err)
	}
	err = m.updateAgent(ctx, conversationID, agentID, runID, func(index *agentIndex, rec *agentRecord, exists bool) error {
		if !exists || rec.Creator != callerID(ctx) {
			return ErrAgentNotFound
		}
		for _, t := range rec.Tasks {
			if t.Status == taskStatusRunning {
				return ErrAgentBusy
			}
		}
		rec.Status = agentStatusClosed
		return nil
	})
	if err != nil {
		return modelError(err)
	}
	return tool.DataResult(map[string]any{"agent_id": agentID, "status": agentStatusClosed}), nil
}
