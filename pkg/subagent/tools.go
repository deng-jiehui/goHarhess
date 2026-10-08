package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// 模型可见的管理工具名称。
const (
	ToolSpawn    = "spawn_agent"
	ToolWait     = "wait_agents"
	ToolMessage  = "send_message"
	ToolCancel   = "cancel_agent"
	ToolFollowup = "followup_task"
	ToolList     = "list_agents"
	ToolGet      = "get_agent"
	ToolClose    = "close_agent"
)

// Tools 返回模型可调用的子 Agent 管理工具，放进父 Agent 的 Tools 即可；
// 无需让 Agent 配方实现新的调度器。工具通过当前调用上下文取得运行时
// 服务，只能访问本会话中调用者自己创建的子 Agent。
func (m *Manager) Tools() []tool.Tool {
	return []tool.Tool{
		tool.NewFunctionTool(spawnSpec, m.spawn),
		tool.NewFunctionTool(waitSpec, m.wait),
		tool.NewFunctionTool(messageSpec, m.message),
		tool.NewFunctionTool(cancelSpec, m.cancel),
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
	spawnSpec = tool.Spec{
		Name:        ToolSpawn,
		Description: "Create a sub-agent for a self-contained task and start it. Returns its agent_id and task_id immediately without waiting; use wait_agents to collect the result.",
		Parameters: objectSchema([]string{"task"}, map[string]any{
			"task":         strProp("The concrete task and the expected deliverable."),
			"instructions": strProp("Optional extra responsibilities for this sub-agent."),
			"name":         strProp("Optional short label that helps you recognize this sub-agent later."),
			"model":        strProp("Optional model name; omit to use the default."),
			"tools":        map[string]any{"type": []string{"array", "null"}, "items": map[string]any{"type": "string"}, "description": "Optional tool names; omit for the default set, an empty array means no tools."},
			"role":         strProp("Optional role template name."),
		}),
	}
	waitSpec = tool.Spec{
		Name:        ToolWait,
		Description: "Wait until the listed tasks of sub-agents you created in this run have finished and return each task's status and final message.",
		Parameters: objectSchema([]string{"task_ids"}, map[string]any{
			"task_ids": map[string]any{"type": "array", "minItems": 1, "items": map[string]any{"type": "string"}, "description": "Task ids returned by spawn_agent or followup_task."},
		}),
	}
	messageSpec = tool.Spec{
		Name:        ToolMessage,
		Description: "Send an extra message to a task that is still running; it reaches the sub-agent at its next model call. It never starts new work and cannot answer an approval request.",
		Parameters: objectSchema([]string{"task_id", "message"}, map[string]any{
			"task_id": strProp("Task id of a running task."),
			"message": strProp("The message to deliver."),
		}),
	}
	cancelSpec = tool.Spec{
		Name:        ToolCancel,
		Description: "Cancel a running task and any work it started. The sub-agent stays available for new tasks.",
		Parameters: objectSchema([]string{"task_id"}, map[string]any{
			"task_id": strProp("Task id to cancel."),
		}),
	}
	followupSpec = tool.Spec{
		Name:        ToolFollowup,
		Description: "Give an idle sub-agent a new task. It keeps its identity and remembers its earlier work. Fails while it still has an unfinished task or after it was closed.",
		Parameters: objectSchema([]string{"agent_id", "task"}, map[string]any{
			"agent_id": strProp("Agent id returned by spawn_agent or list_agents."),
			"task":     strProp("The new task and expected deliverable."),
		}),
	}
	listSpec = tool.Spec{
		Name:        ToolList,
		Description: "List the sub-agents you created in this conversation with their status and a summary of the latest result.",
		Parameters:  objectSchema(nil, map[string]any{}),
		IsReadOnly:  true,
	}
	getSpec = tool.Spec{
		Name:        ToolGet,
		Description: "Read one sub-agent's full task history, including earlier results.",
		Parameters: objectSchema([]string{"agent_id"}, map[string]any{
			"agent_id": strProp("Agent id."),
		}),
		IsReadOnly: true,
	}
	closeSpec = tool.Spec{
		Name:        ToolClose,
		Description: "Close an idle sub-agent so it no longer accepts new tasks. Its saved results stay readable.",
		Parameters: objectSchema([]string{"agent_id"}, map[string]any{
			"agent_id": strProp("Agent id."),
		}),
	}
)

// modelError 把模型可修正的错误转成模型可见的错误结果；框架错误、挂起信号
// 与其他意外错误原样返回，由工具节点上交调度器处理。
// hasControlSignal 判断错误链是否携带框架错误或中断、挂起控制信号——
// 合并错误中只要存在一个，整条错误就必须上交，不能被模型可修正的
// 配置反馈遮蔽。
func hasControlSignal(err error) bool {
	if control.AsFramework(err) {
		return true
	}
	if _, ok := control.AsInterrupt(err); ok {
		return true
	}
	if _, ok := control.AsSuspension(err); ok {
		return true
	}
	return false
}

func modelError(err error) (tool.Result, error) {
	switch {
	case IsConfigError(err) && !hasControlSignal(err),
		errors.Is(err, graph.ErrAgentNotFound),
		errors.Is(err, graph.ErrAgentBusy),
		errors.Is(err, graph.ErrAgentClosed),
		errors.Is(err, graph.ErrManagedNotFound),
		errors.Is(err, graph.ErrManagedScope),
		errors.Is(err, graph.ErrManagedTerminal):
		return tool.ErrorResult(schema.NewTextBlock(friendly(err))), nil
	}
	return tool.Result{}, err
}

func friendly(err error) string {
	switch {
	case IsConfigError(err):
		return configMessage(err)
	case errors.Is(err, graph.ErrManagedNotFound), errors.Is(err, graph.ErrManagedScope):
		return fmt.Sprintf("%v. Only tasks created in the current run can be waited on, messaged or cancelled; use get_agent to read earlier results.", err)
	case errors.Is(err, graph.ErrManagedTerminal):
		return fmt.Sprintf("%v. The task has already finished; use followup_task to give the sub-agent new work.", err)
	}
	return err.Error()
}

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
			return nil, configErrorf("tools must be an array of tool names")
		}
		out = append(out, name)
	}
	return out, nil
}

func encodeTask(task string) (json.RawMessage, error) {
	return json.Marshal(taskInput{Task: task})
}

func (m *Manager) spawn(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	task, err := requiredString(in.Args, "task")
	if err != nil {
		return modelError(err)
	}
	tools, err := optionalTools(in.Args)
	if err != nil {
		return modelError(err)
	}
	req := Request{Task: task, Tools: tools}
	req.Instructions, _ = stringArg(in.Args, "instructions")
	req.Model, _ = stringArg(in.Args, "model")
	req.Role, _ = stringArg(in.Args, "role")
	resolved, err := m.Resolve(req)
	if err != nil {
		return modelError(err)
	}
	// 登记前先构建：构建失败的任务不进入任务清单，也不留下半成品子 Agent。
	if _, err := m.build(ctx, resolved); err != nil {
		return buildFailure(ctx, err)
	}
	config, err := json.Marshal(savedConfigOf(resolved))
	if err != nil {
		return tool.Result{}, err
	}
	task = strings.TrimSpace(task)
	input, err := encodeTask(task)
	if err != nil {
		return tool.Result{}, err
	}
	name, _ := stringArg(in.Args, "name")
	ref, err := graph.SpawnAgent(ctx, graph.ManagedSpec{
		Builder: resolved.BuilderID, Version: resolved.BuilderVersion, Config: config, Input: input,
	}, describe(name, resolved.Role, task))
	if err != nil {
		return modelError(err)
	}
	return tool.DataResult(map[string]any{"agent_id": ref.AgentID, "task_id": ref.TaskID, "status": graph.ManagedStatusRunning}), nil
}

// describe 生成子 Agent 的职责说明：优先用模型给的名称，其次角色，最后任务摘要。
func describe(name, role, task string) string {
	switch {
	case strings.TrimSpace(name) != "":
		return truncate(strings.TrimSpace(name), 80)
	case role != "":
		return role
	}
	return truncate(task, 80)
}

// buildFailure 按错误类别处理创建前的构建失败：模型可修正的配置错误作为
// 错误结果返回；框架错误（恢复不兼容、检查点错误、接入错误等）以及挂起、
// 取消等控制信号原样上交，不能被转换成普通工具结果而让根运行"成功"结束；
// 其余构建失败对模型只给出泛化提示，完整诊断随错误链保留给回调与日志。
func buildFailure(ctx context.Context, err error) (tool.Result, error) {
	// 先判断框架错误与控制信号：错误链里只要有一个，就整体上交——与同时存在的
	// 配置错误的先后顺序无关，不能让模型可修正的反馈掩盖必须交给调用方的错误。
	if hasControlSignal(err) || ctx.Err() != nil ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return tool.Result{}, err
	}
	// 模型可见文本只取配置错误自己声明的 Message，外层包装或合并错误里的内部
	// 诊断不外发；完整原错误留在错误链里，交给工具错误处理、日志和回调。
	visible := "the sub-agent could not be created with this configuration"
	var config *ConfigError
	if errors.As(err, &config) {
		visible = config.Message
	}
	return tool.Result{}, errors.Join(
		tool.ModelVisible(errors.New(visible)),
		fmt.Errorf("build sub-agent: %w", err),
	)
}

// configMessage 取出配置错误声明的模型可见文本，不含外层包装的内容。
func configMessage(err error) string {
	var config *ConfigError
	if errors.As(err, &config) {
		return config.Message
	}
	return err.Error()
}

func truncate(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	return string([]rune(text)[:limit]) + "…"
}

func taskIDs(args map[string]any) ([]string, error) {
	list, ok := args["task_ids"].([]any)
	if !ok || len(list) == 0 {
		return nil, configErrorf("task_ids must be a non-empty array of task ids")
	}
	ids := make([]string, 0, len(list))
	for _, item := range list {
		id, ok := item.(string)
		if !ok || strings.TrimSpace(id) == "" {
			return nil, configErrorf("task_ids must contain non-empty strings")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// resultText 把任务结果还原为模型可读文本：字符串结果取其文本，其余保留 JSON。
func resultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func (m *Manager) wait(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	ids, err := taskIDs(in.Args)
	if err != nil {
		return modelError(err)
	}
	// 未全部结束时返回挂起信号：调用任务释放槽位，子任务完成后重放求值。
	results, err := graph.WaitManaged(ctx, ids)
	if err != nil {
		return modelError(err)
	}
	items := make([]map[string]any, len(results))
	for i, r := range results {
		items[i] = map[string]any{"task_id": r.TaskID, "agent_id": r.AgentID, "status": r.Status, "result": resultText(r.Result), "error": r.Error}
	}
	return tool.DataResult(map[string]any{"results": items}), nil
}

func (m *Manager) message(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	id, err := requiredString(in.Args, "task_id")
	if err != nil {
		return modelError(err)
	}
	text, err := requiredString(in.Args, "message")
	if err != nil {
		return modelError(err)
	}
	messageID, err := graph.SendManagedMessage(ctx, id, text)
	if err != nil {
		return modelError(err)
	}
	return tool.DataResult(map[string]any{"message_id": messageID, "status": "queued"}), nil
}

func (m *Manager) cancel(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	id, err := requiredString(in.Args, "task_id")
	if err != nil {
		return modelError(err)
	}
	result, err := graph.CancelManaged(ctx, id)
	if err != nil {
		return modelError(err)
	}
	return tool.DataResult(map[string]any{"task_id": result.TaskID, "agent_id": result.AgentID, "status": result.Status}), nil
}

func (m *Manager) followup(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	agentID, err := requiredString(in.Args, "agent_id")
	if err != nil {
		return modelError(err)
	}
	task, err := requiredString(in.Args, "task")
	if err != nil {
		return modelError(err)
	}
	if m.cfg.MaxTaskRunes > 0 && utf8.RuneCountInString(task) > m.cfg.MaxTaskRunes {
		return modelError(configErrorf("task is too long, the limit is %d characters", m.cfg.MaxTaskRunes))
	}
	input, err := encodeTask(strings.TrimSpace(task))
	if err != nil {
		return tool.Result{}, err
	}
	ref, err := graph.FollowupAgent(ctx, agentID, input)
	if err != nil {
		return modelError(err)
	}
	return tool.DataResult(map[string]any{"agent_id": ref.AgentID, "task_id": ref.TaskID, "status": graph.ManagedStatusRunning}), nil
}

func (m *Manager) list(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
	agents, err := graph.ListAgents(ctx)
	if err != nil {
		return modelError(err)
	}
	if agents == nil {
		agents = []graph.AgentSummary{}
	}
	return tool.DataResult(map[string]any{"agents": agents}), nil
}

func (m *Manager) get(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	agentID, err := requiredString(in.Args, "agent_id")
	if err != nil {
		return modelError(err)
	}
	info, err := graph.GetAgent(ctx, agentID)
	if err != nil {
		return modelError(err)
	}
	tasks := make([]map[string]any, len(info.Tasks))
	for i, task := range info.Tasks {
		tasks[i] = map[string]any{
			"task_id": task.TaskID, "status": task.Status, "task": inputTask(task.Input),
			"result": resultText(task.Result), "error": task.Error,
		}
	}
	return tool.DataResult(map[string]any{
		"agent_id": info.AgentID, "description": info.Description, "status": info.Status,
		"active_task": info.ActiveTask, "tasks": tasks,
	}), nil
}

// inputTask 从冻结输入里取出任务文本，供历史视图展示。
func inputTask(raw json.RawMessage) string {
	var input taskInput
	if json.Unmarshal(raw, &input) != nil {
		return ""
	}
	return input.Task
}

func (m *Manager) close(ctx context.Context, in tool.CallInput) (tool.Result, error) {
	agentID, err := requiredString(in.Args, "agent_id")
	if err != nil {
		return modelError(err)
	}
	info, err := graph.CloseAgent(ctx, agentID)
	if err != nil {
		return modelError(err)
	}
	return tool.DataResult(map[string]any{"agent_id": info.AgentID, "status": info.Status}), nil
}
