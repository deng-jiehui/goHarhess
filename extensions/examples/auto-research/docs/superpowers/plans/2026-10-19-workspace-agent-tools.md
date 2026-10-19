# AutoResearch 工作区工具 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 AutoResearch 应用层增加接近 Codex/Claude Code 的工作区工具，让 Agent 能读取项目、受控执行命令，并在用户确认后修改代码。

**Architecture:** 不修改 `pkg/graph`、`pkg/llm` 或 `pkg/tool`。应用层新增工作区策略、文件/命令/写入工具，并通过现有 `tool.NewFunctionTool`、`tool.NewToolsNode` 和 Graph 节点组成一个有限的 `model → tools → model` 工作区子图。研究主图仍由代码决定入口、检查、确认和结束条件；模型只选择允许的工具及其参数。

**Tech Stack:** Go 1.24、goHarness 现有 Graph / LLM / Tool / Stream / Checkpoint、Windows/Linux 可配置 shell、SQLite 运行事件、React 现有工具卡片。

## Global Constraints

- 所有实现只放在 `extensions/examples/auto-research/`，不修改 goHarness 框架。
- 工具从可信的 `ResearchGraphDeps` 和 `context.Context` 获得项目与用户身份，不能相信模型传入的 project/user ID。
- 所有路径必须解析到项目工作区根目录下；拒绝 `..`、绝对越界路径和越界符号链接。
- 读取工具默认只读；命令工具默认受控；写入工具必须先获得用户确认。
- 工具输出限制大小、超时时间和调用次数；超限返回模型可见的结构化错误，并记录诊断事件。
- API Key、环境变量密钥、完整环境和未脱敏命令输出不得写入消息、检查点或业务表。
- 先写失败测试，再写最小实现；验证使用项目自己的 Go/前端命令，不清理工作区既有改动。

## Tool Set

首期提供三组共七个工具：

| 组 | 工具 | 权限 | 说明 |
| --- | --- | --- | --- |
| 文件 | `list_directory` | 只读 | 列出工作区内目录，默认深度 2，跳过 `.git`、`node_modules`、构建产物 |
| 文件 | `read_file` | 只读 | 按相对路径读取文本，支持行范围和字节上限，二进制文件返回类型提示 |
| 文件 | `search_files` | 只读 | 在工作区内按字面量或正则搜索，返回文件、行号和截断后的片段 |
| 文件 | `git_diff` | 只读 | 查看指定范围的工作树 diff，默认仅返回统计和受限片段 |
| 命令 | `run_shell` | 受控 | 在工作区内执行配置允许的 Bash/PowerShell 命令，返回退出码、stdout/stderr 摘要 |
| 写入 | `apply_patch` | 需确认 | 校验统一补丁、展示预览、确认后应用；不支持删除仓库外文件 |
| 写入 | `write_file` | 需确认 | 以相对路径和期望哈希写入完整文本，避免覆盖并发修改 |

`run_shell` 首期允许只读检查、测试和构建命令；网络、进程控制、依赖安装、删除、重命名和未知命令进入确认。`apply_patch` 是代码 Agent 的主要修改入口，`write_file` 用于新增或完整重写小文件；删除工具、远程 SSH 和自动提交暂不加入。

### Task 1: 工作区策略与安全边界

**Files:**
- Create: `extensions/examples/auto-research/internal/app/workspace_policy.go`
- Create: `extensions/examples/auto-research/internal/app/workspace_policy_test.go`
- Modify: `extensions/examples/auto-research/internal/app/research_graph.go`

**Interfaces:**
- `type WorkspacePolicy struct { Root string; Shell string; MaxReadBytes int; MaxOutputBytes int; MaxCommandDuration time.Duration; AutoApproveReadOnly bool }`
- `func (p WorkspacePolicy) ResolvePath(relative string) (string, error)`
- `func (p WorkspacePolicy) ValidateCommand(command, cwd string) error`
- `func (p WorkspacePolicy) IsMutation(command string) bool`

- [ ] **Step 1: Write failing tests** for empty root, `..`, absolute escape, symlink escape, missing cwd, disallowed command, timeout above the configured maximum, and valid nested paths.
- [ ] **Step 2: Run the focused Go test** and confirm the new policy functions are absent or fail.
- [ ] **Step 3: Implement path normalization and command policy** with `filepath.Abs`, `filepath.Rel`, `EvalSymlinks` where available, a shell command allowlist, and explicit mutation classification. Keep error text safe for model output.
- [ ] **Step 4: Run focused tests** and add Windows path cases (`C:\...`, slash and backslash forms).
- [ ] **Step 5: Wire policy into `ResearchGraphDeps`** as an optional application dependency; no framework API changes.

### Task 2: 文件工具

**Files:**
- Create: `extensions/examples/auto-research/internal/app/workspace_tools.go`
- Create: `extensions/examples/auto-research/internal/app/workspace_tools_test.go`

**Interfaces:**
- `func NewWorkspaceReadTools(policy WorkspacePolicy) []tool.Tool`
- Typed arguments: `ListDirectoryArgs{Path string; Depth int}`, `ReadFileArgs{Path string; StartLine, EndLine int}`, `SearchFilesArgs{Query, Path, Glob string; Regex bool; MaxResults int}`, `GitDiffArgs{Path, Base string; MaxBytes int}`.
- Results include relative paths, line numbers, hashes where useful, `truncated` and `next_hint` fields.

- [ ] **Step 1: Write failing tests** for valid reads, line slicing, UTF-8 text, binary rejection, hidden/build directory filtering, literal/regex search, result truncation, and diff path scoping.
- [ ] **Step 2: Run focused tests** and confirm failure.
- [ ] **Step 3: Implement the four read-only tools** with `tool.New`/`tool.ReadOnly`, the shared path policy, bounded reads, deterministic ordering, and no shell invocation for file reads.
- [ ] **Step 4: Add safe `git diff` execution** through `exec.CommandContext` with fixed argument construction; reject caller-supplied shell syntax and paths outside the root.
- [ ] **Step 5: Verify tool schemas** by constructing `tool.NewToolsNode` and checking definitions contain only the intended arguments.

### Task 3: 受控 Shell 工具

**Files:**
- Modify: `extensions/examples/auto-research/internal/app/workspace_tools.go`
- Create: `extensions/examples/auto-research/internal/app/workspace_shell_test.go`
- Modify: `extensions/examples/auto-research/internal/app/research_progress.go`

**Interfaces:**
- `type RunShellArgs struct { Command string; CWD string; TimeoutMS int; Reason string }`
- `func NewWorkspaceShellTool(policy WorkspacePolicy, publish func(context.Context, ResearchProgress) error) tool.Tool`

- [ ] **Step 1: Write failing tests** for allowed `go test`/`npm test`, invalid cwd, blocked deletion/network/install commands, timeout, non-zero exit, output truncation, and cancellation.
- [ ] **Step 2: Run focused tests** and confirm failure.
- [ ] **Step 3: Implement bounded command execution** with `exec.CommandContext`, configured shell selection, sanitized environment, timeout clamping, max output, exit code, and `truncated` metadata.
- [ ] **Step 4: Emit progress snapshots** before start, during long output at bounded intervals, and after completion; never emit API keys or complete environment values.
- [ ] **Step 5: Mark mutation commands as confirmation-required** and return a stable pending action instead of executing them automatically.

### Task 4: 写入与确认工具

**Files:**
- Modify: `extensions/examples/auto-research/internal/app/workspace_tools.go`
- Create: `extensions/examples/auto-research/internal/app/workspace_write_test.go`
- Modify: `extensions/examples/auto-research/internal/app/research_state.go`

**Interfaces:**
- `type WriteFileArgs struct { Path, Content, ExpectedSHA256 string; Create bool }`
- `type ApplyPatchArgs struct { Patch, ExpectedSHA256 string }`
- `type PendingWorkspaceChange struct { Tool, Paths []string; Diff string; ConfirmationKey string }`
- `func NewWorkspaceWriteTools(policy WorkspacePolicy, requestConfirmation func(context.Context, PendingWorkspaceChange) (bool, error)) []tool.Tool`

- [ ] **Step 1: Write failing tests** for hash mismatch, patch check failure, path escape, confirmation rejection, successful patch, new file creation, and write idempotency on checkpoint replay.
- [ ] **Step 2: Run focused tests** and confirm failure.
- [ ] **Step 3: Implement `apply_patch`** using a temporary patch file and `git apply --check`; generate a bounded diff preview before confirmation and apply only after approval.
- [ ] **Step 4: Implement `write_file`** with expected hash checking, atomic temporary-file replacement, UTF-8 validation, size limits, and confirmation.
- [ ] **Step 5: Persist only the confirmation key, paths, hash and result status** in `ResearchState`; never persist full secret-bearing command output.

### Task 5: Graph 工作区子图

**Files:**
- Modify: `extensions/examples/auto-research/internal/app/research_graph.go`
- Modify: `extensions/examples/auto-research/internal/app/research_state.go`
- Create: `extensions/examples/auto-research/internal/app/workspace_graph_test.go`
- Modify: `extensions/examples/auto-research/phases/02-agent-runtime/GRAPH_DESIGN.md`

**Interfaces:**
- `type WorkspaceRunState struct { Messages []schema.Message; Rounds, Calls int; Pending *PendingWorkspaceChange; Completed bool }`
- `func addWorkspaceSubgraph(...) error`

- [ ] **Step 1: Write failing graph tests** for model-selected read calls, multiple read calls, tool result returned to the model, bounded loop termination, unknown tool rejection, and write confirmation interruption.
- [ ] **Step 2: Run focused graph tests** and confirm failure.
- [ ] **Step 3: Add a `workspace` route** to `select_research_route` and a dedicated `workspace_model` node whose request includes only the registered workspace tool definitions.
- [ ] **Step 4: Add a `workspace_tools` `ToolsNode`** using existing `tool.NewToolsNode`; append assistant calls and tool results to state, then route back to `workspace_model` only while the round/call budget permits.
- [ ] **Step 5: Add `workspace_gate` and `workspace_finish`** so code controls completion, confirmation and failure. Model tool calls never choose `finish`, `persist_note`, or another business edge.
- [ ] **Step 6: Ensure checkpoint/recovery reuses stable call IDs and pending confirmation keys** and does not repeat an already completed write.
- [ ] **Step 7: Document the distinction** between model-selected tools and code-controlled business edges in `GRAPH_DESIGN.md`.

### Task 6: 事件与前端展示

**Files:**
- Modify: `extensions/examples/auto-research/internal/app/runtime_observer.go`
- Modify: `extensions/examples/auto-research/web/src/components/ToolCallCard.tsx` (if the existing component needs fields)
- Modify: `extensions/examples/auto-research/web/src/types.ts` (only for new event fields)
- Create or modify focused frontend tests beside the affected components.

- [ ] **Step 1: Write failing tests** for tool start/result/error, command output truncation, pending confirmation and write completion.
- [ ] **Step 2: Emit sanitized tool metadata** through the existing event/SSE projection; keep raw model JSON and secrets out of messages.
- [ ] **Step 3: Render file path, command, duration, exit status, result preview and confirmation controls** with collapsed long output.
- [ ] **Step 4: Verify a tool failure remains visible and the final assistant answer does not claim an unexecuted write.**

### Task 7: 集中验证

- [ ] Run `go test ./...` in `extensions/examples/auto-research`.
- [ ] Run `npm run test`, `npm run typecheck`, and `npm run build` in `extensions/examples/auto-research/web`.
- [ ] Run `npm run test:e2e` and verify these flows: list/read/search, allowed test command, blocked command, patch confirmation, rejected confirmation, successful patch, and recovery after a pending action.
- [ ] Run `git diff --check` and review that only `extensions/examples/auto-research/` changed.
- [ ] Record that this validates local tool policy and graph behavior; it does not prove arbitrary shell safety or remote server execution.

