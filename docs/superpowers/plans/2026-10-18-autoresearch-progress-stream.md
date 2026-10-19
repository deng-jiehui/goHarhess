# AutoResearch 中间研究内容流式展示实施计划

> **For agentic workers:** 按任务逐项实施；每项先写失败测试，再写最小实现并运行对应验证。

**Goal:** 让 AutoResearch 在 Graph 执行过程中把经过筛选的研究进度和阶段产物推送到会话页面，并支持断线后从持久事件继续显示。

**Architecture:** Graph 节点在业务边界发布带 `autoresearch.progress` 标记的公开快照；worker 使用 goHarness 的 `StreamEvents`/`ResumeStreamEvents` 消费公共流，只持久化进度事件和最终答复增量，过滤理解/起草节点的内部 JSON。HTTP SSE 按事件游标增量发送，React 页面用事件触发快照刷新并将进度卡和答复增量分别渲染。

**Tech Stack:** Go 1.24、goHarness `pkg/graph`/`pkg/stream`、SQLite、HTTP SSE、React 19、TypeScript、Vitest。

## Global Constraints

- 仅公开业务进度、阶段产物摘要和最终答复增量，不公开提示词、内部 JSON 或模型推理。
- 进度事件必须可持久化、可按 `SessionEvent.ID` 续读，并能在刷新和服务重启后重建。
- 正式会话消息只在 Graph 成功完成后提交；中间进度不进入下一轮模型历史。
- 保留现有运行详情和 `runtime.*` 诊断事件，不改变模型配置和项目笔记 4000 字约束。
- 使用中文注释；Go 代码遵循现有格式，前端保持现有测试和构建命令。

### Task 1: 定义公开研究进度事件

**Files:**
- Create: `extensions/examples/auto-research/internal/app/research_progress.go`
- Test: `extensions/examples/auto-research/internal/app/research_progress_test.go`

**Interfaces:**
- Produces `ResearchProgress`, `ProgressMessage`, `parseProgressMessage` and a rune-safe `progressSummary` helper。
- Progress payload uses stable `id`/`stage`/`status`/`title`/`summary` fields and is embedded under `schema.Message.Extra["autoresearch.progress"]`。

- [x] 写测试：进度快照能被识别、未知 Extra 被拒绝、摘要按 Unicode 字符截断。
- [x] 运行目标测试并确认进度类型测试通过。
- [x] 实现进度类型、快照构造和解析函数。
- [x] 再运行同一测试，确认通过。

### Task 2: 在业务 Graph 边界发布阶段产物

**Files:**
- Modify: `extensions/examples/auto-research/internal/app/research_graph.go`
- Test: `extensions/examples/auto-research/internal/app/research_graph_test.go`

**Interfaces:**
- Graph 节点通过 `stream.PublishSnapshots` 发布进度消息；不改变 State、路由和最终 Output。
- 至少覆盖读取上下文、目标理解、范围确认、阶段草稿、产物检查、笔记保存和边界说明。

- [x] 写测试：使用 `StreamEvents` 执行 fixture 图时，能收到进度消息且消息带业务阶段摘要。
- [x] 运行目标测试并确认 Graph 进度事件测试通过。
- [x] 在节点提交业务状态后发布进度；长草稿只发布有界摘要。
- [x] 运行 Graph 测试，确认进度事件和既有路径都通过。

### Task 3: worker 消费公共流并持久化公开事件

**Files:**
- Modify: `extensions/examples/auto-research/internal/app/turns.go`
- Modify: `extensions/examples/auto-research/internal/domain/runtime.go`（仅补充事件常量/文档）
- Test: `extensions/examples/auto-research/internal/app/turns_test.go`

**Interfaces:**
- `runTurn` 改用 `StreamEvents`/`ResumeStreamEvents` 并持续 drain reader。
- 新增 `research.progress` 和 `assistant.delta` 事件；只接受允许的 progress Extra 和 `compose_reply` 节点消息。
- 最终 result 仍走现有正式消息提交和 `turn.completed`。

- [x] 写集成测试：执行结束前事件列表已包含 `research.progress`，且不包含 `understand_goal`/`draft_*` 原始模型消息。
- [x] 运行目标测试并确认公共事件测试通过。
- [x] 实现公共流消费、事件编码、过滤和错误/中断处理。
- [x] 运行应用包测试，确认恢复、笔记幂等和运行观测不回归。

### Task 4: SSE 增量发送与前端进度卡

**Files:**
- Modify: `extensions/examples/auto-research/internal/transport/httpapi/turns.go`
- Modify: `extensions/examples/auto-research/web/src/api/client.ts`
- Modify: `extensions/examples/auto-research/web/src/types.ts`
- Modify: `extensions/examples/auto-research/web/src/pages/SessionPage.tsx`
- Modify: `extensions/examples/auto-research/web/src/styles.css`
- Test: `extensions/examples/auto-research/internal/transport/httpapi/turns_test.go`
- Test: `extensions/examples/auto-research/web/src/pages/SessionPage.test.ts`

**Interfaces:**
- SSE 接受 `after` 或 `Last-Event-ID`，活动轮期间保持连接，终态后关闭；普通 JSON 查询保持兼容。
- 页面以 SSE 事件触发刷新，保留轮询作为断线兜底；进度卡按阶段稳定 ID 更新，不生成重复消息。
- `assistant.delta` 仅在活动轮期间显示为临时答复，完成后由正式消息替换。

- [x] 写 HTTP、前端解析和去重测试，确认 SSE、解析和去重行为通过。
- [x] 实现 SSE 长轮询/事件游标、客户端订阅、进度卡和临时答复展示。
- [x] 运行 Go 测试、前端单元测试、类型检查、生产构建。

### Task 5: 文档和端到端验收

**Files:**
- Modify: `extensions/examples/auto-research/phases/02-agent-runtime/IMPLEMENTATION.md`
- Modify: `extensions/examples/auto-research/phases/02-agent-runtime/DESIGN.md`
- Test: `extensions/examples/auto-research/web/e2e/workspace.spec.ts`

- [x] 更新阶段二状态和公开事件边界，明确调试事件与用户进度的区别。
- [x] 增加浏览器验收：发送目标后先看到进度卡，完成后看到正式答复，刷新不重复。
- [x] 运行完整后端、前端和浏览器验证，记录实际结果。
