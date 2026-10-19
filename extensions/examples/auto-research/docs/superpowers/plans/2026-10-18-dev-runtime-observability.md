# 开发运行详情面板实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**目标：** 在开发模式下展示 AutoResearch 当前 Turn 的 Graph 节点、模型调用、耗时、Token 用量和错误，使后台中途过程可以被定位。

**架构：** 复用 goHarness `observe.Event`，在 TurnService 执行 Graph 时挂载一个异步事件观察器，把脱敏后的生命周期摘要写入现有 `events` 表。现有会话快照已经返回事件，前端开发模式通过快照轮询读取这些事件，并在独立的运行详情面板中计算指标和显示时间线；正式会话消息仍只显示业务答复。

**技术栈：** Go 1.24、goHarness `pkg/observe`、SQLite 事件表、React + TypeScript、Vitest、Playwright。

## 全局约束

- 只修改 `extensions/examples/auto-research`。
- 不保存 prompt、模型回复、API Key 或事件 Payload；只保存生命周期、节点名称、模型元数据、耗时、Token 用量和错误摘要。
- 运行详情只在 Vite 开发模式显示；生产构建可通过 `?dev=1` 显式打开。
- 复用现有 `SessionSnapshot.events`，不新增数据库表和不改变正式消息投影。
- 每个实现改动先写失败测试并确认失败，再写最小实现。

---

### 任务 1：定义脱敏运行事件摘要

**文件：**

- 创建：`internal/app/runtime_observer.go`
- 测试：`internal/app/runtime_observer_test.go`

**接口：**

- 消费：`observe.Event`、`domain.RuntimeRepository.AppendEvent`。
- 产生：`newRuntimeEventObserver(runtime, scope, sessionID, turnID, attempt)`，以及稳定的 `runtime.<kind>` 事件和 JSON 摘要。

- [x] 写测试 `TestRuntimeEventPayloadOmitsPayloadAndSerializesMetrics`：构造带 Payload、Attrs、Usage 和错误的 `observe.Event`，调用编码函数，确认 JSON 没有 `payload`，包含 `kind`、`phase`、`name`、`duration_ms`、`usage` 和错误摘要。
- [x] 运行 `go test ./internal/app -run TestRuntimeEventPayloadOmitsPayloadAndSerializesMetrics -count=1`，确认因函数不存在而失败。
- [x] 实现事件摘要类型和 Observer；Observer 只复制允许的字段，事件名使用 `runtime.` 前缀，DB 写入失败记录到进程日志但不阻断 Graph。
- [x] 重新运行该测试并确认通过。

### 任务 2：把 Graph/LLM 生命周期接入 Turn 执行

**文件：**

- 修改：`internal/app/turns.go`
- 测试：`internal/app/turns_test.go`

**接口：**

- 消费：任务 1 的 Observer、当前 Turn 的 scope/session/attempt。
- 产生：每次 Turn 的 `runtime.run`、`runtime.node`、`runtime.llm` 事件，并且首次执行和恢复执行都被观测。

- [x] 添加 `TestTurnServicePersistsRuntimeEvents`：用 fixture 模型运行一个只读问答 Turn，断言事件中至少包含 `runtime.run`、`runtime.node` 和 `runtime.llm`。
- [x] 运行该测试，确认当前代码因没有运行事件而失败。
- [x] 在 `runTurn` 创建有界异步 Observer，把执行上下文包装为 `observe.With`，在 Invoke/Resume 返回前关闭并排空观察器；不捕获 Payload。
- [x] 重新运行 `go test ./internal/app -run 'TestTurnServicePersistsRuntimeEvents|TestTurnServiceFocusInterruptResumeAndIdempotency' -count=1`。

### 任务 3：定义前端事件类型和指标计算

**文件：**

- 修改：`web/src/types.ts`
- 修改：`web/src/pages/SessionPage.tsx`
- 测试：`web/src/pages/SessionPage.test.ts`

**接口：**

- 消费：`SessionSnapshot.events` 的 `payload_json`。
- 产生：`parseRuntimeEvent`、`runtimeMetrics`，供运行详情组件展示。

- [x] 添加测试：Base64/JSON 事件可解析；指标正确统计模型调用数、节点数、Token 总数和错误数；未知事件不会使页面崩溃。
- [x] 运行前端单测，确认新函数尚不存在而失败。
- [x] 实现安全解析和指标聚合，只读取 `runtime.*` 事件；耗时统一显示为毫秒/秒，错误和模型名可见，Payload 不渲染。
- [x] 重新运行 `npm test -- --run src/pages/SessionPage.test.ts`。

### 任务 4：开发模式运行详情界面

**文件：**

- 创建：`web/src/components/RuntimeDetails.tsx`
- 修改：`web/src/pages/SessionPage.tsx`
- 修改：`web/src/styles.css`
- 测试：`web/e2e/workspace.spec.ts`

**接口：**

- 消费：任务 3 的解析和指标函数、当前 `snapshot.events`、Turn 状态。
- 产生：会话头部“开发运行详情”按钮、指标卡片、当前节点、生命周期时间线和可展开错误详情。

- [x] 添加 Playwright 场景：开发模式显示按钮；打开面板后能看到模型调用/节点指标和事件时间线；普通消息区域不出现内部 Payload。
- [x] 运行该场景确认因按钮和面板不存在而失败。
- [x] 实现 `RuntimeDetails` 弹窗；`import.meta.env.DEV` 或 URL `dev=1` 才显示入口；执行中面板随 1.5 秒快照轮询更新，完成/失败后保留最后一次详情。
- [x] 更新样式并重新运行该 E2E 场景。

### 任务 5：整体验证与文档说明

**文件：**

- 修改：`README.md`
- 修改：`phases/02-agent-runtime/ACCEPTANCE.md`（如存在对应验收段落）

- [x] 在 README 增加开发调试步骤：开发模式打开“开发运行详情”，关注节点、LLM 调用、耗时、Token 和错误；说明不会显示 Prompt/回复 Payload。
- [x] 运行完整验证：

```powershell
go test -count=1 ./...
go vet ./...
npm --prefix web test -- --run
npm --prefix web run typecheck
npm --prefix web run build
npm --prefix web run test:e2e -- workspace.spec.ts
```

- [x] 检查 `git status`，确认改动只位于 `extensions/examples/auto-research`，不提交、不推送、不部署。
