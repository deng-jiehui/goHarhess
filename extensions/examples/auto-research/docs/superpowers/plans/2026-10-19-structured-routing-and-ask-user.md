# 结构化路由与 ask_user 改造实施计划

**目标：** 业务路线由模型的严格结构化输出决定，用户追问由模型调用真实 `ask_user` 工具发起；恢复时继续原业务节点，不重跑全局路由。

**边界：** 本次实施刚确认的路由和交互闭环，复用现有七个工作区工具、笔记保存和前端可编辑选项。学术工具、实验工具、项目笔记工具目录及 shell 审批扩展仍按后续独立任务推进。只修改 AutoResearch，不修改框架，不调用真实付费模型，不改本地模型密钥和现有用户数据。

**执行方式：** 本会话内按 test-driven-development 实施，保留工作区现有修改。用户已要求开始代码改造，无须再次确认。

## 文件与职责

| 文件 | 职责 |
| --- | --- |
| `internal/app/research_routing.go` | 结构化路由解码与允许值校验，最多一次格式修复 |
| `internal/app/interaction_tools.go` | ask_user 定义、问题参数校验、Graph 中断与回答返回 |
| `internal/app/research_graph.go` | 接线：路由 → 业务模型 → ask_user → 原业务模型；工作区工具共享交互能力 |
| `internal/app/research_state.go` | 保存完整模型调用、返回节点、回答记录与计数；升级 StateCodec |
| `internal/app/turns.go` | 统一图定义版本；旧 pending 版本显式拒绝自动套用新图 |
| `internal/app/research_interaction_test.go` | 真实编译图下的动态提问、跨重建恢复、校验与无副作用回归 |
| `internal/app/research_graph_test.go`、`turns_test.go` | 将旧路由工具与关键词回退测试改为新协议 |
| `web/e2e/workspace.spec.ts` | 验证真实中断选项、编辑、恢复与保存 |

## 任务 1：先固定行为

- [x] 新增图集成测试：focus 范围为空但模型不追问时直接完成。
- [x] 新增图集成测试：模型生成非代码缺陷主题的 ask_user；问题与选项原样进入 pending。
- [x] 新增恢复测试：重建编译图后回答，只返回原起草节点，路由只执行一次，保存意图和用户自定义答案不丢失。
- [x] 新增协议测试：未知路线不按关键词回退；格式错误最多修复一次；路由请求不携带函数定义。
- [x] 新增工作区测试：ask_user 可用；混合提问与写入调用整批拒绝，文件不发生变化。
- [x] 运行 `go test ./internal/app -run 'TestResearchInteraction' -count=1`，确认这些行为在旧实现下失败。

## 任务 2：实现并接线

- [x] 路由节点使用 `llm.NewStructuredOutput[GoalDecision]` 本地校验；兼容仅包裹 JSON 的 Markdown 围栏；真实模型协议错误不再调用关键词决策器。
- [x] 将 ask_user 注册为 `tool.Tool`，接收 question 和可选 choices，校验长度与重复项；通过 `graph.Interrupt` 返回真实用户答案。
- [x] 起草模型和工作区模型按需获得 ask_user 定义；先提交模型响应与调用身份，后进入独立 clarify 工具节点。
- [x] clarify 恢复后追加配对工具结果，返回已保存的原节点；限定提问轮数，并将确认答案保留在当前状态与最终消息元数据中。
- [x] 删除固定 scope_gate 追问与 clarify → understand_goal 接线；纯路由只读取 State。
- [x] 升级定义和状态版本，拒绝用新图恢复旧版本未完成轮次。

## 任务 3：回归与文档

- [x] 更新离线 fixture，使演示也经过真实 ask_user 工具，关键词逻辑只保留在 fixture。
- [x] 更新既有测试到结构化路线协议；保持已有笔记、工作区读写与流式事件行为。
- [x] 执行 `go test ./...`、`go vet ./...`；执行前端单元测试、类型检查和浏览器 E2E。
- [x] 更新阶段文档中的当前行为、测试步骤与未覆盖范围；最终报告真实验证结果。

## 本轮验证结果

2026-10-19：go test ./... 与 go vet ./... 通过；前端单元测试 20 项通过，类型检查和构建通过；Playwright 浏览器测试 11 项通过。浏览器测试使用独立临时数据库与 fixture，未调用真实模型或修改用户密钥。新增测试另覆盖连续追问、SQLite 正式消息中的答案保留、旧图版本拒绝、非法批次无副作用和追问预算。
