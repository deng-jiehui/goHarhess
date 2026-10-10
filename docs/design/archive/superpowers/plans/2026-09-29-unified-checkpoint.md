# 统一检查点实施计划

目标：按架构报告 1.5，将会话、执行进度和结果交给同一存储，以同步提交保障恢复边界。

采用 Go 标准库与现有 Redis 客户端。沿用当前工作区，保留此前未提交的改动；不保留旧字节存储接口，不双写，不迁移旧检查点。按测试先行的顺序在本会话实施。

- [x] 存储：扩展 `pkg/state` 的检查点及 Store，增加历史 ID、父 ID、运行状态、执行数据和结果。Memory 在同一锁内完成版本检查、历史追加和最新指针更新；Redis 用 Lua 完成相同操作。测试冲突、历史分页、删除、TTL、旧格式拒绝。
- [x] Graph：`continuation.go`、`runtime_execute.go`、`interrupt_input.go` 改用统一 Store。初始入口、节点成功路由、中断和完成时同步提交；完成时保存结果；恢复读取最新记录。测试普通失败、保存失败、最终结果复用以及 Invoke/Stream 一致性。
- [x] Agent：`state.go`、`options.go` 和配方 State 纳入完整会话；恢复沿用已保存输入与 Values；移除 `OnComplete` 与生命周期中的二次提交。测试多轮消息、裁剪不损坏完整历史、人工恢复和并发版本冲突。
- [x] 迁移旧测试存储、示例、Redis 实现及使用文档，明确同会话串行执行和外部副作用可能重放的边界。
- [x] 执行核心模块、extensions 模块测试，条件允许时执行 race；检查改动并更新报告实施状态。

验证命令：`go test ./pkg/state ./pkg/graph ./pkg/agent ./internal/agentrecipe`；`go test ./...`；在 extensions 下执行 `go test ./...`。

验证结果：核心与 extensions 全量测试通过。使用用户指定的 MSYS2 UCRT64 GCC，在沙箱外补跑核心模块与 extensions 的 `go test -race ./... -count=1 -timeout=120s`，均通过。编译器 PATH 仅对测试进程生效，未修改系统配置。
