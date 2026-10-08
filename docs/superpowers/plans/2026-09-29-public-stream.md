# 公共运行事件流实施记录

目标：以真正消息增量和公共运行流统一消息、活动、生命周期及最终结果的消费。

架构：schema 负责增量协议；stream 负责有界汇集和公开 Reader；Graph 接入节点和子图范围；
Agent 在自身提交完成后提供最终结果。节点之间继续使用原来的 Runnable/StreamReader。

实施单元：

- [x] schema/delta_protocol：明确开始、内容块、追加、元数据和完成操作；测试重复文本及工具参数校验。
- [x] stream：事件类型、发布作用域、活动句柄、有界队列、包装 Reader；测试关闭、错误排空和取消。
- [x] llm：wireAccumulator 映射 Provider 索引；同一增量生成最终结果；轻量重试进度信号。
- [x] graph：CompiledGraph.StreamEvents，节点来源、子图范围、可选生命周期。
- [x] agent：包级 StreamEvents；复用 Agent.Stream 的会话提交边界。
- [x] cache：已完成缓存输出快照；实时 miss 独立执行并写缓存。
- [x] docs/examples：公开消费说明与无需外部服务的 JSON 输出示例。

兼容决策：不删除旧公开类型，旧消费方式保持可用；公共流不借助旧快照回调转发。
暂不包括 SSE 服务、断线重放、任意自定义负载、Graph 状态 updates/values。

验收：核心和扩展模块测试与 vet；嵌套结果唯一性、失败后不重试、缓存快照测试；
增量/累计快照局部基准；竞态检查在可用 C 编译器的环境执行。
