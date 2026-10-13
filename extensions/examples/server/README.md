# 参考服务（extensions/examples/server）

标准库 `net/http` + SSE，把 goHarness 组件按推荐顺序组装的示例：
认证（示例 token）→ `tenant.With` → `tenant.NewStore` 装饰 → Agent →
`limits.Bulkhead` → SSE 输出（`stream.Config.SendTimeout` 保护）。

**只作演示，不是官方服务端。** 模型是本地脚本模型（回显输入、含
`approve` 时发起审批挂起），无需外部模型服务。默认内存检查点存储。

## 启动

```sh
cd extensions
go run ./examples/server
# 切到真实 Redis（会话、检查点、幂等键共用）：
GOHARNESS_REDIS_ADDR=127.0.0.1:6379 go run ./examples/server
```

本地起 Redis（任选其一）：

```sh
# Docker
docker run -d --name goharness-redis -p 6379:6379 redis:7
# OTel Collector（接入真实 trace/metrics 导出时）：
docker run -d --name otel -p 4318:4318 otel/opentelemetry-collector
```

不提供 compose 文件（设计 7.4）；接入真实导出时在请求 ctx 上并列挂
`otel.With(tracer)` 与 `otel.Metrics(meter, …)`，指标清单见
`docs/运维指标与告警.md`。

## 示例认证

`Authorization: Bearer <tenant>/<user>` —— 只演示字段流转，真实部署
换成 JWT 校验后提取 `tool.Subject{TenantID, ID}`。Store 经
`tenant.NewStore(.., tenant.StrictUser())` 装饰，会话 key 强制带
`t/<租户>/u/<用户>/` 前缀，跨租户与同租户跨用户访问都在存储层拒绝。

## 接口

| 方法与路径 | 作用 |
| --- | --- |
| `POST /v1/sessions/{sid}/runs` | `{"message": "..."}`；`Accept: text/event-stream` 时 SSE |
| `POST /v1/sessions/{sid}/resume` | `{"question_id": "...", "answer": "..."}` 恢复挂起 |
| `DELETE /v1/sessions/{sid}` | 删除会话及关联数据 |
| `GET /healthz`、`GET /readyz` | 存活与就绪；排空期间 `readyz` 失败 |

```sh
H='-H Authorization: Bearer acme/u1'
curl -X POST localhost:8080/v1/sessions/s1/runs $H -d '{"message":"hello"}'
# {"output":"echo: hello"}

# 审批挂起 → 202 + question_id → resume
curl -X POST localhost:8080/v1/sessions/s2/runs $H -d '{"message":"please approve deploy"}'
curl -X POST localhost:8080/v1/sessions/s2/resume $H \
    -d '{"question_id":"<id>","answer":"yes"}'
# {"output":"approved -> yes"}

# SSE
curl -N -X POST localhost:8080/v1/sessions/s3/runs $H \
    -H 'Accept: text/event-stream' -d '{"message":"stream me"}'
```

## 错误到状态码（具体方案 7.3）

| 条件 | 状态码 |
| --- | --- |
| 认证缺失/作用域非法 | 401 |
| 会话 key 越出租户作用域 | 403 |
| `state.ErrConflict` / `ErrUnfinishedRun` / `ErrRunAbandoned` | 409 |
| 挂起等待输入 | 202（body 带 question_id 与问题） |
| `limits.ErrOverloaded` / `ErrDraining` | 503（过载带 `Retry-After`） |
| `graph.ErrCheckpointRequired` | 500 |
| `stream.ErrSlowConsumer` | 关闭连接并记日志 |
| 客户端取消 | 无响应 |

## 停机排空

收到 `SIGINT/SIGTERM`：`readyz` 先失败（负载均衡摘流）→
`Bulkhead.Drain` 用 `control.RequestInterrupt` 逐个取消在途运行并等待
落盘 → `server.Shutdown` 关连接。开检查点的运行进度已保存，另一实例
用同一会话 `Resume` 可续跑；未开检查点的运行只会被取消。

## 压测

```sh
# 另开终端：同会话争用应恰一个成功
go run ./examples/loadtest -store redis -redis 127.0.0.1:6379 \
    -requests 8 -concurrency 8 -same-session
```
