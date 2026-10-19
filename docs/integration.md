# 对接说明集

框架与应用的边界：goHarness 保证并发正确、容量可调、可排空、租户
隔离；以下事项属于应用或网关，框架不内置——每一节给出可以照抄的
流程或片段。

- [额度](#额度)
- [会话串行化](#会话串行化)
- [请求级与跨副本限流](#请求级与跨副本限流)
- [消息队列](#消息队列)
- [事件回放](#事件回放)
- [其他存储](#其他存储)
- [其他向量库](#其他向量库)
- [检查点完整性与加密](#检查点完整性与加密)
- [异步任务模型](#异步任务模型)
- [A2A 对接](#a2a-对接)

## 额度

框架不做跨运行计费；在 `tenant.With` 之后、`Bulkhead` 之前插一个额度
检查即可。Redis 计数示例（按租户每日限额）：

```go
// Lua：递增并惰性设过期，超限返回 -1。
var quotaLua = redis.NewScript(`
local n = redis.call("INCR", KEYS[1])
if n == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
if n > tonumber(ARGV[2]) then return -1 end
return n`)

func checkQuota(ctx context.Context, c redis.UniversalClient, tenantID string, limit int64) error {
	key := "quota/" + tenantID + "/" + time.Now().UTC().Format("2006-01-02")
	n, err := quotaLua.Run(ctx, c, []string{key}, 86400*2, limit).Int64()
	if err != nil {
		return err
	}
	if n < 0 {
		return errQuotaExceeded // 映射 429，带 Retry-After
	}
	return nil
}
```

按请求粒度扣（一次运行多次 LLM 调用）时，用 `tool.Middleware` 或模型
装饰器在调用边界扣数；token 计费读事件的 `usage`（`input_tokens`/
`output_tokens`）在运行结束后结算更准确。

## 会话串行化

同一 conversationID 的并发 `Invoke` 框架不排队：恰好一个成功，其余
`ErrUnfinishedRun`/`ErrConflict`。串行化由应用选：

| 做法 | 要点 |
| --- | --- |
| 粘性路由 | 负载均衡按会话 ID hash 路由到固定实例；简单，但实例漂移时失效 |
| Redis 锁 | `SET session-lock:<sid> <token> NX PX 30000`，持锁跑完释放；续租可选 |
| 数据库行锁 | `SELECT ... FOR UPDATE` 按会话行；复用现有事务边界 |
| 队列分区键 | 消息队列按会话 ID 分区，同分区天然串行（见[消息队列](#消息队列)） |

**兜底**：串行化失效时检查点 CAS 保证不出现两份提交——后到保存
收到 `state.ErrConflict`，客户端按 409 重试即可。

## 请求级与跨副本限流

- **进程内**：`llm.NewRateLimitedModel(model, llm.RateLimitConfig{
  MaxConcurrent, MinInterval})` 包一层即可，是装饰器。
- **跨副本**：接你们的 LLM 网关（推荐，网关通常已有配额体系），或
  用 Redis 令牌桶自己实现：

```go
// 简化令牌桶：每分钟 limit 次。
var bucketLua = redis.NewScript(`
local n = redis.call("INCR", KEYS[1])
if n == 1 then redis.call("EXPIRE", KEYS[1], 120) end
return n <= tonumber(ARGV[1]) and 1 or 0`)

func allow(ctx context.Context, c redis.UniversalClient, key string, perMin int64) (bool, error) {
	minute := time.Now().UTC().Unix() / 60
	return bucketLua.Run(ctx, c, []string{"rl/" + key + "/" + strconv.FormatInt(minute, 10)}, perMin).Bool()
}
```

把 `allow` 包进 `tool.Middleware` 或模型装饰器即得跨副本限流。

## 消息队列

入库、长任务、异步运行都适合队列驱动。消费者流程：

```
loop:
  msg := queue.Receive()
  ctx := tenant.With(msg.Tenant, msg.User)
  out, err := assistant.Invoke(ctx, msg.Messages,
      agent.WithCheckpoint(store, msg.ConversationID))
  // 最大尝试次数内重试；超过进死信（队列自带）
  if err == nil || errors.Is(err, state.ErrConflict) { msg.Ack() } // 冲突说明别人在处理
  else if errIsRetryable(err) { msg.Nack() }                       // 重投
  else { msg.DeadLetter() }
```

- **Kafka 分区键**：`conversationID`——同会话消息同分区天然串行，
  配合上面「会话串行化」即可省掉显式锁。
- **断点续跑**：消费者崩溃后同会话消息重投，`Invoke` 撞
  `ErrUnfinishedRun` 时转 `agent.Resume` 或直接交给下一分区消息触发。

## 事件回放

框架的事件流是内存内的（SSE 直出）；「断线重放」由应用补：

- 需要回放时，把 `observe.With` 的消费端或 SSE 出口再写一份到
  Redis Stream / Kafka（`XADD`），客户端断线后用 `Last-Event-ID`
  续读。
- 轻量做法：SSE 输出侧按事件序号缓存最近 N 条（ring buffer），
  断线请求带 `?since=` 续读；运行结束清掉。
- 检查点是事件回放的状态兜底：重连后先 `PendingInterrupts`/`Load`
  拿当前状态，再补事件流。

## 其他存储

`state.Store` 只有五个方法，语义核心是 **Save 的原子 CAS**：校验
`Version`（和已有 `ID` 一致性）→ 追加历史 → 更新最新指针，三步必须
原子。Postgres 思路：

```sql
CREATE TABLE checkpoints (
  conversation_id text NOT NULL,
  checkpoint_id   text NOT NULL,
  version         bigint NOT NULL,
  parent_id       text,
  payload         jsonb NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (conversation_id, checkpoint_id)
);
CREATE TABLE latest (
  conversation_id text PRIMARY KEY,
  checkpoint_id   text NOT NULL,
  version         bigint NOT NULL
);

-- Save：单事务里乐观版本校验 + 追加 + 推指针。
BEGIN;
INSERT INTO checkpoints VALUES ($1,$2,$3,$4,$5);
UPDATE latest SET checkpoint_id=$2, version=$3
 WHERE conversation_id=$1 AND version=$2_expected;  -- 0 行 → ErrConflict
COMMIT;
```

要点：`Load` 走 `latest` 表；`List` 按 `created_at` 分页；`Delete` 删两表；
`RollbackBasisReader`/`ConversationProvider` 等可选接口按需实现
（装饰器通过类型断言透传）。

## 其他向量库

`retrieval` 的四个接口：`VectorStore`（`VectorRequest`：query/upsert/
delete/list）、`KeywordSearch`、`Embedder`、`Reranker`。pgvector 要点：

```go
// 伪代码：VectorStore.Invoke 的分支。
case retrieval.VectorQueryOperation:
    // SELECT id, text, meta, embedding <=> $1 AS score ... ORDER BY score LIMIT k
    // 命名空间是强制前置过滤：WHERE ns = $2 AND (meta @> $3 的声明字段)
case retrieval.VectorUpsertOperation:
    // INSERT ... ON CONFLICT (ns, id) DO UPDATE
case retrieval.VectorDeleteOperation:
    // DELETE WHERE ns = $1 AND (id = ANY($2) 或 $2 为空)
case retrieval.VectorListOperation:
    // SELECT WHERE ns = $1 AND meta @> $2（入库清理用，分页扫描）
```

- 租户：`Scoped` 把 `Namespace` 钉成租户 ID——你的实现要把它当成
  强制 `WHERE` 条件，不是可选项。
- 维度不一致在建表/索引期拒绝（对齐 redisstore `EnsureIndex`）。
- 关键词检索用 `tsvector`/`pg_trgm` 或外接搜索引擎均可，接口相同。

## 检查点完整性与加密

框架默认信任存储返回的内容；需要防篡改时在 `state.Store` 外包一层：

```go
// 思路：payload 序列化后 AES-GCM 加密 + HMAC 覆盖 (id|parent|version|cipher)。
func (s *signedStore) Save(ctx context.Context, cp state.Checkpoint) (state.Checkpoint, error) {
    raw, _ := json.Marshal(cp)          // 或只签敏感字段
    blob := seal(s.key, raw)            // AEAD：密文即完整性证明
    return s.inner.Save(ctx, rewritePayload(cp, blob))
}
```

- 校验放在 `Load`/`Get` 里，篡改记录直接报错而非带病恢复。
- 密钥管理归应用（KMS/信封加密）；租户级密钥可在 `tenant.With` 后
  从作用域取租户 ID 选钥。
- 注意完整性校验会让 CAS 依赖明文版本号——签名时把 `Version`
  一并覆盖进 MAC。

## 异步任务模型

「整次运行异步」由应用提供：同步接口收到请求 → 生成任务 ID → 入队 →
立即返回任务 ID → 后台消费者跑 `Invoke`/`Resume` → 状态查询接口读
检查点。

```
POST /tasks            → {task_id}（检查点键用任务 ID 做会话键）
GET  /tasks/{id}       → Load(sessionKey(taskID)) 得状态：
                         无记录=排队中 / 有 pending=待输入(带问题) / 完成
POST /tasks/{id}/input → 对挂起运行 Resume + WithResume(questionID, input)
```

- 运行状态从检查点读：`PendingInterrupts` 给出等待的问题，
  `GraphStateCheckpoint` 带节点进度。
- 失败重试交给队列的死信策略；恢复语义由检查点保证。
- 这正是[消息队列](#消息队列)一节消费者流程的另一个视角。

## A2A 对接

**远端 Agent 当工具**：把对端 HTTP 调用包成 `tool.NewFunctionTool`，
用 `subagent.AsTool` 的思路做上下文裁剪——只把任务描述发给远端，
不转本地历史。远端长任务用它的任务 ID 轮询，本地侧用
`graph.Interrupt` 挂起等回调，对异步协议天然契合。

**对外暴露自己的 Agent**：包一个薄 HTTP 层（参考服务
`extensions/examples/server` 就是这个形态），约定请求/响应 schema；
把 `agent.Invoke`/`Resume` 映射到对端协议的任务提交/输入回灌两个动词；
中断对应对方的 input-required 状态。
