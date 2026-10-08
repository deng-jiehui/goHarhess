package redisstore

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goharness/goharness/pkg/schema"
	corestate "github.com/goharness/goharness/pkg/state"

	"github.com/redis/go-redis/v9"
)

const DefaultRedisPrefix = "goharness:checkpoint"

// 主 hash 保存最新指针、不可变记录与日志元数据；消息日志按代次存放在
// 同哈希槽的独立 LIST key（{id} 标签），保存脚本一次原子完成 CAS、前缀
// 校验、增量追加、代次分叉与记录登记。
// 客户端发送全量消息摘要 + 增量消息体；脚本用 sha1hex 逐条比对求出真实
// 共享边界 k，记录内 messages/messages_from/messages_epoch 三处占位符
// 由脚本以校验结果替换——记录体字节始终由调用方编码，不做 cjson 往返。
// ARGV: expected_version, record_json, ttl_ms, new_version, expected_head,
// new_id, claimed, nhashes, hash_1..hash_n, msg_{c+1}..msg_n, maxhistory
// 返回: {1, new_version, shared, epoch}；{0, actual} CAS 冲突；
// {2, actual_shared} 客户端乐观前缀失配，收缩 claimed 后重试。
var saveCheckpointScript = redis.NewScript(`
local key = KEYS[1]
local mkey = KEYS[2]
local actual = redis.call('HGET', key, 'version') or '0'
local head = redis.call('HGET', key, 'head') or ''
if actual ~= ARGV[1] or head ~= ARGV[5] then
 return {0, actual}
end
local claimed = tonumber(ARGV[7])
local nhashes = tonumber(ARGV[8])
local mstart = 9 + nhashes
local epoch = tonumber(redis.call('HGET', key, 'epoch') or '0')
local shared = 0
local stored = {}
local loglen = 0
if epoch > 0 then
 -- LLEN 取真实日志长度：LRANGE 按 nhashes 截断，用 #stored 判断
 -- 会漏掉「调用方截断历史后又改写」场景的分叉。
 loglen = redis.call('LLEN', mkey .. ':' .. epoch)
 stored = redis.call('LRANGE', mkey .. ':' .. epoch, 0, nhashes - 1)
 local n = math.min(#stored, nhashes)
 for i = 1, n do
  if redis.sha1hex(stored[i]) == ARGV[8 + i] then shared = i else break end
 end
end
if shared < claimed then
 return {2, shared}
end
-- 增量消息参数按客户端 claimed 边界编码；真实共享边界 shared 之后的
-- 部分才是要写入日志与记录的新消息。
local tail = shared - claimed
local dlen = #ARGV - mstart
if epoch == 0 then
 epoch = 1
 redis.call('HSET', key, 'epoch', '1', 'epochs', '[1]')
elseif loglen > shared and shared < nhashes then
 -- 已存日志在共享前缀之后仍有消息且本次有新增：代次分叉，新代次
 -- 复制共享前缀，旧代次列表保留供历史记录物化。
 -- shared 为 0 时跳过前缀复制：LRANGE 0,-1 会误取整份旧日志。
 if shared > 0 then
  local prefix = redis.call('LRANGE', mkey .. ':' .. epoch, 0, shared - 1)
  for i = 1, #prefix do redis.call('RPUSH', mkey .. ':' .. epoch + 1, prefix[i]) end
 end
 epoch = epoch + 1
 local epochs = cjson.decode(redis.call('HGET', key, 'epochs') or '[]')
 epochs[#epochs + 1] = epoch
 redis.call('HSET', key, 'epoch', tostring(epoch), 'epochs', cjson.encode(epochs))
end
local parts = {}
for i = tail + 1, dlen do
 local m = ARGV[mstart + i - 1]
 redis.call('RPUSH', mkey .. ':' .. epoch, m)
 parts[#parts + 1] = m
end
local delta = '[' .. table.concat(parts, ',') .. ']'
-- 函数形式替换避免消息内容中的 % 被解释为捕获引用。
local record = string.gsub(ARGV[2], '"@@GH_M@@"', function() return delta end, 1)
record = string.gsub(record, '"@@GH_F@@"', tostring(shared), 1)
record = string.gsub(record, '"@@GH_E@@"', tostring(epoch), 1)
-- 运行轮换：新记录 run_id 与当前头部不同时，把头部记录留作本轮
-- 放弃运行的回退依据；它独立于历史裁剪保留。
local prev = redis.call('HGET', key, 'latest')
if prev then
 local okp, prec = pcall(cjson.decode, prev)
 local okn, nrec = pcall(cjson.decode, record)
 if okp and okn and prec['run_id'] ~= nrec['run_id'] then
  redis.call('HSET', key, 'rollback', prev)
 end
end
redis.call('HSET', key, 'version', ARGV[4], 'head', ARGV[6], 'latest', record, 'record:' .. ARGV[6], record)
local ids = cjson.decode(redis.call('HGET', key, 'order') or '[]')
ids[#ids + 1] = ARGV[6]
local maxhistory = tonumber(ARGV[#ARGV])
if maxhistory and maxhistory > 0 and #ids > maxhistory then
 local drop = #ids - maxhistory
 for i = 1, drop do redis.call('HDEL', key, 'record:' .. ids[i]) end
 local kept = {}
 for i = drop + 1, #ids do kept[#kept + 1] = ids[i] end
 ids = kept
 -- 代次回收：删除不再被任何保留记录引用的消息日志。
 local used = {}
 for i = 1, #ids do
  local raw = redis.call('HGET', key, 'record:' .. ids[i])
  if raw then
   local ok, rec = pcall(cjson.decode, raw)
   if ok and rec['messages_epoch'] then used[tostring(rec['messages_epoch'])] = true end
  end
 end
 used[tostring(epoch)] = true
 -- 回退依据引用的日志代次同样不可回收。
 local rb = redis.call('HGET', key, 'rollback')
 if rb then
  local okr, rrec = pcall(cjson.decode, rb)
  if okr and rrec['messages_epoch'] then used[tostring(rrec['messages_epoch'])] = true end
 end
 local epochs = cjson.decode(redis.call('HGET', key, 'epochs') or '[]')
 local alive = {}
 for i = 1, #epochs do
  local e = tostring(epochs[i])
  if used[e] then
   alive[#alive + 1] = epochs[i]
  else
   redis.call('DEL', mkey .. ':' .. e)
  end
 end
 redis.call('HSET', key, 'epochs', cjson.encode(alive))
end
redis.call('HSET', key, 'order', cjson.encode(ids))
local ttl = tonumber(ARGV[3])
local epochs = cjson.decode(redis.call('HGET', key, 'epochs') or '[]')
if ttl and ttl > 0 then
 redis.call('PEXPIRE', key, ttl)
 for i = 1, #epochs do redis.call('PEXPIRE', mkey .. ':' .. tostring(epochs[i]), ttl) end
else
 redis.call('PERSIST', key)
 for i = 1, #epochs do redis.call('PERSIST', mkey .. ':' .. tostring(epochs[i])) end
end
return {1, ARGV[4], shared, epoch}
`)

// RedisStoreConfig 配置 Redis 检查点存储。MaxHistory 为每会话保留的
// 历史记录上限，0 表示不裁剪。
type RedisStoreConfig struct {
	Client     redis.UniversalClient
	Prefix     string
	TTL        time.Duration
	MaxHistory int
}

// RedisStore persists checkpoints across processes with an atomic Lua CAS.
type RedisStore struct {
	client     redis.UniversalClient
	prefix     string
	ttl        time.Duration
	maxHistory int
	// mu 保护 logs 客户端视图缓存；缓存只作乐观前缀猜测，正确性由
	// 保存脚本的原子校验保证，失配按脚本报告的边界重试。
	mu   sync.Mutex
	logs map[string]*logView
}

// logView 缓存一次读取或保存得到的消息日志代次与逐条摘要。
type logView struct {
	epoch  int
	hashes []string
}

var _ corestate.Store = (*RedisStore)(nil)

func NewRedisStore(config RedisStoreConfig) (*RedisStore, error) {
	if config.Client == nil {
		return nil, errors.New("redis checkpoint store requires a client")
	}
	if config.TTL < 0 {
		return nil, errors.New("redis checkpoint TTL cannot be negative")
	}
	if config.MaxHistory < 0 {
		return nil, errors.New("checkpoint MaxHistory cannot be negative")
	}
	prefix := strings.TrimSpace(config.Prefix)
	if prefix == "" {
		prefix = DefaultRedisPrefix
	}
	return &RedisStore{client: config.Client, prefix: prefix, ttl: config.TTL, maxHistory: config.MaxHistory, logs: make(map[string]*logView)}, nil
}

func (s *RedisStore) Load(ctx context.Context, conversationID string) (corestate.Checkpoint, bool, error) {
	return s.Get(ctx, conversationID, "")
}

func (s *RedisStore) Get(ctx context.Context, conversationID, checkpointID string) (corestate.Checkpoint, bool, error) {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return corestate.Checkpoint{}, false, err
	}
	field := "latest"
	if checkpointID != "" {
		field = "record:" + checkpointID
	}
	data, err := s.client.HGet(ctx, s.key(conversationID), field).Bytes()
	if errors.Is(err, redis.Nil) {
		return corestate.Checkpoint{}, false, nil
	}
	if err != nil {
		return corestate.Checkpoint{}, false, fmt.Errorf("load unified checkpoint (legacy string records are incompatible): %w", err)
	}
	checkpoint, err := corestate.DecodeCheckpoint(data)
	if err != nil {
		return corestate.Checkpoint{}, false, err
	}
	if checkpoint.MessagesFrom <= 0 {
		checkpoint.MessagesEpoch = 0
		return checkpoint, true, nil
	}
	// 记录只携带增量消息：按记录引用的代次日志取回前缀后物化完整会话。
	prefix, err := s.logPrefix(ctx, conversationID, checkpoint.MessagesEpoch, checkpoint.MessagesFrom)
	if err != nil {
		return corestate.Checkpoint{}, false, err
	}
	checkpoint.Messages = append(prefix, checkpoint.Messages...)
	checkpoint.MessagesFrom = 0
	checkpoint.MessagesEpoch = 0
	return checkpoint, true, nil
}

// logPrefix 取回指定代次日志的前 n 条消息。元素是保存脚本原样写入的
// 消息 JSON；长度不足或解码失败即数据损坏，按错误返回。
func (s *RedisStore) logPrefix(ctx context.Context, conversationID string, epoch, n int) ([]schema.Message, error) {
	if epoch <= 0 {
		return nil, fmt.Errorf("checkpoint references %d log messages without epoch", n)
	}
	raw, err := s.client.LRange(ctx, s.messagesKey(conversationID, epoch), 0, int64(n-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("load checkpoint message log: %w", err)
	}
	if len(raw) < n {
		return nil, fmt.Errorf("checkpoint message log epoch %d holds %d entries, need %d", epoch, len(raw), n)
	}
	messages := make([]schema.Message, 0, n)
	for _, item := range raw {
		var m schema.Message
		if err := json.Unmarshal([]byte(item), &m); err != nil {
			return nil, fmt.Errorf("decode checkpoint message log entry: %w", err)
		}
		messages = append(messages, m)
	}
	return messages, nil
}

func (s *RedisStore) List(ctx context.Context, conversationID string, options corestate.ListOptions) ([]corestate.Checkpoint, error) {
	if options.Limit < 0 {
		return nil, errors.New("history limit cannot be negative")
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	c, found, err := s.Get(ctx, conversationID, options.Before)
	if err != nil {
		return nil, err
	}
	if !found {
		if options.Before != "" {
			if _, exists, e := s.Load(ctx, conversationID); e != nil {
				return nil, e
			} else if exists {
				return nil, errors.New("history cursor not found")
			}
		}
		return nil, nil
	}
	if options.Before != "" {
		if c.ParentID == "" {
			return nil, nil
		}
		c, found, err = s.Get(ctx, conversationID, c.ParentID)
	}
	var records []corestate.Checkpoint
	for found && err == nil && len(records) < limit {
		records = append(records, c)
		if c.ParentID == "" || len(records) == limit {
			break
		}
		c, found, err = s.Get(ctx, conversationID, c.ParentID)
	}
	return records, err
}

func (s *RedisStore) Save(ctx context.Context, checkpoint corestate.Checkpoint) (corestate.Checkpoint, error) {
	if err := validateContextAndID(ctx, checkpoint.ConversationID); err != nil {
		return corestate.Checkpoint{}, err
	}
	prepared, err := normalizeCheckpoint(checkpoint)
	if err != nil {
		return corestate.Checkpoint{}, err
	}
	if err := prepared.Validate(); err != nil {
		return corestate.Checkpoint{}, err
	}
	now := time.Now().UTC()
	// PersistClone 的副本归本次调用所有，直接作为返回值，不再二次克隆。
	saved := prepared
	saved.Version = checkpoint.Version + 1
	saved.ID = corestate.NewID()
	saved.ParentID = checkpoint.ID
	if saved.CreatedAt.IsZero() {
		saved.CreatedAt = now
	}
	saved.UpdatedAt = now
	// 逐条编码消息并计算 sha1 摘要：保存脚本用同一口径比对前缀。
	encoded := make([]string, len(saved.Messages))
	hashes := make([]string, len(saved.Messages))
	for i, m := range saved.Messages {
		raw, err := json.Marshal(m)
		if err != nil {
			return corestate.Checkpoint{}, fmt.Errorf("encode Redis checkpoint message %d: %w", i, err)
		}
		encoded[i] = string(raw)
		sum := sha1.Sum(raw)
		hashes[i] = hex.EncodeToString(sum[:])
	}
	// 乐观共享前缀基于上次读写的日志视图；失配时脚本报告真实边界，
	// 收缩后重试，CAS 保证期间不会有第三方写入改变日志。
	// 缓存访问与网络调用分开：锁只保护 logs 视图，脚本执行不持锁，
	// 避免跨会话的保存互相串行；失配收缩 claimed 后重试。
	s.mu.Lock()
	claimed := 0
	if view := s.logs[checkpoint.ConversationID]; view != nil && view.epoch > 0 {
		n := len(view.hashes)
		if len(hashes) < n {
			n = len(hashes)
		}
		for claimed < n && hashes[claimed] == view.hashes[claimed] {
			claimed++
		}
	}
	s.mu.Unlock()
	for attempt := 0; attempt < 4; attempt++ {
		shared, epoch, retry, err := s.saveAttempt(ctx, checkpoint, saved, encoded, hashes, claimed)
		if err != nil {
			return corestate.Checkpoint{}, err
		}
		if !retry {
			s.mu.Lock()
			s.logs[checkpoint.ConversationID] = &logView{epoch: epoch, hashes: hashes}
			s.mu.Unlock()
			return saved, nil
		}
		// 失配的真实前缀可能是零：不能用数值符号表达重试语义，
		// 必须以脚本的显式响应为准收缩 claimed 后重试。
		claimed = shared
	}
	return corestate.Checkpoint{}, fmt.Errorf("save Redis checkpoint: message log prefix did not converge")
}

// saveAttempt 编码增量记录并执行一次保存脚本。retry 为真时脚本未写入
// 任何记录，shared 是校验出的真实共享前缀长度（可为零），调用方须收缩
// claimed 重新编码增量后重试；retry 为假时保存已提交，epoch 为新代次。
func (s *RedisStore) saveAttempt(ctx context.Context, checkpoint, saved corestate.Checkpoint, encoded, hashes []string, claimed int) (shared int, epoch int, retry bool, err error) {
	data, err := encodeRecordWithLogRef(saved)
	if err != nil {
		return 0, 0, false, err
	}
	args := make([]any, 0, 10+len(hashes)+len(encoded)-claimed)
	args = append(args,
		strconv.FormatUint(checkpoint.Version, 10),
		data,
		s.ttl.Milliseconds(),
		strconv.FormatUint(saved.Version, 10),
		checkpoint.ID,
		saved.ID,
		strconv.Itoa(claimed),
		strconv.Itoa(len(hashes)),
	)
	for _, h := range hashes {
		args = append(args, h)
	}
	for i := claimed; i < len(encoded); i++ {
		args = append(args, encoded[i])
	}
	args = append(args, strconv.Itoa(s.maxHistory))
	result, err := saveCheckpointScript.Run(
		ctx,
		s.client,
		[]string{s.key(checkpoint.ConversationID), s.messagesKeyPrefix(checkpoint.ConversationID)},
		args...,
	).Slice()
	if err != nil {
		return 0, 0, false, fmt.Errorf("save Redis checkpoint: %w", err)
	}
	if len(result) < 2 {
		return 0, 0, false, fmt.Errorf("save Redis checkpoint returned %d values", len(result))
	}
	ok, err := redisInteger(result[0])
	if err != nil {
		return 0, 0, false, err
	}
	if ok == 2 {
		actual, err := redisInteger(result[1])
		if err != nil {
			return 0, 0, false, err
		}
		return int(actual), 0, true, nil
	}
	if ok != 1 {
		actual, err := redisInteger(result[1])
		if err != nil {
			return 0, 0, false, err
		}
		return 0, 0, false, &corestate.ConflictError{
			ConversationID: checkpoint.ConversationID,
			Expected:       checkpoint.Version,
			Actual:         uint64(actual),
		}
	}
	sharedN, err := redisInteger(result[2])
	if err != nil {
		return 0, 0, false, err
	}
	epochN, err := redisInteger(result[3])
	if err != nil {
		return 0, 0, false, err
	}
	return int(sharedN), int(epochN), false, nil
}

// recordWire 把 messages/messages_from/messages_epoch 换成字符串占位符；
// 脚本校验后用增量数组与边界数字替换同名字段位置，记录其余字节保持
// 调用方编码结果。
type recordWire struct {
	corestate.Checkpoint
	Messages      string `json:"messages,omitempty"`
	MessagesFrom  string `json:"messages_from,omitempty"`
	MessagesEpoch string `json:"messages_epoch,omitempty"`
}

func encodeRecordWithLogRef(c corestate.Checkpoint) (string, error) {
	c.Messages = nil
	c.MessagesFrom = 0
	c.MessagesEpoch = 0
	data, err := json.Marshal(recordWire{Checkpoint: c, Messages: "@@GH_M@@", MessagesFrom: "@@GH_F@@", MessagesEpoch: "@@GH_E@@"})
	if err != nil {
		return "", fmt.Errorf("encode Redis checkpoint: %w", err)
	}
	return string(data), nil
}

func validateContextAndID(ctx context.Context, conversationID string) error {
	if ctx == nil {
		return errors.New("checkpoint context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(conversationID) == "" {
		return errors.New("checkpoint conversation ID cannot be empty")
	}
	return nil
}

// normalizeCheckpoint 在持久化边界克隆：结构克隆不支持的元数据值
// 经 JSON 保底转换，克隆实现失败等硬错误直接返回给调用方。
func normalizeCheckpoint(checkpoint corestate.Checkpoint) (corestate.Checkpoint, error) {
	return checkpoint.PersistClone()
}

// RollbackBasis 返回当前运行开始前的回退依据记录：保存脚本在运行轮换时
// 把原头部记录留到 rollback 字段，历史裁剪不触及它。
func (s *RedisStore) RollbackBasis(ctx context.Context, conversationID string) (corestate.Checkpoint, bool, error) {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return corestate.Checkpoint{}, false, err
	}
	raw, err := s.client.HGet(ctx, s.key(conversationID), "rollback").Bytes()
	if errors.Is(err, redis.Nil) {
		return corestate.Checkpoint{}, false, nil
	}
	if err != nil {
		return corestate.Checkpoint{}, false, fmt.Errorf("load rollback basis: %w", err)
	}
	checkpoint, err := corestate.DecodeCheckpoint(raw)
	if err != nil {
		return corestate.Checkpoint{}, false, err
	}
	if checkpoint.MessagesFrom <= 0 {
		checkpoint.MessagesEpoch = 0
		return checkpoint, true, nil
	}
	prefix, err := s.logPrefix(ctx, conversationID, checkpoint.MessagesEpoch, checkpoint.MessagesFrom)
	if err != nil {
		return corestate.Checkpoint{}, false, err
	}
	checkpoint.Messages = append(prefix, checkpoint.Messages...)
	checkpoint.MessagesFrom = 0
	checkpoint.MessagesEpoch = 0
	return checkpoint, true, nil
}

func (s *RedisStore) Delete(ctx context.Context, conversationID string) error {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return err
	}
	// 日志视图缓存与 Save 共用同一把锁：保存与删除即使作用于不同
	// 会话，也不能在锁外读写同一个 map。
	s.mu.Lock()
	delete(s.logs, conversationID)
	s.mu.Unlock()
	var keys []string
	raw, err := s.client.HGet(ctx, s.key(conversationID), "epochs").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if raw != "" {
		var epochs []int
		if err := json.Unmarshal([]byte(raw), &epochs); err == nil {
			for _, e := range epochs {
				keys = append(keys, s.messagesKey(conversationID, e))
			}
		}
	}
	keys = append(keys, s.key(conversationID))
	return s.client.Del(ctx, keys...).Err()
}

func (s *RedisStore) key(conversationID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(conversationID))
	return s.prefix + ":{" + encoded + "}"
}

// messagesKey 是与主 hash 同槽的消息日志 LIST：{id} 哈希标签保证集群
// 模式下脚本能原子访问。
func (s *RedisStore) messagesKey(conversationID string, epoch int) string {
	return s.messagesKeyPrefix(conversationID) + ":" + strconv.Itoa(epoch)
}

func (s *RedisStore) messagesKeyPrefix(conversationID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(conversationID))
	return s.prefix + ":{" + encoded + "}:msgs"
}

func redisInteger(value any) (int64, error) {
	switch current := value.(type) {
	case int64:
		return current, nil
	case uint64:
		return int64(current), nil
	case string:
		parsed, err := strconv.ParseInt(current, 10, 64)
		return parsed, err
	case []byte:
		parsed, err := strconv.ParseInt(string(current), 10, 64)
		return parsed, err
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}
