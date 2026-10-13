package redisstore

import (
	"container/list"
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
	"sync/atomic"
	"time"

	"github.com/goharness/goharness/pkg/schema"
	corestate "github.com/goharness/goharness/pkg/state"

	"github.com/redis/go-redis/v9"
)

const DefaultRedisPrefix = "goharness:checkpoint"

// 主 hash 保存最新指针、不可变记录与日志元数据；消息日志按代次存放在
// 同哈希槽的独立 LIST key（{id} 标签），保存脚本一次原子完成 CAS、前缀
// 校验、增量追加、代次分叉与记录登记。
// 客户端发送增量消息体；消息摘要仅在需要服务端逐条比对时携带（缓存视图
// 命中且前缀全吻合时省略，nhashes=0）。脚本用 sha1hex 逐条比对求出真实
// 共享边界 k，记录内 messages/messages_from/messages_epoch 三处占位符
// 由脚本以校验结果替换——记录体字节始终由调用方编码，不做 cjson 往返。
// ARGV: expected_version, record_json, ttl_ms, new_version, expected_head,
// new_id, claimed, nhashes, hash_1..hash_n, msg_{c+1}..msg_n, maxhistory,
// new_run_id, view_head
// 返回: {1, new_version, shared, epoch}；{0, actual} CAS 冲突；
// {2, actual_shared} 客户端乐观前缀失配，收缩 claimed 后重试；
// {3} 客户端未带摘要且视图头失配，须带完整 hash_1..hash_n 重发。
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
local mstart = 9 + math.max(nhashes, 0)
local maxhistory = tonumber(ARGV[#ARGV - 2])
local newrun = ARGV[#ARGV - 1]
local viewhead = ARGV[#ARGV]
local epoch = tonumber(redis.call('HGET', key, 'epoch') or '0')
local shared = 0
local loglen = 0
if epoch > 0 then
 -- LLEN 取真实日志长度：LRANGE 按 nhashes 截断，用 #stored 判断
 -- 会漏掉「调用方截断历史后又改写」场景的分叉。
 loglen = redis.call('LLEN', mkey .. ':' .. epoch)
 if viewhead ~= '' and viewhead == head and claimed >= loglen then
  -- 客户端视图对应的头部记录在 CAS 保护下未变：自上次保存以来
  -- 日志没有任何其他写入，直接信任 claimed 前缀，跳过逐条比对。
  -- claimed 必须覆盖整条日志：截断保存会留下日志尾巴（loglen 大
  -- 于视图长度），尾巴之后的改写不能凭视图信任，须逐条比对。
  shared = math.min(claimed, loglen)
 else
  if nhashes < 0 then
   -- nhashes 为 -1 表示客户端依赖视图信任省略了摘要（区别于消息
   -- 列表为空的 0）：头部已变化，须带完整摘要重发。
   return {3}
  end
  local stored = redis.call('LRANGE', mkey .. ':' .. epoch, 0, nhashes - 1)
  local n = math.min(#stored, nhashes)
  for i = 1, n do
   if redis.sha1hex(stored[i]) == ARGV[8 + i] then shared = i else break end
  end
 end
end
if shared < claimed then
 return {2, shared}
end
-- 增量消息参数按客户端 claimed 边界编码；真实共享边界 shared 之后的
-- 部分才是要写入日志与记录的新消息。
local tail = shared - claimed
local dlen = #ARGV - mstart - 2
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
-- 放弃运行的回退依据；它独立于历史裁剪保留。head_run 由本脚本维护，
-- 字符串比较取代对整条记录的两次 cjson.decode。
local prev = redis.call('HGET', key, 'latest')
local headrun = redis.call('HGET', key, 'head_run') or ''
if prev and headrun ~= newrun then
 redis.call('HSET', key, 'rollback', prev)
end
redis.call('HSET', key, 'version', ARGV[4], 'head', ARGV[6], 'head_run', newrun, 'latest', record, 'record:' .. ARGV[6], record)
local ids = cjson.decode(redis.call('HGET', key, 'order') or '[]')
ids[#ids + 1] = ARGV[6]
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
// 历史记录上限，0 表示不裁剪。LogCacheSize 是进程内消息日志视图缓存
// 的会话数上限（LRU），0 使用默认值 1024；淘汰只让下一次保存多花
// 网络流量，正确性由保存脚本的原子校验保证。
type RedisStoreConfig struct {
	Client       redis.UniversalClient
	Prefix       string
	TTL          time.Duration
	MaxHistory   int
	LogCacheSize int
}

// RedisStore persists checkpoints across processes with an atomic Lua CAS.
type RedisStore struct {
	client     redis.UniversalClient
	prefix     string
	ttl        time.Duration
	maxHistory int
	logCap     int
	// mu 保护 logs 视图缓存与 LRU 链表；缓存只作乐观前缀猜测，
	// 正确性由保存脚本的原子校验保证，失配按脚本报告的边界重试。
	mu        sync.Mutex
	logs      map[string]*list.Element
	lru       *list.List
	evictions uint64
}

// logView 缓存一次保存得到的消息日志代次、对应头部记录与逐条摘要：
// head 是视图建立时的头部记录 ID，保存脚本据此判断是否信任前缀。
type logView struct {
	epoch  int
	head   string
	hashes []string
}

// logEntry 是 LRU 链表元素，key 供淘汰时回删 logs 索引。
type logEntry struct {
	key  string
	view *logView
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
	if config.LogCacheSize < 0 {
		return nil, errors.New("checkpoint LogCacheSize cannot be negative")
	}
	prefix := strings.TrimSpace(config.Prefix)
	if prefix == "" {
		prefix = DefaultRedisPrefix
	}
	logCap := config.LogCacheSize
	if logCap == 0 {
		logCap = 1024
	}
	return &RedisStore{client: config.Client, prefix: prefix, ttl: config.TTL, maxHistory: config.MaxHistory, logCap: logCap, logs: make(map[string]*list.Element), lru: list.New()}, nil
}

// viewLocked 取回会话的日志视图并标记为最近使用；调用方须持有 s.mu。
func (s *RedisStore) viewLocked(conversationID string) *logView {
	entry := s.logs[conversationID]
	if entry == nil {
		return nil
	}
	s.lru.MoveToFront(entry)
	return entry.Value.(*logEntry).view
}

// storeViewLocked 写入或刷新视图并执行 LRU 淘汰；调用方须持有 s.mu。
func (s *RedisStore) storeViewLocked(conversationID string, view *logView) {
	if entry := s.logs[conversationID]; entry != nil {
		entry.Value.(*logEntry).view = view
		s.lru.MoveToFront(entry)
	} else {
		s.logs[conversationID] = s.lru.PushFront(&logEntry{key: conversationID, view: view})
	}
	for s.lru.Len() > s.logCap {
		oldest := s.lru.Back()
		delete(s.logs, oldest.Value.(*logEntry).key)
		s.lru.Remove(oldest)
		s.evictions++
	}
}

// dropViewLocked 删除视图；调用方须持有 s.mu。
func (s *RedisStore) dropViewLocked(conversationID string) {
	if entry := s.logs[conversationID]; entry != nil {
		s.lru.Remove(entry)
		delete(s.logs, conversationID)
	}
}

// LogCacheEntries 返回进程内日志视图缓存的当前会话数（供指标使用）。
func (s *RedisStore) LogCacheEntries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Len()
}

// LogCacheEvictions 返回日志视图缓存的累计淘汰次数（供指标使用）。
func (s *RedisStore) LogCacheEvictions() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evictions
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
	// 大历史下单趟编码是主要客户端成本（实测 ~30ms@1000 条），
	// 超过阈值后并行分摊到少量 worker；结果顺序与下标保持一致。
	encoded := make([]string, len(saved.Messages))
	hashes := make([]string, len(saved.Messages))
	if err := encodeMessages(saved.Messages, encoded, hashes); err != nil {
		return corestate.Checkpoint{}, err
	}
	// 乐观共享前缀基于上次读写的日志视图；失配时脚本报告真实边界，
	// 收缩后重试，CAS 保证期间不会有第三方写入改变日志。
	// 缓存访问与网络调用分开：锁只保护 logs 视图，脚本执行不持锁，
	// 避免跨会话的保存互相串行；失配收缩 claimed 后重试。
	s.mu.Lock()
	claimed := 0
	viewHead := ""
	sendHashes := true
	if view := s.viewLocked(checkpoint.ConversationID); view != nil && view.epoch > 0 {
		viewHead = view.head
		n := len(view.hashes)
		if len(hashes) < n {
			n = len(hashes)
		}
		for claimed < n && hashes[claimed] == view.hashes[claimed] {
			claimed++
		}
		// 前缀与缓存视图全部吻合（纯追加）时省略整份摘要：脚本在视图头
		// 未变时直接信任 claimed；调用方改写、截断或冷视图仍带完整 hash
		// 供服务端逐条比对。省略把常规保存的 ARGV 从 O(历史) 降到 O(增量)。
		sendHashes = claimed < len(view.hashes)
	}
	s.mu.Unlock()
	for attempt := 0; attempt < 4; attempt++ {
		outcome, err := s.saveAttempt(ctx, checkpoint, saved, encoded, hashes, claimed, viewHead, sendHashes)
		if err != nil {
			return corestate.Checkpoint{}, err
		}
		switch {
		case outcome.saved:
			s.mu.Lock()
			s.storeViewLocked(checkpoint.ConversationID, &logView{epoch: outcome.epoch, head: saved.ID, hashes: hashes})
			s.mu.Unlock()
			return saved, nil
		case outcome.needHashes:
			// 服务端头部已在视图建立后变化：带完整摘要重发同一 claimed，
			// 由服务端逐条比对求真值。
			sendHashes = true
		default:
			// 失配的真实前缀可能是零：不能用数值符号表达重试语义，
			// 必须以脚本的显式响应为准收缩 claimed 后重试。
			claimed = outcome.shared
		}
	}
	return corestate.Checkpoint{}, fmt.Errorf("save Redis checkpoint: message log prefix did not converge")
}

// saveOutcome 是一次保存脚本尝试的结果：saved 表示已提交（epoch 为新
// 代次）；needHashes 表示服务端要求带完整摘要重发；否则是前缀失配，
// shared 为脚本校验出的真实共享边界（可为零），调用方收缩 claimed 重试。
type saveOutcome struct {
	saved      bool
	epoch      int
	shared     int
	needHashes bool
}

// saveAttempt 编码增量记录并执行一次保存脚本。sendHashes 为假时省略
// 整份摘要（nhashes=0），由脚本在视图头校验下直接信任 claimed；头部
// 已变化时脚本要求带摘要重发（needHashes）。
func (s *RedisStore) saveAttempt(ctx context.Context, checkpoint, saved corestate.Checkpoint, encoded, hashes []string, claimed int, viewHead string, sendHashes bool) (saveOutcome, error) {
	data, err := encodeRecordWithLogRef(saved)
	if err != nil {
		return saveOutcome{}, err
	}
	// nhashes 为 -1 表示省略摘要（与消息列表为空的 0 区分）：脚本在
	// 视图头校验下直接信任 claimed，头部已变化时要求带摘要重发。
	nhashes := -1
	if sendHashes {
		nhashes = len(hashes)
	}
	args := make([]any, 0, 11+nhashes+len(encoded)-claimed)
	args = append(args,
		strconv.FormatUint(checkpoint.Version, 10),
		data,
		s.ttl.Milliseconds(),
		strconv.FormatUint(saved.Version, 10),
		checkpoint.ID,
		saved.ID,
		strconv.Itoa(claimed),
		strconv.Itoa(nhashes),
	)
	if sendHashes {
		for _, h := range hashes {
			args = append(args, h)
		}
	}
	for i := claimed; i < len(encoded); i++ {
		args = append(args, encoded[i])
	}
	// viewHead 为空表示客户端没有可信视图，脚本退回逐条 sha1 比对。
	args = append(args, strconv.Itoa(s.maxHistory), saved.RunID, viewHead)
	result, err := saveCheckpointScript.Run(
		ctx,
		s.client,
		[]string{s.key(checkpoint.ConversationID), s.messagesKeyPrefix(checkpoint.ConversationID)},
		args...,
	).Slice()
	if err != nil {
		return saveOutcome{}, fmt.Errorf("save Redis checkpoint: %w", err)
	}
	if len(result) < 1 {
		return saveOutcome{}, fmt.Errorf("save Redis checkpoint returned %d values", len(result))
	}
	ok, err := redisInteger(result[0])
	if err != nil {
		return saveOutcome{}, err
	}
	switch ok {
	case 3:
		return saveOutcome{needHashes: true}, nil
	case 2:
		actual, err := redisInteger(result[1])
		if err != nil {
			return saveOutcome{}, err
		}
		return saveOutcome{shared: int(actual)}, nil
	case 1:
		sharedN, err := redisInteger(result[2])
		if err != nil {
			return saveOutcome{}, err
		}
		epochN, err := redisInteger(result[3])
		if err != nil {
			return saveOutcome{}, err
		}
		return saveOutcome{saved: true, epoch: int(epochN), shared: int(sharedN)}, nil
	default:
		actual, err := redisInteger(result[1])
		if err != nil {
			return saveOutcome{}, err
		}
		return saveOutcome{}, &corestate.ConflictError{
			ConversationID: checkpoint.ConversationID,
			Expected:       checkpoint.Version,
			Actual:         uint64(actual),
		}
	}
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

// encodeParallelThreshold 是启用并行编码的消息数下限：低于该值时
// 串行循环的固定开销更小。
const encodeParallelThreshold = 128

// encodeParallelWorkers 限制并行编码的协程数：编码是 CPU 与内存带宽
// 受限的短任务，超过 4 个 worker 收益趋平。
const encodeParallelWorkers = 4

// encodeMessages 逐条 marshal 消息并计算 sha1 摘要，写入 encoded 与
// hashes 对应下标。消息数达到阈值时分段并行编码，结果与串行完全一致。
func encodeMessages(messages []schema.Message, encoded, hashes []string) error {
	encode := func(m schema.Message, i int) error {
		raw, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("encode Redis checkpoint message %d: %w", i, err)
		}
		encoded[i] = string(raw)
		sum := sha1.Sum(raw)
		hashes[i] = hex.EncodeToString(sum[:])
		return nil
	}
	if len(messages) < encodeParallelThreshold {
		for i, m := range messages {
			if err := encode(m, i); err != nil {
				return err
			}
		}
		return nil
	}
	var firstErr atomic.Int32
	var failed atomic.Value
	var wg sync.WaitGroup
	workers := encodeParallelWorkers
	if workers > len(messages) {
		workers = len(messages)
	}
	next := atomic.Int64{}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(messages) || firstErr.Load() != 0 {
					return
				}
				if err := encode(messages[i], i); err != nil {
					if firstErr.CompareAndSwap(0, 1) {
						failed.Store(err)
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr.Load() != 0 {
		return failed.Load().(error)
	}
	return nil
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
	s.dropViewLocked(conversationID)
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
