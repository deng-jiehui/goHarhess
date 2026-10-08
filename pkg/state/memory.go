package state

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/goharness/goharness/pkg/schema"
)

type MemoryStoreConfig struct {
	TTL time.Duration
	// MaxHistory 为历史保留上限（条数），0 表示不限制。
	// 超过后丢弃最旧的记录；最新记录与最新会话始终保留。
	MaxHistory int
}

type memoryEntry struct {
	checkpoint Checkpoint
	// messages 是当前代次的会话消息日志：只追加、不截断，供增量记录物化。
	// 提交后不再原地修改；分叉时整体替换，旧代次日志移入 archives。
	messages []schema.Message
	epoch    int
	// archives 保存前缀分叉前的旧日志，按代次索引，供历史记录物化。
	// 每条日志提交后不可变；读取在锁内取切片，物化在锁外进行。
	archives map[int][]schema.Message
	// rollback 是当前运行开始前的回退依据记录（增量形式），独立于
	// 历史淘汰保留；运行轮换时由上一次保存的头部记录接任。
	rollback    Checkpoint
	hasRollback bool
	expiresAt   time.Time
	history     []Checkpoint
}

// MemoryStore is a concurrency-safe checkpoint store for local applications
// and tests. It implements the same optimistic version contract as durable
// stores supplied by optional extensions.
type MemoryStore struct {
	mu         sync.RWMutex
	entries    map[string]memoryEntry
	ttl        time.Duration
	maxHistory int
	now        func() time.Time
}

func NewMemoryStore(configs ...MemoryStoreConfig) *MemoryStore {
	var config MemoryStoreConfig
	if len(configs) > 0 {
		config = configs[0]
	}
	return &MemoryStore{
		entries:    map[string]memoryEntry{},
		ttl:        config.TTL,
		maxHistory: config.MaxHistory,
		now:        time.Now,
	}
}

func (s *MemoryStore) Load(ctx context.Context, conversationID string) (Checkpoint, bool, error) {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return Checkpoint{}, false, err
	}
	now := s.now()
	s.mu.RLock()
	entry, found := s.entries[conversationID]
	var log []schema.Message
	if found {
		// 在锁内取出本次物化所需的日志切片：提交后的日志不可变，
		// 锁外物化不再接触 entry 或归档 map。
		log = entry.logFor(entry.checkpoint)
	}
	s.mu.RUnlock()
	if !found {
		return Checkpoint{}, false, nil
	}
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		s.mu.Lock()
		if current, ok := s.entries[conversationID]; ok && current.expiresAt == entry.expiresAt {
			delete(s.entries, conversationID)
		}
		s.mu.Unlock()
		return Checkpoint{}, false, nil
	}
	// entry.checkpoint 提交后不再修改，是不可变视图；物化与复制放到锁外，
	// 克隆失败作为错误返回而不是回退到共享引用。
	cloned, err := materializeCheckpointMessages(log, entry.checkpoint).PersistClone()
	if err != nil {
		return Checkpoint{}, false, err
	}
	return cloned, true, nil
}

func (s *MemoryStore) Save(ctx context.Context, checkpoint Checkpoint) (Checkpoint, error) {
	if err := validateContextAndID(ctx, checkpoint.ConversationID); err != nil {
		return Checkpoint{}, err
	}
	// 持久化克隆在锁外完成：隔离失败（Clone() 错误、JSON 保底失败等）
	// 直接返回，不产生新版本、不更新存储。
	prepared, err := checkpoint.PersistClone()
	if err != nil {
		return Checkpoint{}, err
	}
	if err := prepared.Validate(); err != nil {
		return Checkpoint{}, err
	}
	// 存储副本同样在锁外生成：两份副本都成功后才提交版本与记录，
	// 克隆失败不会污染存储。
	stored, err := prepared.PersistClone()
	if err != nil {
		return Checkpoint{}, err
	}
	checkpoint = prepared
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.entries[checkpoint.ConversationID]
	if found && !current.expiresAt.IsZero() && !now.Before(current.expiresAt) {
		delete(s.entries, checkpoint.ConversationID)
		current = memoryEntry{}
		found = false
	}
	actual := uint64(0)
	if found {
		actual = current.checkpoint.Version
	}
	if checkpoint.Version != actual || checkpoint.ID != current.checkpoint.ID {
		return Checkpoint{}, &ConflictError{
			ConversationID: checkpoint.ConversationID,
			Expected:       checkpoint.Version,
			Actual:         actual,
		}
	}
	// PersistClone 已产出来自调用方的独立副本：一份直接作为返回值，
	// 另一份作为存储副本，最新记录与历史条目共享同一份不可变数据
	// （写入后不再修改），避免每次保存重复深拷贝整份会话。
	saved := checkpoint
	saved.Version = actual + 1
	saved.ID = NewID()
	saved.ParentID = current.checkpoint.ID
	if found {
		saved.CreatedAt = current.checkpoint.CreatedAt
	} else if saved.CreatedAt.IsZero() {
		saved.CreatedAt = now
	}
	saved.UpdatedAt = now
	// 消息增量：stored 的会话与会话日志的最长公共前缀只保存引用，
	// 历史随轮数按新增部分增长而不是重复完整会话。分叉（调用方
	// 截断或改写历史）时开新代次，旧日志归档供历史记录物化。
	stored.Version = saved.Version
	stored.ID = saved.ID
	stored.ParentID = saved.ParentID
	stored.CreatedAt = saved.CreatedAt
	stored.UpdatedAt = saved.UpdatedAt
	// 代次从 1 开始：0 保留给无引用的记录。
	epoch := current.epoch
	if !found {
		epoch = 1
	}
	arena := current.messages
	shared := sharedMessagePrefix(stored.Messages, arena)
	fork := found && shared < len(arena) && shared < len(stored.Messages)
	if fork {
		epoch++
		if current.archives == nil {
			current.archives = map[int][]schema.Message{}
		}
		current.archives[current.epoch] = arena
		arena = nil
	}
	full := stored.Messages
	// 尾部增量复制为独立切片：full[shared:] 的视图会让整条底层数组
	// （含前缀消息对象）随记录存活，违背日志去重的内存目标。
	stored.Messages = append([]schema.Message(nil), full[shared:]...)
	stored.MessagesFrom = shared
	stored.MessagesEpoch = epoch
	switch {
	case fork:
		arena = full
	case len(full) > len(arena):
		arena = full
	}
	// 运行轮换：新记录 RunID 与当前头部不同，则头部是本轮运行的
	// 回退依据（对应 runBase 找到的第一条不同 RunID 父记录），
	// 独立于历史淘汰保留。
	rollback, hasRollback := current.rollback, current.hasRollback
	if found && saved.RunID != current.checkpoint.RunID {
		rollback, hasRollback = current.checkpoint, true
	}
	history := append(current.history, stored)
	if s.maxHistory > 0 && len(history) > s.maxHistory {
		// 历史保留：丢弃最旧记录；回退依据已由 rollback 独立保留，
		// 不占用历史条数。
		history = append([]Checkpoint(nil), history[len(history)-s.maxHistory:]...)
	}
	// 代次回收：只保留当前代次与仍被保留记录及回退依据引用的旧日志。
	archives := current.archives
	if len(archives) > 0 {
		used := map[int]bool{epoch: true}
		if hasRollback {
			used[rollback.MessagesEpoch] = true
		}
		for _, c := range history {
			used[c.MessagesEpoch] = true
		}
		next := map[int][]schema.Message{}
		for e, log := range archives {
			if used[e] {
				next[e] = log
			}
		}
		archives = next
	}
	entry := memoryEntry{checkpoint: stored, messages: arena, epoch: epoch, archives: archives, rollback: rollback, hasRollback: hasRollback, history: history}
	if s.ttl > 0 {
		entry.expiresAt = now.Add(s.ttl)
	}
	s.entries[saved.ConversationID] = entry
	return saved, nil
}

// logFor 返回物化记录 c 所需的日志切片；必须在持锁期间调用。
// 切片指向提交后不再修改的日志，可以在锁外安全物化。
func (e memoryEntry) logFor(c Checkpoint) []schema.Message {
	if c.MessagesEpoch != e.epoch {
		return e.archives[c.MessagesEpoch]
	}
	return e.messages
}

// RollbackBasis 返回当前运行开始前的回退依据记录。它在锁内取所属代次
// 日志切片，锁外物化；没有前一轮运行时返回未命中。
func (s *MemoryStore) RollbackBasis(ctx context.Context, conversationID string) (Checkpoint, bool, error) {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return Checkpoint{}, false, err
	}
	s.mu.RLock()
	entry, found := s.entries[conversationID]
	var source Checkpoint
	var log []schema.Message
	if found && entry.hasRollback && (entry.expiresAt.IsZero() || s.now().Before(entry.expiresAt)) {
		source = entry.rollback
		log = entry.logFor(entry.rollback)
	}
	s.mu.RUnlock()
	if source.ID == "" {
		return Checkpoint{}, false, nil
	}
	cloned, err := materializeCheckpointMessages(log, source).PersistClone()
	if err != nil {
		return Checkpoint{}, false, err
	}
	return cloned, true, nil
}

func (s *MemoryStore) Delete(ctx context.Context, conversationID string) error {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.entries, conversationID)
	s.mu.Unlock()
	return nil
}

// Get 读取指定历史记录；空 ID 表示最新记录。
func (s *MemoryStore) Get(ctx context.Context, conversationID, checkpointID string) (Checkpoint, bool, error) {
	if checkpointID == "" {
		return s.Load(ctx, conversationID)
	}
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return Checkpoint{}, false, err
	}
	s.mu.RLock()
	entry, found := s.entries[conversationID]
	var source Checkpoint
	var log []schema.Message
	matched := false
	if found && (entry.expiresAt.IsZero() || s.now().Before(entry.expiresAt)) {
		for _, c := range entry.history {
			if c.ID == checkpointID {
				source = c
				log = entry.logFor(c)
				matched = true
				break
			}
		}
	}
	s.mu.RUnlock()
	if !matched {
		return Checkpoint{}, false, nil
	}
	cloned, err := materializeCheckpointMessages(log, source).PersistClone()
	if err != nil {
		return Checkpoint{}, false, err
	}
	return cloned, true, nil
}

func (s *MemoryStore) List(ctx context.Context, conversationID string, options ListOptions) ([]Checkpoint, error) {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return nil, err
	}
	if options.Limit < 0 {
		return nil, errors.New("history limit cannot be negative")
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	s.mu.RLock()
	entry, found := s.entries[conversationID]
	var sources []Checkpoint
	var logs [][]schema.Message
	cursorMissing := false
	if found && (entry.expiresAt.IsZero() || s.now().Before(entry.expiresAt)) {
		end := len(entry.history)
		if options.Before != "" {
			end = -1
			for i, c := range entry.history {
				if c.ID == options.Before {
					end = i
					break
				}
			}
			if end < 0 {
				cursorMissing = true
			}
		}
		for i := end - 1; i >= 0 && len(sources) < limit; i-- {
			sources = append(sources, entry.history[i])
			logs = append(logs, entry.logFor(entry.history[i]))
		}
	}
	s.mu.RUnlock()
	if cursorMissing {
		return nil, errors.New("history cursor not found")
	}
	result := make([]Checkpoint, 0, len(sources))
	for i, c := range sources {
		cloned, err := materializeCheckpointMessages(logs[i], c).PersistClone()
		if err != nil {
			return nil, err
		}
		result = append(result, cloned)
	}
	return result, nil
}
