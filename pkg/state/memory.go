package state

import (
	"context"
	"sync"
	"time"
)

type MemoryStoreConfig struct {
	TTL time.Duration
}

type memoryEntry struct {
	checkpoint Checkpoint
	expiresAt  time.Time
}

// MemoryStore is a concurrency-safe checkpoint store for local applications
// and tests. It implements the same optimistic version contract as durable
// stores supplied by optional extensions.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]memoryEntry
	ttl     time.Duration
	now     func() time.Time
}

func NewMemoryStore(configs ...MemoryStoreConfig) *MemoryStore {
	var config MemoryStoreConfig
	if len(configs) > 0 {
		config = configs[0]
	}
	return &MemoryStore{
		entries: map[string]memoryEntry{},
		ttl:     config.TTL,
		now:     time.Now,
	}
}

func (s *MemoryStore) Load(ctx context.Context, conversationID string) (Checkpoint, bool, error) {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return Checkpoint{}, false, err
	}
	now := s.now()
	s.mu.RLock()
	entry, found := s.entries[conversationID]
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
	return entry.checkpoint.Clone(), true, nil
}

func (s *MemoryStore) Save(ctx context.Context, checkpoint Checkpoint) (Checkpoint, error) {
	if err := validateContextAndID(ctx, checkpoint.ConversationID); err != nil {
		return Checkpoint{}, err
	}
	checkpoint = prepareCheckpoint(checkpoint)
	if err := checkpoint.Validate(); err != nil {
		return Checkpoint{}, err
	}
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
	if checkpoint.Version != actual {
		return Checkpoint{}, &ConflictError{
			ConversationID: checkpoint.ConversationID,
			Expected:       checkpoint.Version,
			Actual:         actual,
		}
	}
	saved := checkpoint.Clone()
	saved.Version = actual + 1
	if found {
		saved.CreatedAt = current.checkpoint.CreatedAt
	} else if saved.CreatedAt.IsZero() {
		saved.CreatedAt = now
	}
	saved.UpdatedAt = now
	entry := memoryEntry{checkpoint: saved.Clone()}
	if s.ttl > 0 {
		entry.expiresAt = now.Add(s.ttl)
	}
	s.entries[saved.ConversationID] = entry
	return saved.Clone(), nil
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
