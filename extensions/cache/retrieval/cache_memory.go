package retrieval

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

type MemoryVectorCacheConfig struct {
	Capacity int
}

// MemoryVectorCache is a bounded LRU/TTL cache with in-process scope versions.
type MemoryVectorCache struct {
	mu       sync.Mutex
	capacity int
	lru      *list.List
	items    map[string]*list.Element
	versions map[string]uint64
}

type memoryVectorCacheItem struct {
	key       string
	entry     VectorCacheEntry
	expiresAt time.Time
}

func NewMemoryVectorCache(cfg MemoryVectorCacheConfig) (*MemoryVectorCache, error) {
	if cfg.Capacity <= 0 {
		return nil, errors.New("memory vector cache capacity must be greater than zero")
	}
	return &MemoryVectorCache{
		capacity: cfg.Capacity,
		lru:      list.New(),
		items:    map[string]*list.Element{},
		versions: map[string]uint64{},
	}, nil
}

func (c *MemoryVectorCache) Get(ctx context.Context, key string) (VectorCacheEntry, bool, error) {
	if err := vectorCacheContextError(ctx); err != nil {
		return VectorCacheEntry{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.items[key]
	if element == nil {
		return VectorCacheEntry{}, false, nil
	}
	item := element.Value.(*memoryVectorCacheItem)
	if !item.expiresAt.IsZero() && time.Now().After(item.expiresAt) {
		c.remove(element)
		return VectorCacheEntry{}, false, nil
	}
	c.lru.MoveToFront(element)
	return item.entry.Clone(), true, nil
}

func (c *MemoryVectorCache) Set(ctx context.Context, key string, entry VectorCacheEntry, ttl time.Duration) error {
	if err := vectorCacheContextError(ctx); err != nil {
		return err
	}
	if ttl < 0 {
		return errors.New("memory vector cache TTL cannot be negative")
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key]; element != nil {
		item := element.Value.(*memoryVectorCacheItem)
		item.entry = entry.Clone()
		item.expiresAt = expiresAt
		c.lru.MoveToFront(element)
		return nil
	}
	element := c.lru.PushFront(&memoryVectorCacheItem{key: key, entry: entry.Clone(), expiresAt: expiresAt})
	c.items[key] = element
	for c.lru.Len() > c.capacity {
		c.remove(c.lru.Back())
	}
	return nil
}

func (c *MemoryVectorCache) Version(ctx context.Context, scope string) (uint64, error) {
	if err := vectorCacheContextError(ctx); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.versions[scope], nil
}

func (c *MemoryVectorCache) BumpVersion(ctx context.Context, scope string) (uint64, error) {
	if err := vectorCacheContextError(ctx); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.versions[scope]++
	return c.versions[scope], nil
}

func (c *MemoryVectorCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeExpired()
	return c.lru.Len()
}

func (c *MemoryVectorCache) removeExpired() {
	now := time.Now()
	for element := c.lru.Back(); element != nil; {
		previous := element.Prev()
		item := element.Value.(*memoryVectorCacheItem)
		if !item.expiresAt.IsZero() && now.After(item.expiresAt) {
			c.remove(element)
		}
		element = previous
	}
}

func (c *MemoryVectorCache) remove(element *list.Element) {
	if element == nil {
		return
	}
	item := element.Value.(*memoryVectorCacheItem)
	delete(c.items, item.key)
	c.lru.Remove(element)
}

func vectorCacheContextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("vector cache context cannot be nil")
	}
	return ctx.Err()
}
