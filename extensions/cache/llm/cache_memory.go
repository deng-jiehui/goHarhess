package llm

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

type MemoryCacheConfig struct {
	Capacity int
	Now      func() time.Time
}

type MemoryCache struct {
	mu       sync.Mutex
	capacity int
	now      func() time.Time
	items    map[string]*list.Element
	lru      *list.List
}

type memoryCacheItem struct {
	key       string
	entry     CacheEntry
	expiresAt time.Time
}

func NewMemoryCache(cfg MemoryCacheConfig) (*MemoryCache, error) {
	if cfg.Capacity <= 0 {
		return nil, errors.New("memory cache capacity must be greater than zero")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &MemoryCache{
		capacity: cfg.Capacity,
		now:      now,
		items:    make(map[string]*list.Element, cfg.Capacity),
		lru:      list.New(),
	}, nil
}

func (c *MemoryCache) Get(ctx context.Context, key string) (CacheEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return CacheEntry{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	element := c.items[key]
	if element == nil {
		return CacheEntry{}, false, nil
	}
	item := element.Value.(*memoryCacheItem)
	if !item.expiresAt.IsZero() && !c.now().Before(item.expiresAt) {
		c.remove(element)
		return CacheEntry{}, false, nil
	}
	c.lru.MoveToFront(element)
	return item.entry.Clone(), true, nil
}

func (c *MemoryCache) Set(ctx context.Context, key string, entry CacheEntry, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl < 0 {
		return errors.New("memory cache TTL cannot be negative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = c.now().Add(ttl)
	}
	if element := c.items[key]; element != nil {
		item := element.Value.(*memoryCacheItem)
		item.entry = entry.Clone()
		item.expiresAt = expiresAt
		c.lru.MoveToFront(element)
		return nil
	}

	element := c.lru.PushFront(&memoryCacheItem{key: key, entry: entry.Clone(), expiresAt: expiresAt})
	c.items[key] = element
	for c.lru.Len() > c.capacity {
		c.remove(c.lru.Back())
	}
	return nil
}

func (c *MemoryCache) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[key]; element != nil {
		c.remove(element)
	}
	return nil
}

func (c *MemoryCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeExpired()
	return c.lru.Len()
}

func (c *MemoryCache) removeExpired() {
	now := c.now()
	for element := c.lru.Back(); element != nil; {
		previous := element.Prev()
		item := element.Value.(*memoryCacheItem)
		if !item.expiresAt.IsZero() && !now.Before(item.expiresAt) {
			c.remove(element)
		}
		element = previous
	}
}

func (c *MemoryCache) remove(element *list.Element) {
	if element == nil {
		return
	}
	item := element.Value.(*memoryCacheItem)
	delete(c.items, item.key)
	c.lru.Remove(element)
}
