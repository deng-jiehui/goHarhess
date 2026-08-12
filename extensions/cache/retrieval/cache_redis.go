package retrieval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisVectorCacheConfig struct {
	Client redis.UniversalClient
	Prefix string
}

// RedisVectorCache shares query entries and invalidation versions across
// application instances.
type RedisVectorCache struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisVectorCache(cfg RedisVectorCacheConfig) (*RedisVectorCache, error) {
	if cfg.Client == nil {
		return nil, errors.New("redis vector cache requires a client")
	}
	prefix := strings.TrimSpace(cfg.Prefix)
	if prefix == "" {
		prefix = DefaultVectorCacheNamespace
	}
	return &RedisVectorCache{client: cfg.Client, prefix: prefix}, nil
}

func (c *RedisVectorCache) Get(ctx context.Context, key string) (VectorCacheEntry, bool, error) {
	data, err := c.client.Get(ctx, c.entryKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return VectorCacheEntry{}, false, nil
	}
	if err != nil {
		return VectorCacheEntry{}, false, err
	}
	var entry VectorCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return VectorCacheEntry{}, false, fmt.Errorf("decode Redis vector cache entry: %w", err)
	}
	return entry.Clone(), true, nil
}

func (c *RedisVectorCache) Set(ctx context.Context, key string, entry VectorCacheEntry, ttl time.Duration) error {
	if ttl < 0 {
		return errors.New("redis vector cache TTL cannot be negative")
	}
	data, err := json.Marshal(entry.Clone())
	if err != nil {
		return fmt.Errorf("encode Redis vector cache entry: %w", err)
	}
	return c.client.Set(ctx, c.entryKey(key), data, ttl).Err()
}

func (c *RedisVectorCache) Version(ctx context.Context, scope string) (uint64, error) {
	value, err := c.client.Get(ctx, c.versionKey(scope)).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return value, err
}

func (c *RedisVectorCache) BumpVersion(ctx context.Context, scope string) (uint64, error) {
	value, err := c.client.Incr(ctx, c.versionKey(scope)).Uint64()
	return value, err
}

func (c *RedisVectorCache) entryKey(key string) string {
	return c.prefix + ":entry:" + key
}

func (c *RedisVectorCache) versionKey(scope string) string {
	return c.prefix + ":version:" + scope
}
