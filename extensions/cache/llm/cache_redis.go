package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client redis.UniversalClient
	prefix string
}

type RedisCacheConfig struct {
	Client redis.UniversalClient
	Prefix string
}

func NewRedisCache(cfg RedisCacheConfig) (*RedisCache, error) {
	if cfg.Client == nil {
		return nil, errors.New("redis cache requires a client")
	}
	return &RedisCache{client: cfg.Client, prefix: strings.TrimSpace(cfg.Prefix)}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string) (CacheEntry, bool, error) {
	data, err := c.client.Get(ctx, c.key(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return CacheEntry{}, false, nil
	}
	if err != nil {
		return CacheEntry{}, false, err
	}
	var entry CacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return CacheEntry{}, false, fmt.Errorf("decode Redis cache entry: %w", err)
	}
	return entry.Clone(), true, nil
}

func (c *RedisCache) Set(ctx context.Context, key string, entry CacheEntry, ttl time.Duration) error {
	if ttl < 0 {
		return errors.New("redis cache TTL cannot be negative")
	}
	data, err := json.Marshal(entry.Clone())
	if err != nil {
		return fmt.Errorf("encode Redis cache entry: %w", err)
	}
	return c.client.Set(ctx, c.key(key), data, ttl).Err()
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, c.key(key)).Err()
}

func (c *RedisCache) key(key string) string {
	if c.prefix == "" {
		return key
	}
	return c.prefix + ":" + key
}
