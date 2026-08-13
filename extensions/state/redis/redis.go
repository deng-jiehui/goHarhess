package redisstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	corestate "goharness/pkg/state"

	"github.com/redis/go-redis/v9"
)

const DefaultRedisPrefix = "goharness:checkpoint"

var saveCheckpointScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
local expected = tonumber(ARGV[1])
local actual = 0
if current then
    local decoded = cjson.decode(current)
    actual = tonumber(decoded.version) or 0
end
if actual ~= expected then
    return {0, actual}
end
redis.call('SET', KEYS[1], ARGV[2])
local ttl = tonumber(ARGV[3])
if ttl and ttl > 0 then
    redis.call('PEXPIRE', KEYS[1], ttl)
end
return {1, expected + 1}
`)

type RedisStoreConfig struct {
	Client redis.UniversalClient
	Prefix string
	TTL    time.Duration
}

// RedisStore persists checkpoints across processes with an atomic Lua CAS.
type RedisStore struct {
	client redis.UniversalClient
	prefix string
	ttl    time.Duration
}

var _ corestate.Store = (*RedisStore)(nil)

func NewRedisStore(config RedisStoreConfig) (*RedisStore, error) {
	if config.Client == nil {
		return nil, errors.New("redis checkpoint store requires a client")
	}
	if config.TTL < 0 {
		return nil, errors.New("redis checkpoint TTL cannot be negative")
	}
	prefix := strings.TrimSpace(config.Prefix)
	if prefix == "" {
		prefix = DefaultRedisPrefix
	}
	return &RedisStore{client: config.Client, prefix: prefix, ttl: config.TTL}, nil
}

func (s *RedisStore) Load(ctx context.Context, conversationID string) (corestate.Checkpoint, bool, error) {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return corestate.Checkpoint{}, false, err
	}
	data, err := s.client.Get(ctx, s.key(conversationID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return corestate.Checkpoint{}, false, nil
	}
	if err != nil {
		return corestate.Checkpoint{}, false, err
	}
	var checkpoint corestate.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return corestate.Checkpoint{}, false, fmt.Errorf("decode Redis checkpoint: %w", err)
	}
	checkpoint = normalizeCheckpoint(checkpoint)
	if err := checkpoint.Validate(); err != nil {
		return corestate.Checkpoint{}, false, fmt.Errorf("validate Redis checkpoint: %w", err)
	}
	return checkpoint.Clone(), true, nil
}

func (s *RedisStore) Save(ctx context.Context, checkpoint corestate.Checkpoint) (corestate.Checkpoint, error) {
	if err := validateContextAndID(ctx, checkpoint.ConversationID); err != nil {
		return corestate.Checkpoint{}, err
	}
	checkpoint = normalizeCheckpoint(checkpoint)
	if err := checkpoint.Validate(); err != nil {
		return corestate.Checkpoint{}, err
	}
	now := time.Now().UTC()
	saved := checkpoint.Clone()
	saved.Version = checkpoint.Version + 1
	if saved.CreatedAt.IsZero() {
		saved.CreatedAt = now
	}
	saved.UpdatedAt = now
	data, err := json.Marshal(saved)
	if err != nil {
		return corestate.Checkpoint{}, fmt.Errorf("encode Redis checkpoint: %w", err)
	}
	result, err := saveCheckpointScript.Run(
		ctx,
		s.client,
		[]string{s.key(checkpoint.ConversationID)},
		checkpoint.Version,
		data,
		s.ttl.Milliseconds(),
	).Slice()
	if err != nil {
		return corestate.Checkpoint{}, fmt.Errorf("save Redis checkpoint: %w", err)
	}
	if len(result) != 2 {
		return corestate.Checkpoint{}, fmt.Errorf("save Redis checkpoint returned %d values", len(result))
	}
	ok, err := redisInteger(result[0])
	if err != nil {
		return corestate.Checkpoint{}, err
	}
	actual, err := redisInteger(result[1])
	if err != nil {
		return corestate.Checkpoint{}, err
	}
	if ok != 1 {
		return corestate.Checkpoint{}, &corestate.ConflictError{
			ConversationID: checkpoint.ConversationID,
			Expected:       checkpoint.Version,
			Actual:         uint64(actual),
		}
	}
	return saved.Clone(), nil
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

func normalizeCheckpoint(checkpoint corestate.Checkpoint) corestate.Checkpoint {
	checkpoint = checkpoint.Clone()
	if checkpoint.SchemaVersion == 0 {
		checkpoint.SchemaVersion = corestate.CurrentCheckpointSchemaVersion
	}
	return checkpoint
}

func (s *RedisStore) Delete(ctx context.Context, conversationID string) error {
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return err
	}
	return s.client.Del(ctx, s.key(conversationID)).Err()
}

func (s *RedisStore) key(conversationID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(conversationID))
	return s.prefix + ":" + encoded
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
