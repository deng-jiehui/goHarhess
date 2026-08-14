package llm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corellm "goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

const (
	DefaultCacheTTL       = 5 * time.Minute
	DefaultCacheNamespace = "llm"
	cacheKeyVersion       = "v3"
)

type CacheEntry struct {
	Response  corellm.Response `json:"response"`
	CreatedAt time.Time        `json:"created_at"`
}

func (e CacheEntry) Clone() CacheEntry {
	return CacheEntry{Response: e.Response.Clone(), CreatedAt: e.CreatedAt}
}

type CacheStore interface {
	Get(ctx context.Context, key string) (CacheEntry, bool, error)
	Set(ctx context.Context, key string, entry CacheEntry, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

type SensitiveDetector func(ctx context.Context, messages []schema.Message, opts corellm.ChatOptions) bool

type CacheConfig struct {
	Store              CacheStore
	ModelID            string
	Namespace          string
	TTL                time.Duration
	CacheFunctionCalls bool
	StrictStoreErrors  bool
	SensitiveDetector  SensitiveDetector
}

type CacheStats struct {
	Hits          uint64
	Misses        uint64
	Writes        uint64
	Shared        uint64
	Bypasses      uint64
	StoreErrors   uint64
	SkippedWrites uint64
}

type cacheCounters struct {
	hits          atomic.Uint64
	misses        atomic.Uint64
	writes        atomic.Uint64
	shared        atomic.Uint64
	bypasses      atomic.Uint64
	storeErrors   atomic.Uint64
	skippedWrites atomic.Uint64
}

// CachedModel stores one complete provider-neutral Response per request. Live
// model chunks remain on the caller's Context EventStream and never enter the
// cache data plane.
type CachedModel struct {
	next               corellm.ChatModel
	store              CacheStore
	modelID            string
	namespace          string
	ttl                time.Duration
	cacheFunctionCalls bool
	strictStoreErrors  bool
	sensitiveDetector  SensitiveDetector

	mu      sync.Mutex
	flights map[string]*cacheFlight
	stats   cacheCounters
}

var _ corellm.ChatModel = (*CachedModel)(nil)

func NewCachedModel(next corellm.ChatModel, cfg CacheConfig) (*CachedModel, error) {
	if next == nil {
		return nil, errors.New("cached model requires an underlying model")
	}
	if cfg.Store == nil {
		return nil, errors.New("cached model requires a cache store")
	}
	modelID := strings.TrimSpace(cfg.ModelID)
	if modelID == "" {
		return nil, errors.New("cached model requires a model ID")
	}
	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = DefaultCacheNamespace
	}
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = DefaultCacheTTL
	}
	if ttl < 0 {
		return nil, fmt.Errorf("cache TTL cannot be negative: %s", ttl)
	}
	return &CachedModel{
		next: next, store: cfg.Store, modelID: modelID, namespace: namespace, ttl: ttl,
		cacheFunctionCalls: cfg.CacheFunctionCalls, strictStoreErrors: cfg.StrictStoreErrors,
		sensitiveDetector: cfg.SensitiveDetector, flights: map[string]*cacheFlight{},
	}, nil
}

func (m *CachedModel) Invoke(ctx context.Context, request corellm.Request, options ...runnable.Option) (corellm.Response, error) {
	reader, err := m.Stream(ctx, request, options...)
	if err != nil {
		return corellm.Response{}, err
	}
	return corellm.CollectResponse(ctx, reader)
}

func (m *CachedModel) Stream(ctx context.Context, request corellm.Request, options ...runnable.Option) (*runnable.StreamReader[corellm.Response], error) {
	if ctx == nil {
		return nil, errors.New("cached model context cannot be nil")
	}
	request = cloneRequest(request)
	if m.shouldBypass(ctx, request.Messages, request.Options) {
		m.stats.bypasses.Add(1)
		return m.next.Stream(ctx, request, options...)
	}

	key, err := m.cacheKey(ctx, request)
	if err != nil {
		if m.strictStoreErrors {
			return nil, fmt.Errorf("build LLM cache key: %w", err)
		}
		m.stats.bypasses.Add(1)
		return m.next.Stream(ctx, request, options...)
	}
	entry, found, err := m.store.Get(ctx, key)
	if err != nil {
		m.stats.storeErrors.Add(1)
		if m.strictStoreErrors {
			return nil, fmt.Errorf("read LLM cache: %w", err)
		}
		m.stats.bypasses.Add(1)
		return m.next.Stream(ctx, request, options...)
	}
	if found {
		m.stats.hits.Add(1)
		return responseReader(ctx, entry.Response), nil
	}
	m.stats.misses.Add(1)

	m.mu.Lock()
	if flight := m.flights[key]; flight != nil && flight.active() {
		m.stats.shared.Add(1)
		reader := flight.subscribe(ctx)
		m.mu.Unlock()
		return reader, nil
	}
	flight := newCacheFlight(ctx)
	m.flights[key] = flight
	leader := flight.subscribe(ctx)
	m.mu.Unlock()

	upstream, err := m.next.Stream(flight.ctx, request, options...)
	if err != nil {
		flight.complete(corellm.Response{}, err)
		m.removeFlight(key, flight)
		_ = leader.Close()
		return nil, err
	}
	if upstream == nil {
		err = errors.New("underlying model returned a nil stream")
		flight.complete(corellm.Response{}, err)
		m.removeFlight(key, flight)
		_ = leader.Close()
		return nil, err
	}
	go m.consumeFlight(key, flight, upstream)
	return leader, nil
}

func (m *CachedModel) Collect(ctx context.Context, input *runnable.StreamReader[corellm.Request], options ...runnable.Option) (corellm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}

func (m *CachedModel) Transform(ctx context.Context, input *runnable.StreamReader[corellm.Request], options ...runnable.Option) (*runnable.StreamReader[corellm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}

func (m *CachedModel) Capabilities() corellm.Capabilities { return m.next.Capabilities() }

func (m *CachedModel) ModelInfo() corellm.ModelInfo {
	info := corellm.DescribeModel(m.next)
	if info.Name == "" {
		info.Name = m.modelID
	}
	if info.Attributes == nil {
		info.Attributes = map[string]any{}
	}
	info.Attributes["cache.enabled"] = true
	info.Attributes["cache.namespace"] = m.namespace
	return info
}

func (m *CachedModel) Stats() CacheStats {
	return CacheStats{
		Hits: m.stats.hits.Load(), Misses: m.stats.misses.Load(), Writes: m.stats.writes.Load(),
		Shared: m.stats.shared.Load(), Bypasses: m.stats.bypasses.Load(), StoreErrors: m.stats.storeErrors.Load(),
		SkippedWrites: m.stats.skippedWrites.Load(),
	}
}

type cacheContextKey int

const (
	cacheTenantKey cacheContextKey = iota
	cacheBypassKey
	cacheSensitiveKey
)

func WithCacheTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, cacheTenantKey, strings.TrimSpace(tenant))
}
func WithCacheBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheBypassKey, true)
}
func WithSensitiveRequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheSensitiveKey, true)
}

func (m *CachedModel) shouldBypass(ctx context.Context, messages []schema.Message, opts corellm.ChatOptions) bool {
	if bypass, _ := ctx.Value(cacheBypassKey).(bool); bypass {
		return true
	}
	if sensitive, _ := ctx.Value(cacheSensitiveKey).(bool); sensitive {
		return true
	}
	if m.sensitiveDetector != nil && m.sensitiveDetector(ctx, schema.CloneMessages(messages), cloneChatOptions(opts)) {
		return true
	}
	return len(opts.Functions) > 0 && !m.cacheFunctionCalls
}

func (m *CachedModel) cacheKey(ctx context.Context, request corellm.Request) (string, error) {
	tenant, _ := ctx.Value(cacheTenantKey).(string)
	if tenant == "" {
		tenant = "default"
	}
	payload := struct {
		Version string          `json:"version"`
		ModelID string          `json:"model_id"`
		Request corellm.Request `json:"request"`
	}{Version: cacheKeyVersion, ModelID: m.modelID, Request: cloneRequest(request)}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	tenantHash := sha256.Sum256([]byte(tenant))
	requestHash := sha256.Sum256(data)
	return fmt.Sprintf("%s:%s:%s:%s", m.namespace, cacheKeyVersion, hex.EncodeToString(tenantHash[:8]), hex.EncodeToString(requestHash[:])), nil
}

func (m *CachedModel) consumeFlight(key string, flight *cacheFlight, upstream *runnable.StreamReader[corellm.Response]) {
	defer upstream.Close()
	response, err := corellm.CollectResponse(flight.ctx, upstream)
	if err != nil {
		flight.complete(corellm.Response{}, err)
		m.removeFlight(key, flight)
		return
	}
	cacheable := m.cacheFunctionCalls || !containsFunctionCall(response.Messages)
	if cacheable {
		entry := CacheEntry{Response: response.Clone(), CreatedAt: time.Now().UTC()}
		if err := m.store.Set(context.WithoutCancel(flight.ctx), key, entry, m.ttl); err != nil {
			m.stats.storeErrors.Add(1)
			if m.strictStoreErrors {
				flight.complete(corellm.Response{}, fmt.Errorf("write LLM cache: %w", err))
				m.removeFlight(key, flight)
				return
			}
		} else {
			m.stats.writes.Add(1)
		}
	} else {
		m.stats.skippedWrites.Add(1)
	}
	flight.complete(response, nil)
	m.removeFlight(key, flight)
}

func (m *CachedModel) removeFlight(key string, flight *cacheFlight) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.flights[key] == flight {
		delete(m.flights, key)
	}
}

type cacheFlight struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu          sync.Mutex
	response    corellm.Response
	err         error
	finished    bool
	subscribers int
}

func newCacheFlight(ctx context.Context) *cacheFlight {
	flightCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &cacheFlight{ctx: flightCtx, cancel: cancel, done: make(chan struct{})}
}

func (f *cacheFlight) subscribe(ctx context.Context) *runnable.StreamReader[corellm.Response] {
	reader, writer := runnable.Pipe[corellm.Response](context.WithoutCancel(ctx))
	f.mu.Lock()
	f.subscribers++
	f.mu.Unlock()
	go func() {
		defer f.releaseSubscriber()
		select {
		case <-ctx.Done():
			_ = writer.CloseWithError(ctx.Err())
		case <-reader.Context().Done():
		case <-f.done:
			response, err := f.result()
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			if err := writer.Send(response); err != nil {
				return
			}
			_ = writer.Close()
		}
	}()
	return reader
}

func (f *cacheFlight) releaseSubscriber() {
	f.mu.Lock()
	if f.subscribers > 0 {
		f.subscribers--
	}
	shouldCancel := f.subscribers == 0 && !f.finished
	f.mu.Unlock()
	if shouldCancel {
		f.cancel()
	}
}

func (f *cacheFlight) active() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.finished && f.ctx.Err() == nil
}

func (f *cacheFlight) complete(response corellm.Response, err error) {
	f.mu.Lock()
	if f.finished {
		f.mu.Unlock()
		return
	}
	f.response = response.Clone()
	f.err = err
	f.finished = true
	close(f.done)
	f.mu.Unlock()
	f.cancel()
}

func (f *cacheFlight) result() (corellm.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.response.Clone(), f.err
}

func responseReader(ctx context.Context, response corellm.Response) *runnable.StreamReader[corellm.Response] {
	reader, writer := runnable.Pipe[corellm.Response](context.WithoutCancel(ctx))
	go func() {
		if err := writer.Send(response.Clone()); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader
}

func cloneRequest(request corellm.Request) corellm.Request {
	request.Messages = schema.CloneMessages(request.Messages)
	request.Options = cloneChatOptions(request.Options)
	return request
}

func cloneChatOptions(opts corellm.ChatOptions) corellm.ChatOptions {
	functions := make([]schema.FunctionDefinition, len(opts.Functions))
	copy(functions, opts.Functions)
	return corellm.ChatOptions{
		Functions: functions, DeltaStream: opts.DeltaStream,
		ExtraGenerateConfig: corellm.MergeGenerateConfig(opts.ExtraGenerateConfig, nil),
	}
}

func containsFunctionCall(messages []schema.Message) bool {
	for _, message := range messages {
		if len(message.FunctionToolCalls()) > 0 {
			return true
		}
	}
	return false
}
