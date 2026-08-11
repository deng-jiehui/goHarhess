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
	cacheKeyVersion       = "v2"
)

type CacheEntry struct {
	Events    []corellm.Event `json:"events"`
	CreatedAt time.Time       `json:"created_at"`
}

func (e CacheEntry) Clone() CacheEntry {
	events := make([]corellm.Event, len(e.Events))
	for index := range e.Events {
		events[index] = e.Events[index].Clone()
	}
	return CacheEntry{Events: events, CreatedAt: e.CreatedAt}
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

// CachedModel decorates the canonical Model protocol. Provider wire formats
// remain private to the provider; the cache stores normalized model events.
type CachedModel struct {
	next               corellm.Model
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

var _ corellm.Model = (*CachedModel)(nil)

func NewCachedModel(next corellm.Model, cfg CacheConfig) (*CachedModel, error) {
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
		next:               next,
		store:              cfg.Store,
		modelID:            modelID,
		namespace:          namespace,
		ttl:                ttl,
		cacheFunctionCalls: cfg.CacheFunctionCalls,
		strictStoreErrors:  cfg.StrictStoreErrors,
		sensitiveDetector:  cfg.SensitiveDetector,
		flights:            map[string]*cacheFlight{},
	}, nil
}

func (m *CachedModel) Invoke(ctx context.Context, request corellm.Request, options ...runnable.Option) (corellm.Response, error) {
	stream, err := m.Stream(ctx, request, options...)
	if err != nil {
		return corellm.Response{}, err
	}
	return corellm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *CachedModel) Stream(ctx context.Context, request corellm.Request, options ...runnable.Option) (runnable.Stream[corellm.Event], error) {
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
		return replayEvents(ctx, entry.Events), nil
	}
	m.stats.misses.Add(1)

	m.mu.Lock()
	if flight := m.flights[key]; flight != nil {
		if flight.active() {
			m.stats.shared.Add(1)
			m.mu.Unlock()
			return flight.subscribe(ctx), nil
		}
		delete(m.flights, key)
	}
	flight := newCacheFlight(ctx)
	m.flights[key] = flight
	m.mu.Unlock()
	leader := flight.subscribe(ctx)

	upstream, err := m.next.Stream(flight.ctx, request, options...)
	if err != nil {
		flight.append(runnable.Item[corellm.Event]{Err: err})
		m.removeFlight(key, flight)
		flight.finish()
		flight.releaseSubscriber()
		return nil, err
	}
	if upstream == nil {
		err = errors.New("underlying model returned a nil stream")
		flight.append(runnable.Item[corellm.Event]{Err: err})
		m.removeFlight(key, flight)
		flight.finish()
		flight.releaseSubscriber()
		return nil, err
	}

	go m.consumeFlight(flight.ctx, key, flight, upstream)
	return leader, nil
}

func (m *CachedModel) Capabilities() corellm.Capabilities {
	return m.next.Capabilities()
}

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
		Hits:          m.stats.hits.Load(),
		Misses:        m.stats.misses.Load(),
		Writes:        m.stats.writes.Load(),
		Shared:        m.stats.shared.Load(),
		Bypasses:      m.stats.bypasses.Load(),
		StoreErrors:   m.stats.storeErrors.Load(),
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

func (m *CachedModel) consumeFlight(ctx context.Context, key string, flight *cacheFlight, upstream runnable.Stream[corellm.Event]) {
	entry := CacheEntry{CreatedAt: time.Now().UTC()}
	cacheable := true
	hasPayload := false
	done := corellm.Event{Kind: corellm.EventDone}

	for {
		var item runnable.Item[corellm.Event]
		var open bool
		select {
		case <-ctx.Done():
			m.removeFlight(key, flight)
			flight.finish()
			return
		case item, open = <-upstream:
		}
		if !open {
			break
		}
		item = cloneEventItem(item)
		if item.Err != nil {
			flight.append(item)
			m.removeFlight(key, flight)
			flight.finish()
			return
		}
		if item.Value.Kind == corellm.EventDone {
			done = item.Value.Clone()
			continue
		}
		hasPayload = true
		entry.Events = append(entry.Events, item.Value.Clone())
		flight.append(item)
		if containsFunctionCall(item.Value.Messages) && !m.cacheFunctionCalls {
			cacheable = false
		}
	}
	if err := ctx.Err(); err != nil {
		m.removeFlight(key, flight)
		flight.finish()
		return
	}

	if !hasPayload {
		cacheable = false
	}
	entry.Events = append(entry.Events, done.Clone())
	if cacheable {
		if err := m.store.Set(context.WithoutCancel(ctx), key, entry, m.ttl); err != nil {
			m.stats.storeErrors.Add(1)
			if m.strictStoreErrors {
				item := runnable.Item[corellm.Event]{Err: fmt.Errorf("write LLM cache: %w", err)}
				flight.append(item)
				m.removeFlight(key, flight)
				flight.finish()
				return
			}
		} else {
			m.stats.writes.Add(1)
		}
	} else {
		m.stats.skippedWrites.Add(1)
	}

	doneItem := runnable.Item[corellm.Event]{Value: done}
	flight.append(doneItem)
	m.removeFlight(key, flight)
	flight.finish()
}

func (m *CachedModel) removeFlight(key string, flight *cacheFlight) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.flights[key] == flight {
		delete(m.flights, key)
	}
}

type cacheFlight struct {
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	history     []runnable.Item[corellm.Event]
	changed     chan struct{}
	finished    bool
	subscribers int
}

func newCacheFlight(ctx context.Context) *cacheFlight {
	control, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &cacheFlight{ctx: control, cancel: cancel, changed: make(chan struct{})}
}

func (f *cacheFlight) subscribe(ctx context.Context) runnable.Stream[corellm.Event] {
	out := make(chan runnable.Item[corellm.Event])
	f.mu.Lock()
	f.subscribers++
	f.mu.Unlock()
	go func() {
		defer close(out)
		defer f.releaseSubscriber()
		index := 0
		for {
			f.mu.Lock()
			pending := cloneEventItems(f.history[index:])
			index = len(f.history)
			finished := f.finished
			changed := f.changed
			f.mu.Unlock()
			for _, item := range pending {
				if !emitEventItem(ctx, out, item) {
					return
				}
			}
			if finished {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}()
	return out
}

func (f *cacheFlight) releaseSubscriber() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribers > 0 {
		f.subscribers--
	}
	if f.subscribers == 0 && !f.finished {
		f.cancel()
	}
}

func (f *cacheFlight) active() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.finished && f.ctx.Err() == nil
}

func (f *cacheFlight) append(item runnable.Item[corellm.Event]) {
	f.mu.Lock()
	if f.finished {
		f.mu.Unlock()
		return
	}
	f.history = append(f.history, cloneEventItem(item))
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
}

func (f *cacheFlight) finish() {
	f.mu.Lock()
	if f.finished {
		f.mu.Unlock()
		return
	}
	f.finished = true
	close(f.changed)
	f.mu.Unlock()
	f.cancel()
}

func replayEvents(ctx context.Context, events []corellm.Event) runnable.Stream[corellm.Event] {
	out := make(chan runnable.Item[corellm.Event])
	go func() {
		defer close(out)
		for _, event := range events {
			if !emitEventItem(ctx, out, runnable.Item[corellm.Event]{Value: event}) {
				return
			}
		}
	}()
	return out
}

func emitEventItem(ctx context.Context, out chan<- runnable.Item[corellm.Event], item runnable.Item[corellm.Event]) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- cloneEventItem(item):
		return true
	}
}

func cloneEventItem(item runnable.Item[corellm.Event]) runnable.Item[corellm.Event] {
	return runnable.Item[corellm.Event]{Value: item.Value.Clone(), Err: item.Err}
}

func cloneEventItems(items []runnable.Item[corellm.Event]) []runnable.Item[corellm.Event] {
	cloned := make([]runnable.Item[corellm.Event], len(items))
	for index := range items {
		cloned[index] = cloneEventItem(items[index])
	}
	return cloned
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
		Functions:           functions,
		DeltaStream:         opts.DeltaStream,
		ExtraGenerateConfig: corellm.MergeGenerateConfig(opts.ExtraGenerateConfig, nil),
	}
}

func containsFunctionCall(messages []schema.Message) bool {
	for _, message := range messages {
		if message.FunctionCall != nil {
			return true
		}
	}
	return false
}
