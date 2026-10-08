package llm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corellm "github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

const (
	DefaultCacheTTL       = 5 * time.Minute
	DefaultCacheNamespace = "llm"
	cacheKeyVersion       = "v4"
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

// SensitiveDetector 用解析后的模型配置判断是否跳过缓存。
type SensitiveDetector func(ctx context.Context, messages []schema.Message, opts corellm.Options) bool

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
	// 缓存边界冻结全部组件配置：每种类型解析一次，解析值进入缓存身份，
	// 上游调用只应用冻结结果；扩展 Provider 配置同样计入缓存键，无法稳定
	// 编码为缓存身份时直接绕过而不是命中错误结果。
	specs, err := runnable.FlattenOptions(options)
	if err != nil {
		return nil, err
	}
	// 带剩余路径的选项不属于本组件可确定身份的参数，缓存一律绕过。
	for _, spec := range specs {
		if len(spec.Path) > 0 {
			m.stats.bypasses.Add(1)
			return m.next.Stream(ctx, request, options...)
		}
	}
	frozenSpecs, values, err := runnable.FreezeSpecs(specs)
	if err != nil {
		return nil, err
	}
	// 冻结后仍存在的原始 spec 是不可观测写入的透传（如显式赋零），
	// 效果取决于组件真实解析基底，无法纳入缓存身份，一律绕过。
	for _, spec := range frozenSpecs {
		if spec.ConfigType != nil && !spec.Frozen {
			m.stats.bypasses.Add(1)
			return m.next.Stream(ctx, request, runnable.OptionsFromSpecs(frozenSpecs)...)
		}
	}
	resolved, _ := values[runnable.OptionType[corellm.Options]()].(corellm.Options)
	upstreamOptions := runnable.OptionsFromSpecs(frozenSpecs)
	if m.shouldBypass(ctx, request, resolved) {
		m.stats.bypasses.Add(1)
		return m.next.Stream(ctx, request, upstreamOptions...)
	}

	key, err := m.cacheKey(ctx, request, values)
	if err != nil {
		if m.strictStoreErrors {
			return nil, fmt.Errorf("build LLM cache key: %w", err)
		}
		m.stats.bypasses.Add(1)
		return m.next.Stream(ctx, request, upstreamOptions...)
	}
	entry, found, err := m.store.Get(ctx, key)
	if err != nil {
		m.stats.storeErrors.Add(1)
		if m.strictStoreErrors {
			return nil, fmt.Errorf("read LLM cache: %w", err)
		}
		m.stats.bypasses.Add(1)
		return m.next.Stream(ctx, request, upstreamOptions...)
	}
	if found {
		m.stats.hits.Add(1)
		entry = entry.Clone()
		for i := range entry.Response.Messages {
			entry.Response.Messages[i].ID = stream.NewID()
		}
		if err := stream.PublishSnapshots(ctx, entry.Response.Messages); err != nil {
			return nil, err
		}
		return responseReader(ctx, entry.Response), nil
	}
	m.stats.misses.Add(1)
	if stream.Enabled(ctx) {
		return m.streamPublicMiss(ctx, key, request, upstreamOptions...)
	}

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

	upstream, err := m.next.Stream(flight.ctx, request, upstreamOptions...)
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

// Live callers own their producer and backpressure. They still use completed
// cache entries, but do not join in-flight requests with another run's Context.
func (m *CachedModel) streamPublicMiss(ctx context.Context, key string, request corellm.Request, options ...runnable.Option) (*runnable.StreamReader[corellm.Response], error) {
	execution, cancel := context.WithCancel(ctx)
	execution, seen := stream.TrackMessages(execution)
	upstream, err := m.next.Stream(execution, request, options...)
	if err != nil {
		cancel()
		return nil, err
	}
	if upstream == nil {
		cancel()
		return nil, errors.New("underlying model returned a nil stream")
	}
	reader, writer := runnable.Pipe[corellm.Response](context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		select {
		case <-reader.Context().Done():
			cancel()
			_ = upstream.Close()
		case <-done:
		}
	}()
	go func() {
		defer close(done)
		defer cancel()
		defer upstream.Close()
		defer writer.Close()
		response, err := corellm.CollectResponse(execution, upstream)
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if m.cacheFunctionCalls || !containsFunctionCall(response.Messages) {
			err = m.store.Set(execution, key, CacheEntry{Response: response.Clone(), CreatedAt: time.Now().UTC()}, m.ttl)
			if err != nil {
				m.stats.storeErrors.Add(1)
				if m.strictStoreErrors {
					_ = writer.CloseWithError(err)
					return
				}
			} else {
				m.stats.writes.Add(1)
			}
		} else {
			m.stats.skippedWrites.Add(1)
		}
		if !seen.Load() {
			if err := stream.PublishSnapshots(execution, response.Messages); err != nil {
				_ = writer.CloseWithError(err)
				return
			}
		}
		_ = writer.Send(response)
	}()
	return reader, nil
}

func (m *CachedModel) Collect(ctx context.Context, input *runnable.StreamReader[corellm.Request], options ...runnable.Option) (corellm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}

func (m *CachedModel) Transform(ctx context.Context, input *runnable.StreamReader[corellm.Request], options ...runnable.Option) (*runnable.StreamReader[corellm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}

func (m *CachedModel) Capabilities() corellm.Capabilities { return m.next.Capabilities() }

// OptionTypes 透传下游模型声明的扩展配置类型，让 Provider 专属选项能穿过
// 缓存包装层参与按类型分发；解析值已纳入缓存身份。
func (m *CachedModel) OptionTypes() []reflect.Type {
	if m == nil {
		return nil
	}
	if provider, ok := m.next.(interface{ OptionTypes() []reflect.Type }); ok && provider != nil {
		return provider.OptionTypes()
	}
	return nil
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

func (m *CachedModel) shouldBypass(ctx context.Context, request corellm.Request, opts corellm.Options) bool {
	if bypass, _ := ctx.Value(cacheBypassKey).(bool); bypass {
		return true
	}
	if sensitive, _ := ctx.Value(cacheSensitiveKey).(bool); sensitive {
		return true
	}
	if m.sensitiveDetector != nil && m.sensitiveDetector(ctx, schema.CloneMessages(request.Messages), cloneModelOptions(opts)) {
		return true
	}
	return len(request.Functions) > 0 && !m.cacheFunctionCalls
}

// cacheKey 的缓存身份包含请求业务输入与全部组件配置的解析值（按类型名分
// 组）。配置值经确定性结构指纹进入键身份：递归覆盖导出与未导出字段，
// 浮点按位比较（区分 NaN 载荷），指针按指向内容而非地址，GoStringer/
// Formatter 无法劫持输出；func、chan、unsafe.Pointer 等无法稳定表示
// 的种类报错，宽松模式绕过缓存、严格模式报错。
func (m *CachedModel) cacheKey(ctx context.Context, request corellm.Request, values map[reflect.Type]any) (string, error) {
	request = cloneRequest(request)
	// Framework message identity is transport metadata, not model input semantics.
	for i := range request.Messages {
		request.Messages[i].ID = ""
	}
	tenant, _ := ctx.Value(cacheTenantKey).(string)
	if tenant == "" {
		tenant = "default"
	}
	encoded := make(map[string]string, len(values))
	for configType, value := range values {
		fingerprint, err := configFingerprint(value)
		if err != nil {
			return "", fmt.Errorf("option value for %s has no stable cache identity: %w", configType, err)
		}
		encoded[configType.String()] = fingerprint
	}
	payload := struct {
		Version string            `json:"version"`
		ModelID string            `json:"model_id"`
		Request corellm.Request   `json:"request"`
		Options map[string]string `json:"options"`
	}{Version: cacheKeyVersion, ModelID: m.modelID, Request: cloneRequest(request), Options: encoded}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	tenantHash := sha256.Sum256([]byte(tenant))
	requestHash := sha256.Sum256(data)
	return fmt.Sprintf("%s:%s:%s:%s", m.namespace, cacheKeyVersion, hex.EncodeToString(tenantHash[:8]), hex.EncodeToString(requestHash[:])), nil
}

// configFingerprint 生成配置值的确定性结构指纹，用作缓存身份的一部分。
// 与 %#v 不同：不输出指针地址、不受 GoString/Format 方法影响、覆盖未
// 导出字段，无法安全表示的种类返回错误而不是生成可能碰撞的键。
func configFingerprint(value any) (string, error) {
	var out strings.Builder
	seen := map[fingerprintVisit]bool{}
	if err := writeFingerprint(reflect.ValueOf(value), &out, seen, 0); err != nil {
		return "", err
	}
	return out.String(), nil
}

type fingerprintVisit struct {
	typ reflect.Type
	ptr uintptr
}

const maxFingerprintDepth = 64

func writeFingerprint(v reflect.Value, out *strings.Builder, seen map[fingerprintVisit]bool, depth int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unreadable value: %v", r)
		}
	}()
	if depth > maxFingerprintDepth {
		return errors.New("fingerprint exceeds max depth")
	}
	if !v.IsValid() {
		out.WriteString("~")
		return nil
	}
	// 标量一律输出 v.Type() 而非 Kind：接口字段中底层类型相同但命名
	// 不同的具体类型（如 type Celsius float64 与 float64）必须区分。
	switch v.Kind() {
	case reflect.Bool:
		fmt.Fprintf(out, "%s:%v;", v.Type(), v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprintf(out, "%s:%d;", v.Type(), v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		fmt.Fprintf(out, "%s:%d;", v.Type(), v.Uint())
	case reflect.Float32:
		fmt.Fprintf(out, "%s:%x;", v.Type(), math.Float32bits(float32(v.Float())))
	case reflect.Float64:
		fmt.Fprintf(out, "%s:%x;", v.Type(), math.Float64bits(v.Float()))
	case reflect.Complex64, reflect.Complex128:
		c := v.Complex()
		fmt.Fprintf(out, "%s:%x:%x;", v.Type(), math.Float64bits(real(c)), math.Float64bits(imag(c)))
	case reflect.String:
		fmt.Fprintf(out, "%s:%q;", v.Type(), v.String())
	case reflect.Interface:
		if v.IsNil() {
			out.WriteString("inil;")
			return nil
		}
		return writeFingerprint(v.Elem(), out, seen, depth+1)
	case reflect.Pointer:
		if v.IsNil() {
			fmt.Fprintf(out, "%s:nil;", v.Type())
			return nil
		}
		visit := fingerprintVisit{typ: v.Type(), ptr: v.Pointer()}
		if seen[visit] {
			out.WriteString("cyc;")
			return nil
		}
		seen[visit] = true
		fmt.Fprintf(out, "%s*", v.Type().Elem())
		return writeFingerprint(v.Elem(), out, seen, depth+1)
	case reflect.Map:
		if v.IsNil() {
			fmt.Fprintf(out, "%s:nil;", v.Type())
			return nil
		}
		keys := v.MapKeys()
		pairs := make([]string, 0, len(keys))
		for _, key := range keys {
			var pair strings.Builder
			if err := writeFingerprint(key, &pair, seen, depth+1); err != nil {
				return err
			}
			pair.WriteByte('=')
			if err := writeFingerprint(v.MapIndex(key), &pair, seen, depth+1); err != nil {
				return err
			}
			pairs = append(pairs, pair.String())
		}
		sort.Strings(pairs)
		fmt.Fprintf(out, "%s{", v.Type())
		for _, pair := range pairs {
			out.WriteString(pair)
		}
		out.WriteByte('}')
	case reflect.Slice:
		if v.IsNil() {
			fmt.Fprintf(out, "%s:nil;", v.Type())
			return nil
		}
		fallthrough
	case reflect.Array:
		fmt.Fprintf(out, "%s[", v.Type())
		for i := 0; i < v.Len(); i++ {
			if err := writeFingerprint(v.Index(i), out, seen, depth+1); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case reflect.Struct:
		typ := v.Type()
		fmt.Fprintf(out, "%s{", typ)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			fmt.Fprintf(out, "%s=", field.Name)
			fv := v.Field(i)
			if field.PkgPath != "" {
				// 未导出字段只能取标量访问器；复合值无法可靠读取时
				// 放弃指纹而不是生成可能碰撞的键。
				switch fv.Kind() {
				case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
					reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
				default:
					return fmt.Errorf("unexported field %s.%s of kind %s cannot be fingerprinted", typ, field.Name, fv.Kind())
				}
			}
			if err := writeFingerprint(fv, out, seen, depth+1); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return fmt.Errorf("kind %s has no stable fingerprint", v.Kind())
	default:
		return fmt.Errorf("kind %s has no stable fingerprint", v.Kind())
	}
	return nil
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
	request.Functions = append([]schema.FunctionDefinition(nil), request.Functions...)
	return request
}

func cloneModelOptions(opts corellm.Options) corellm.Options {
	return corellm.Options{
		DeltaStream:         opts.DeltaStream,
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
