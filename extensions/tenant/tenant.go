// Package tenant 提供统一的租户作用域入口：一次设置工具主体、观测元数据
// 与会话 key 前缀，让“忘了加租户”在构造层面做不到。
package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// 租户与用户标识只允许 [A-Za-z0-9._-]、长度 1 到 64：用 '/' 作 key 分隔符
// 不会产生歧义。会话部分只限制长度。
const (
	maxIDLen      = 64
	maxSessionLen = 256
)

var (
	// ErrNoScope 表示 ctx 上没有租户作用域；Store 与 SessionKey 拒绝放行。
	ErrNoScope = errors.New("tenant scope missing in context")
	// ErrTenantMismatch 表示会话 key 不属于当前租户作用域；越权访问
	// 返回错误而不是空结果，避免把越权当成“不存在”。
	ErrTenantMismatch = errors.New("conversation key does not belong to the tenant")
	// ErrInvalidID 表示租户/用户/会话标识不合法。
	ErrInvalidID = errors.New("tenant id segment invalid")
)

// Scope 是 ctx 上携带的租户作用域。
type Scope struct {
	TenantID string
	UserID   string
}

type scopeContextKey struct{}

func validID(id string) bool {
	if len(id) == 0 || len(id) > maxIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// With 校验 subject 的 TenantID 与 ID 后一次设置三件事：
// 1. tool.WithSubject(ctx, subject)
// 2. observe.WithMetadata(ctx, {tenant_id, user_id})
// 3. 在 ctx 中记录作用域，供 Store 装饰器与 SessionKey 使用
func With(ctx context.Context, subject tool.Subject) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !validID(subject.TenantID) || !validID(subject.ID) {
		return ctx, fmt.Errorf("%w: tenant %q user %q", ErrInvalidID, subject.TenantID, subject.ID)
	}
	scope := Scope{TenantID: subject.TenantID, UserID: subject.ID}
	ctx = tool.WithSubject(ctx, subject)
	ctx = observe.WithMetadata(ctx, map[string]any{
		observe.AttrTenantID: scope.TenantID,
		observe.AttrUserID:   scope.UserID,
	})
	return context.WithValue(ctx, scopeContextKey{}, scope), nil
}

// FromContext 返回当前租户作用域。
func FromContext(ctx context.Context) (Scope, bool) {
	if ctx == nil {
		return Scope{}, false
	}
	scope, ok := ctx.Value(scopeContextKey{}).(Scope)
	return scope, ok
}

// SessionKey 生成带租户前缀的会话 key：t/<租户>/u/<用户>/s/<会话>。
// ctx 无作用域返回 ErrNoScope；标识不合法返回 ErrInvalidID。
func SessionKey(ctx context.Context, sessionID string) (string, error) {
	scope, ok := FromContext(ctx)
	if !ok {
		return "", ErrNoScope
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || len(sessionID) > maxSessionLen || strings.ContainsAny(sessionID, "\x00\r\n") {
		return "", fmt.Errorf("%w: session %q", ErrInvalidID, sessionID)
	}
	return "t/" + scope.TenantID + "/u/" + scope.UserID + "/s/" + sessionID, nil
}

// storeConfig 是 Store 装饰器的选项集合。
type storeConfig struct {
	strictUser bool
}

// StoreOption 调整 Store 装饰器行为。
type StoreOption func(*storeConfig)

// StrictUser 额外要求会话 key 含 "u/<用户>/" 段：同租户内的用户之间也隔离。
func StrictUser() StoreOption {
	return func(c *storeConfig) { c.strictUser = true }
}

// tenantStore 包装任意 state.Store：每个方法先从 ctx 取作用域并要求
// 会话 key 以 "t/<租户>/" 开头（严格模式再要求 "u/<用户>/"），否则拒绝。
// 子 Agent 与嵌套子执行的 key 在会话 key 后追加，前缀不变，天然通过校验。
type tenantStore struct {
	inner  state.Store
	config storeConfig
}

// NewStore 创建租户 Store 装饰器；应用只应暴露装饰后的 Store。
func NewStore(inner state.Store, opts ...StoreOption) state.Store {
	if inner == nil {
		panic("tenant: inner store is nil")
	}
	config := storeConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	return &tenantStore{inner: inner, config: config}
}

// authorize 校验作用域与会话 key 前缀，返回 nil 表示放行。
func (s *tenantStore) authorize(ctx context.Context, conversationID string) error {
	scope, ok := FromContext(ctx)
	if !ok {
		return ErrNoScope
	}
	prefix := "t/" + scope.TenantID + "/"
	if s.config.strictUser {
		prefix += "u/" + scope.UserID + "/"
	}
	if !strings.HasPrefix(conversationID, prefix) {
		return fmt.Errorf("%w: key %q outside %q", ErrTenantMismatch, conversationID, prefix)
	}
	return nil
}

func (s *tenantStore) Get(ctx context.Context, conversationID, checkpointID string) (state.Checkpoint, bool, error) {
	if err := s.authorize(ctx, conversationID); err != nil {
		return state.Checkpoint{}, false, err
	}
	return s.inner.Get(ctx, conversationID, checkpointID)
}

func (s *tenantStore) List(ctx context.Context, conversationID string, options state.ListOptions) ([]state.Checkpoint, error) {
	if err := s.authorize(ctx, conversationID); err != nil {
		return nil, err
	}
	return s.inner.List(ctx, conversationID, options)
}

func (s *tenantStore) Load(ctx context.Context, conversationID string) (state.Checkpoint, bool, error) {
	if err := s.authorize(ctx, conversationID); err != nil {
		return state.Checkpoint{}, false, err
	}
	return s.inner.Load(ctx, conversationID)
}

func (s *tenantStore) Save(ctx context.Context, checkpoint state.Checkpoint) (state.Checkpoint, error) {
	if err := s.authorize(ctx, checkpoint.ConversationID); err != nil {
		return state.Checkpoint{}, err
	}
	return s.inner.Save(ctx, checkpoint)
}

func (s *tenantStore) Delete(ctx context.Context, conversationID string) error {
	if err := s.authorize(ctx, conversationID); err != nil {
		return err
	}
	return s.inner.Delete(ctx, conversationID)
}

// RollbackBasis 透传可选接口：引擎对 Store 做类型断言使用
// state.RollbackBasisReader，装饰器不实现会让放弃运行的回退静默失效。
// 内层未实现时返回未找到，调用方回退到父链遍历。
func (s *tenantStore) RollbackBasis(ctx context.Context, conversationID string) (state.Checkpoint, bool, error) {
	if err := s.authorize(ctx, conversationID); err != nil {
		return state.Checkpoint{}, false, err
	}
	reader, ok := s.inner.(state.RollbackBasisReader)
	if !ok {
		return state.Checkpoint{}, false, nil
	}
	return reader.RollbackBasis(ctx, conversationID)
}

var (
	_ state.Store               = (*tenantStore)(nil)
	_ state.RollbackBasisReader = (*tenantStore)(nil)
)
