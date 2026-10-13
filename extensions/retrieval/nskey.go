package retrieval

import "context"

// namespaceKey 是检索请求在 ctx 上的生效命名空间载体：Scoped 写入，
// 检索缓存（cache/retrieval）读取，保证存储与缓存使用同一租户来源。
type namespaceKey struct{}

// WithNamespace 标记本次检索调用的生效命名空间。由 Scoped 写入；
// 直接调用存储时一般不需要手动设置。
func WithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, namespaceKey{}, namespace)
}

// NamespaceFromContext 返回调用生效的检索命名空间。
func NamespaceFromContext(ctx context.Context) (string, bool) {
	ns, ok := ctx.Value(namespaceKey{}).(string)
	return ns, ok
}
