package retrieval

import (
	"context"
	"errors"

	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/runnable"
)

// scopedVector 把向量存储调用钉在当前租户：Namespace 一律取自
// tenant.FromContext，调用方显式传入其他命名空间时拒绝。
type scopedVector struct {
	next runnable.Runnable[VectorRequest, VectorResponse]
}

// Scoped 返回租户安全的向量存储视图：每次调用从 ctx 的租户作用域取
// Namespace 写入请求，并把同一个租户写进检索缓存上下文（供
// cache/retrieval 的键派生），两条路径不可能指向不同租户。
// ctx 无租户作用域返回 tenant.ErrNoScope。
func Scoped(next runnable.Runnable[VectorRequest, VectorResponse]) runnable.Runnable[VectorRequest, VectorResponse] {
	return &scopedVector{next: next}
}

func (s *scopedVector) Invoke(ctx context.Context, req VectorRequest, options ...runnable.Option) (VectorResponse, error) {
	scope, ok := tenant.FromContext(ctx)
	if !ok {
		return VectorResponse{}, tenant.ErrNoScope
	}
	tid := scope.TenantID
	switch req.Operation {
	case VectorQueryOperation:
		if req.Query == nil {
			return VectorResponse{}, errors.New("vector query is required")
		}
		if req.Query.Namespace != "" && req.Query.Namespace != tid {
			return VectorResponse{}, tenant.ErrTenantMismatch
		}
		req.Query.Namespace = tid
	case VectorUpsertOperation:
		for i := range req.Records {
			if req.Records[i].Namespace != "" && req.Records[i].Namespace != tid {
				return VectorResponse{}, tenant.ErrTenantMismatch
			}
			req.Records[i].Namespace = tid
		}
	case VectorDeleteOperation:
		if req.Namespace != "" && req.Namespace != tid {
			return VectorResponse{}, tenant.ErrTenantMismatch
		}
		req.Namespace = tid
	}
	ctx = WithNamespace(ctx, tid)
	return s.next.Invoke(ctx, req, options...)
}

func (s *scopedVector) Stream(ctx context.Context, input VectorRequest, options ...runnable.Option) (*runnable.StreamReader[VectorResponse], error) {
	return runnable.StreamFromInvoke(ctx, input, s.Invoke, options...)
}

func (s *scopedVector) Collect(ctx context.Context, input *runnable.StreamReader[VectorRequest], options ...runnable.Option) (VectorResponse, error) {
	return runnable.CollectFromInvoke(ctx, input, s.Invoke, options...)
}

func (s *scopedVector) Transform(ctx context.Context, input *runnable.StreamReader[VectorRequest], options ...runnable.Option) (*runnable.StreamReader[VectorResponse], error) {
	return runnable.TransformFromInvoke(ctx, input, s.Invoke, options...)
}
