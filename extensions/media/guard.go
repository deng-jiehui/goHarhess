package media

import (
	"context"
	"fmt"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// storeGuard 拒绝内联媒体过大的检查点：Save 前扫描 Messages 的媒体块，
// Data（JSON base64）超过阈值返回 ErrMediaTooLarge，防止内联字节撑大
// 存储与每个检查点版本。
type storeGuard struct {
	inner state.Store
	max   int64
}

// NewStoreGuard 创建检查点守卫装饰器：内联媒体超过 maxInlineBytes 的
// 检查点被拒绝保存。maxInlineBytes 按 Data 的编码长度计（约原始字节 4/3）。
func NewStoreGuard(inner state.Store, maxInlineBytes int) state.Store {
	if inner == nil {
		panic("media: inner store is nil")
	}
	return &storeGuard{inner: inner, max: int64(maxInlineBytes)}
}

func (s *storeGuard) check(checkpoint state.Checkpoint) error {
	for mi, message := range checkpoint.Messages {
		for bi, block := range message.ContentBlocks {
			if media := mediaOf(block); media != nil && int64(len(media.Data)) > s.max {
				return fmt.Errorf("%w: checkpoint messages[%d].content_blocks[%d] inline media is %d bytes > %d",
					ErrMediaTooLarge, mi, bi, len(media.Data), s.max)
			}
		}
	}
	return nil
}

func (s *storeGuard) Get(ctx context.Context, conversationID, checkpointID string) (state.Checkpoint, bool, error) {
	return s.inner.Get(ctx, conversationID, checkpointID)
}

func (s *storeGuard) List(ctx context.Context, conversationID string, options state.ListOptions) ([]state.Checkpoint, error) {
	return s.inner.List(ctx, conversationID, options)
}

func (s *storeGuard) Load(ctx context.Context, conversationID string) (state.Checkpoint, bool, error) {
	return s.inner.Load(ctx, conversationID)
}

func (s *storeGuard) Save(ctx context.Context, checkpoint state.Checkpoint) (state.Checkpoint, error) {
	if err := s.check(checkpoint); err != nil {
		return state.Checkpoint{}, err
	}
	return s.inner.Save(ctx, checkpoint)
}

func (s *storeGuard) Delete(ctx context.Context, conversationID string) error {
	return s.inner.Delete(ctx, conversationID)
}

// RollbackBasis 透传可选接口，同租户装饰器的做法。
func (s *storeGuard) RollbackBasis(ctx context.Context, conversationID string) (state.Checkpoint, bool, error) {
	reader, ok := s.inner.(state.RollbackBasisReader)
	if !ok {
		return state.Checkpoint{}, false, nil
	}
	return reader.RollbackBasis(ctx, conversationID)
}

var (
	_ state.Store               = (*storeGuard)(nil)
	_ state.RollbackBasisReader = (*storeGuard)(nil)
	_                           = schema.Message{} // 保持 schema 引用（MediaContent 遍历在 media.go）
)
