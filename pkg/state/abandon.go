package state

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrUnfinishedRun 表示会话停在未完成的运行上，新输入需先 Resume 或 AbandonRun。
	ErrUnfinishedRun = errors.New("checkpoint has an unfinished run; use Resume or AbandonRun")
	// ErrNoUnfinishedRun 表示会话没有可放弃的运行。
	ErrNoUnfinishedRun = errors.New("checkpoint has no unfinished run")
)

// AbandonRun 放弃会话中未完成的运行：追加一条 abandoned 记录，清除执行数据，
// 并把会话内容与 Values 回退到该运行开始前。被放弃的记录仍保留在历史中。
// 写入与其他保存一样受版本检查保护，并发写入会返回 ErrConflict。
func AbandonRun(ctx context.Context, store Store, conversationID string) (Checkpoint, error) {
	if store == nil || (reflect.ValueOf(store).Kind() == reflect.Pointer && reflect.ValueOf(store).IsNil()) {
		return Checkpoint{}, errors.New("abandon run requires a checkpoint store")
	}
	if err := validateContextAndID(ctx, conversationID); err != nil {
		return Checkpoint{}, err
	}
	latest, found, err := store.Load(ctx, conversationID)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("load checkpoint %q: %w", conversationID, err)
	}
	if !found || !latest.Unfinished() {
		return Checkpoint{}, fmt.Errorf("%w: %q", ErrNoUnfinishedRun, conversationID)
	}
	// 保留策略可能已淘汰父链中间记录：支持 RollbackBasis 的 Store 会独立
	// 保留回退依据，优先使用；其余 Store 沿父链遍历。
	var base Checkpoint
	if reader, ok := store.(RollbackBasisReader); ok {
		anchor, found, rerr := reader.RollbackBasis(ctx, conversationID)
		if rerr != nil {
			return Checkpoint{}, rerr
		}
		if !found {
			// 首轮运行没有运行前记录，回退为空会话。
			base = Checkpoint{}
		} else {
			base = anchor
		}
	} else {
		var rerr error
		base, rerr = runBase(ctx, store, latest)
		if rerr != nil {
			return Checkpoint{}, rerr
		}
	}
	next := latest.Clone()
	next.Status = StatusAbandoned
	next.Execution = nil
	next.Result = nil
	next.Messages = cloneMessages(base.Messages)
	next.Values = cloneMap(base.Values)
	return store.Save(ctx, next)
}

// runBase 沿父链找到当前运行开始前的最后一条记录；会话首轮运行没有前驱，返回空记录。
func runBase(ctx context.Context, store Store, latest Checkpoint) (Checkpoint, error) {
	current := latest
	for current.ParentID != "" {
		parent, found, err := store.Get(ctx, latest.ConversationID, current.ParentID)
		if err != nil {
			return Checkpoint{}, fmt.Errorf("load checkpoint %q history: %w", latest.ConversationID, err)
		}
		if !found {
			return Checkpoint{}, fmt.Errorf("checkpoint %q history record %q is missing", latest.ConversationID, current.ParentID)
		}
		if parent.RunID != latest.RunID {
			return parent, nil
		}
		current = parent
	}
	return Checkpoint{}, nil
}
