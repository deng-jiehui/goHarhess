package graph

import (
	"context"
	"sync"
)

// newSlotPool 构造并发额度池：上限取根运行的 maxConcurrency，未设置
// （0）时不限制——与批内任务同一量级口径。
func newSlotPool(limit int) chan struct{} {
	if limit <= 0 {
		return nil
	}
	return make(chan struct{}, limit)
}

// slotLease 是一个实际工作单元持有的并发额度令牌。额度按工作领取、按
// 工作结束归还；工作单元进入嵌套图运行时先让出（yield），由图内部的
// 节点各自向同一额度池领取——包装层不持额度包围整图，上限 1 时不会
// 形成「外层持额度等内层、内层等外层释放」的等待环。运行结束后 resume
// 取回，外层工作单元恢复记账。
type slotLease struct {
	slots chan struct{}
	mu    sync.Mutex
	held  bool
}

type slotLeaseKey struct{}

func withSlotLease(ctx context.Context, lease *slotLease) context.Context {
	return context.WithValue(ctx, slotLeaseKey{}, lease)
}

func slotLeaseFromContext(ctx context.Context) *slotLease {
	lease, _ := ctx.Value(slotLeaseKey{}).(*slotLease)
	return lease
}

// acquireSlotLease 领取一个额度令牌；slots 为 nil（不限并发）时返回空
// 令牌。等待额度期间响应 ctx 取消。
func acquireSlotLease(ctx context.Context, slots chan struct{}) (*slotLease, error) {
	lease := &slotLease{slots: slots}
	if slots == nil {
		return lease, nil
	}
	select {
	case slots <- struct{}{}:
		lease.held = true
		return lease, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// yield 把持有的令牌归还额度池；重复调用安全。
func (l *slotLease) yield() {
	if l == nil || l.slots == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held {
		<-l.slots
		l.held = false
	}
}

// resume 在嵌套运行结束后取回令牌：额度被其他工作占用时等待其释放或
// ctx 取消。
func (l *slotLease) resume(ctx context.Context) {
	if l == nil || l.slots == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held {
		return
	}
	select {
	case l.slots <- struct{}{}:
		l.held = true
	case <-ctx.Done():
	}
}

// release 归还令牌并结束本工作单元的额度记账；已让出的令牌幂等返回。
func (l *slotLease) release() {
	l.yield()
}

// sharedSlots 返回本轮批任务的并发额度：协调器存在且根运行设了上限时
// 共享同一额度池；没有协调器但祖先工作单元持有额度时沿用其额度池
// （未登记嵌套图同样受根上限约束）；否则按本图上限独立建池。
func sharedSlots(ctx context.Context, coord *runCoordinator, limit int) chan struct{} {
	if coord != nil && coord.slots != nil {
		return coord.slots
	}
	if lease := slotLeaseFromContext(ctx); lease != nil && lease.slots != nil {
		return lease.slots
	}
	return newSlotPool(limit)
}
