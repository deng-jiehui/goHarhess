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
// 形成「外层持额度等内层、内层等外层释放」的等待环。
//
// 多个并发子调用共享同一个父令牌时，让出按借用计数（outstanding）：
// 先结束的子调用只递减计数，不把令牌提前取回——否则最后仍在运行的
// 子调用会因额度被父令牌重新占住而与父节点互等。全部借用归还后
// （resume 使计数归零）父工作单元才重新领取额度恢复记账。
type slotLease struct {
	slots chan struct{}
	mu    sync.Mutex
	held  bool
	// outstanding 已让出且尚未归还的借用数；每次嵌套运行 yield 加一、
	// resume 减一，与 held/released 在同一锁下更新。
	outstanding int
	// released 标记父工作单元已结束记账：其后的 resume 只递减借用，
	// 不再尝试取回令牌。
	released bool
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

// yield 登记一次借用并把持有的令牌归还额度池；重复借用按计数累计，
// 已归还的令牌不会重复退池。
func (l *slotLease) yield() {
	if l == nil || l.slots == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.outstanding++
	if l.held {
		<-l.slots
		l.held = false
	}
}

// resume 归还一次借用：还有借用未归还时直接返回（令牌留给在跑的兄弟
// 调用）；最后一笔归还且令牌已被让出时重新领取额度，额度被其他工作
// 占用时等待其释放或 ctx 取消。
func (l *slotLease) resume(ctx context.Context) {
	if l == nil || l.slots == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.outstanding--
	if l.outstanding > 0 || l.held || l.released {
		return
	}
	select {
	case l.slots <- struct{}{}:
		l.held = true
	case <-ctx.Done():
	}
}

// release 结束本工作单元的额度记账：归还仍持有的令牌，标记不再取回；
// 尚未归还的借用在各自 resume 时递减，不再尝试恢复持有。
func (l *slotLease) release() {
	if l == nil || l.slots == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released = true
	if l.held {
		<-l.slots
		l.held = false
	}
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
