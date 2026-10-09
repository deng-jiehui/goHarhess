package observe

import (
	"context"
	"sync"
	"sync/atomic"
)

// AsyncObserver 把事件经有界队列转交给下游 Observer：Emit 不再等待
// observer 处理，慢速观察者不会阻塞业务执行。Invoke 返回时队列内
// 事件可能尚未处理完；需要排空时调用 Close。
type AsyncObserver struct {
	inner        Observer
	queue        chan Event
	stop         chan struct{}
	drained      chan struct{}
	dropWhenFull bool
	dropped      atomic.Uint64
	closeOnce    sync.Once
}

// Async 包装下游 Observer。buffer 为队列容量；dropWhenFull 为真时队列
// 满即丢弃并计数（Dropped() 可读），为假时阻塞到有空位或 ctx 取消。
func Async(observer Observer, buffer int, dropWhenFull bool) *AsyncObserver {
	if buffer < 0 {
		buffer = 0
	}
	a := &AsyncObserver{
		inner:        observer,
		queue:        make(chan Event, buffer),
		stop:         make(chan struct{}),
		drained:      make(chan struct{}),
		dropWhenFull: dropWhenFull,
	}
	go a.drain()
	return a
}

func (a *AsyncObserver) OnEvent(ctx context.Context, event Event) {
	if a == nil || a.inner == nil {
		return
	}
	select {
	case <-a.stop:
		return
	default:
	}
	if a.dropWhenFull {
		select {
		case a.queue <- event:
		case <-a.stop:
		default:
			a.dropped.Add(1)
		}
		return
	}
	select {
	case a.queue <- event:
	case <-a.stop:
	case <-ctx.Done():
	}
}

// Dropped 返回因队列满而丢弃的事件数。
func (a *AsyncObserver) Dropped() uint64 {
	if a == nil {
		return 0
	}
	return a.dropped.Load()
}

// Close 停止接收新事件，等待队列排空后返回；之后 OnEvent 直接返回。
func (a *AsyncObserver) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() { close(a.stop) })
	<-a.drained
}

func (a *AsyncObserver) drain() {
	defer close(a.drained)
	handle := func(event Event) {
		dispatchOne(context.Background(), a.inner, event)
	}
	for {
		select {
		case event := <-a.queue:
			handle(event)
		case <-a.stop:
			// stop 后把已入队的事件处理完再退出。
			for {
				select {
				case event := <-a.queue:
					handle(event)
				default:
					return
				}
			}
		}
	}
}
