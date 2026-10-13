// Package limits 提供容量治理组件。Bulkhead 包装任意 Runnable，给出
// 并发上限、有界排队、单租户限额与停机排空（Drain）语义。
package limits

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/runnable"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ErrOverloaded 在舱壁已满且不排队（或排队超限/排队超时）时返回；
// ErrDraining 在 Drain 已开始后拒绝新调用。两者都不阻塞调用方。
var (
	ErrOverloaded = errors.New("limits: bulkhead overloaded")
	ErrDraining   = errors.New("limits: bulkhead is draining")
)

// BulkheadConfig 配置舱壁。
type BulkheadConfig struct {
	MaxConcurrent int           // 进程内同时运行的上限；0 表示不限并发（仍可 Drain）
	MaxQueue      int           // 等待队列长度，0 表示不排队
	QueueTimeout  time.Duration // 排队等待上限，0 表示随调用方 ctx
	PerTenant     int           // 单租户并发上限，0 表示不限
	// TenantKey 取运行所属租户的键；默认读 tenant.FromContext 的租户 ID，
	// 无作用域记为空串（共享同一桶）。PerTenant=0 时不会调用。
	TenantKey func(context.Context) string
	// Meter 可选：非空时上报 goharness.limits.bulkhead（inflight/queued 观察值
	// 与 rejected 计数）。nil 关闭指标。
	Meter metric.Meter
}

// Bulkhead 包装一个 Runnable：自身也是 Runnable，可直接替换被包装对象。
type Bulkhead[I, O any] struct {
	next runnable.Runnable[I, O]
	cfg  BulkheadConfig

	slots chan struct{} // 全局并发名额，nil 表示不限
	queue chan struct{} // 排队名额，nil 表示不排队

	mu       sync.Mutex
	draining bool
	inflight map[*inflightRun[I, O]]struct{}
	tenants  map[string]*tenantGate
	queued   int

	rejectedTotal uint64
	metrics       bulkheadMetrics
}

type tenantGate struct {
	slots chan struct{}
	refs  int // 持有或等待名额的调用数；归零时从表中删除，防止表无界增长
}

type inflightRun[I, O any] struct {
	runCtx context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}
}

type bulkheadMetrics struct {
	inflight metric.Int64ObservableGauge
	queued   metric.Int64ObservableGauge
	rejected metric.Int64Counter
}

// NewBulkhead 校验配置并包装 next。
func NewBulkhead[I, O any](next runnable.Runnable[I, O], cfg BulkheadConfig) (*Bulkhead[I, O], error) {
	if next == nil {
		return nil, errors.New("limits: bulkhead requires a runnable")
	}
	if cfg.MaxConcurrent < 0 || cfg.MaxQueue < 0 || cfg.PerTenant < 0 || cfg.QueueTimeout < 0 {
		return nil, fmt.Errorf("limits: negative bulkhead field: %+v", cfg)
	}
	b := &Bulkhead[I, O]{
		next:     next,
		cfg:      cfg,
		inflight: map[*inflightRun[I, O]]struct{}{},
		tenants:  map[string]*tenantGate{},
	}
	if cfg.MaxConcurrent > 0 {
		b.slots = make(chan struct{}, cfg.MaxConcurrent)
	}
	if cfg.MaxQueue > 0 {
		b.queue = make(chan struct{}, cfg.MaxQueue)
	}
	if cfg.TenantKey == nil {
		b.cfg.TenantKey = func(ctx context.Context) string {
			if scope, ok := tenant.FromContext(ctx); ok {
				return scope.TenantID
			}
			return ""
		}
	}
	if cfg.Meter != nil {
		if err := b.initMetrics(cfg.Meter); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (b *Bulkhead[I, O]) initMetrics(meter metric.Meter) error {
	var err error
	b.metrics.inflight, err = meter.Int64ObservableGauge("goharness.limits.bulkhead",
		metric.WithDescription("bulkhead in-flight and queued runs"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			b.mu.Lock()
			inflight, queued := len(b.inflight), b.queued
			b.mu.Unlock()
			o.Observe(int64(inflight), metric.WithAttributes(attribute.String("state", "inflight")))
			o.Observe(int64(queued), metric.WithAttributes(attribute.String("state", "queued")))
			return nil
		}))
	if err != nil {
		return err
	}
	b.metrics.rejected, err = meter.Int64Counter("goharness.limits.bulkhead.rejected",
		metric.WithDescription("bulkhead rejected runs"),
		metric.WithUnit("{count}"))
	return err
}

func (b *Bulkhead[I, O]) Invoke(ctx context.Context, input I, options ...runnable.Option) (O, error) {
	var zero O
	run, err := b.acquire(ctx)
	if err != nil {
		return zero, err
	}
	defer b.release(run)
	return b.next.Invoke(run.ctx(), input, options...)
}

func (b *Bulkhead[I, O]) Stream(ctx context.Context, input I, options ...runnable.Option) (*runnable.StreamReader[O], error) {
	run, err := b.acquire(ctx)
	if err != nil {
		return nil, err
	}
	upstream, err := b.next.Stream(run.ctx(), input, options...)
	if err != nil {
		b.release(run)
		return nil, err
	}
	// 名额在读端关闭或结束时才释放：与 lifecycle.wrap 相同的方式包装。
	reader, writer := runnable.Pipe[O](run.ctx())
	go func() {
		defer b.release(run)
		defer upstream.Close()
		for {
			value, recvErr := upstream.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, context.Canceled) && context.Cause(run.ctx()) != nil {
					recvErr = context.Cause(run.ctx())
				}
				_ = writer.CloseWithError(recvErr)
				return
			}
			if err := writer.Send(value); err != nil {
				return
			}
		}
	}()
	return reader, nil
}

func (b *Bulkhead[I, O]) Collect(ctx context.Context, input *runnable.StreamReader[I], options ...runnable.Option) (O, error) {
	return runnable.CollectFromInvoke(ctx, input, b.Invoke, options...)
}

func (b *Bulkhead[I, O]) Transform(ctx context.Context, input *runnable.StreamReader[I], options ...runnable.Option) (*runnable.StreamReader[O], error) {
	return runnable.TransformFromInvoke(ctx, input, b.Invoke, options...)
}

// acquire 取全局与租户名额；通过后用 WithCancelCause 登记在途运行。
func (b *Bulkhead[I, O]) acquire(ctx context.Context) (*inflightRun[I, O], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	if b.draining {
		b.mu.Unlock()
		return nil, ErrDraining
	}
	b.mu.Unlock()

	var gate *tenantGate
	if b.cfg.PerTenant > 0 {
		var err error
		gate, err = b.acquireTenant(ctx)
		if err != nil {
			return nil, err
		}
	}
	releaseGate := func() {
		if gate != nil {
			b.releaseTenant(b.tenantKey(ctx), gate)
		}
	}
	if b.slots != nil {
		queued, err := b.enterQueue()
		if err != nil {
			releaseGate()
			return nil, err
		}
		var timer *time.Timer
		var timeout <-chan time.Time
		if b.cfg.QueueTimeout > 0 {
			timer = time.NewTimer(b.cfg.QueueTimeout)
			timeout = timer.C
		}
		select {
		case b.slots <- struct{}{}:
		case <-timeout:
			b.leaveQueue(queued)
			releaseGate()
			b.countRejected()
			return nil, ErrOverloaded
		case <-ctx.Done():
			b.leaveQueue(queued)
			releaseGate()
			return nil, ctx.Err()
		}
		if timer != nil {
			timer.Stop()
		}
		b.leaveQueue(queued)
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	run := &inflightRun[I, O]{runCtx: runCtx, cancel: cancel, done: make(chan struct{})}
	b.mu.Lock()
	if b.draining {
		// acquire 与 Drain 竞态：已拿到名额但排空开始，按排空语义取消本运行。
		b.mu.Unlock()
		cancel(control.RequestInterrupt("draining", nil))
	}
	b.inflight[run] = struct{}{}
	b.mu.Unlock()
	return run, nil
}

// enterQueue 占用排队名额；queue 为 nil（MaxQueue=0）时直接判定一次。
func (b *Bulkhead[I, O]) enterQueue() (bool, error) {
	if b.queue == nil {
		// 不排队模式：空队列检查只看不占——select 里仍会阻塞等待名额，
		// 因此先探测一次 slots，满了立即拒绝。
		select {
		case b.slots <- struct{}{}:
			<-b.slots
			return false, nil
		default:
			b.countRejected()
			return false, ErrOverloaded
		}
	}
	select {
	case b.queue <- struct{}{}:
		b.mu.Lock()
		b.queued++
		b.mu.Unlock()
		return true, nil
	default:
		b.countRejected()
		return false, ErrOverloaded
	}
}

func (b *Bulkhead[I, O]) leaveQueue(held bool) {
	if !held {
		return
	}
	<-b.queue
	b.mu.Lock()
	b.queued--
	b.mu.Unlock()
}

func (b *Bulkhead[I, O]) acquireTenant(ctx context.Context) (*tenantGate, error) {
	key := b.tenantKey(ctx)
	b.mu.Lock()
	gate := b.tenants[key]
	if gate == nil {
		gate = &tenantGate{slots: make(chan struct{}, b.cfg.PerTenant)}
		b.tenants[key] = gate
	}
	gate.refs++
	b.mu.Unlock()
	select {
	case gate.slots <- struct{}{}:
		return gate, nil
	default:
		b.mu.Lock()
		gate.refs--
		if gate.refs == 0 && len(gate.slots) == 0 {
			delete(b.tenants, key)
		}
		b.mu.Unlock()
		b.countRejected()
		return nil, ErrOverloaded
	}
}

func (b *Bulkhead[I, O]) releaseTenant(key string, gate *tenantGate) {
	<-gate.slots
	b.mu.Lock()
	gate.refs--
	if gate.refs == 0 && len(gate.slots) == 0 {
		delete(b.tenants, key)
	}
	b.mu.Unlock()
}

func (b *Bulkhead[I, O]) tenantKey(ctx context.Context) string {
	return b.cfg.TenantKey(ctx)
}

func (b *Bulkhead[I, O]) countRejected() {
	b.mu.Lock()
	b.rejectedTotal++
	b.mu.Unlock()
	if b.metrics.rejected != nil {
		b.metrics.rejected.Add(context.Background(), 1)
	}
}

func (b *Bulkhead[I, O]) release(run *inflightRun[I, O]) {
	run.cancel(nil)
	close(run.done)
	b.mu.Lock()
	delete(b.inflight, run)
	b.mu.Unlock()
	if b.slots != nil {
		<-b.slots
	}
}

func (r *inflightRun[I, O]) ctx() context.Context {
	return r.runCtx
}

// Drain 用 control.RequestInterrupt(reason) 逐个取消全部在途运行，并等待
// 它们返回。开启检查点的运行会以 graph.ErrInterrupted 收尾，进度已保存，
// 其他实例可 Resume；未开启检查点的运行只会被取消。Drain 开始后新调用
// 返回 ErrDraining。ctx 到期返回其错误，排空未完成的运行仍在后台收尾。
func (b *Bulkhead[I, O]) Drain(ctx context.Context, reason string) error {
	b.mu.Lock()
	b.draining = true
	runs := make([]*inflightRun[I, O], 0, len(b.inflight))
	for run := range b.inflight {
		runs = append(runs, run)
	}
	b.mu.Unlock()
	for _, run := range runs {
		run.cancel(control.RequestInterrupt(reason, nil))
	}
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		for _, run := range runs {
			<-run.done
		}
	}()
	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Rejected 返回累计被拒绝的调用数（排队满、租户限满、排队超时）。
func (b *Bulkhead[I, O]) Rejected() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rejectedTotal
}
