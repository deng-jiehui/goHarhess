package stream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

type Config struct {
	Buffer    int
	Lifecycle bool
}

// Reader intentionally exposes no business-value Collect operation.
type Reader[O any] struct {
	reader *runnable.StreamReader[Event[O]]
	cancel context.CancelFunc
}

func (r *Reader[O]) Recv() (Event[O], error) { return r.reader.Recv() }
func (r *Reader[O]) Close() error            { r.cancel(); return r.reader.Close() }

type payload struct {
	meta      Meta
	message   *MessageUpdate
	activity  *ActivityUpdate
	lifecycle *Lifecycle
}
type scope struct {
	publish   func(context.Context, payload) error
	meta      Meta
	lifecycle bool
}
type scopeKey struct{}
type trackingKey struct{}

func HasMessageTracking(ctx context.Context) bool {
	flags, _ := ctx.Value(trackingKey{}).([]*atomic.Bool)
	return len(flags) > 0
}

func MessagesPublished(ctx context.Context) bool {
	flags, _ := ctx.Value(trackingKey{}).([]*atomic.Bool)
	return len(flags) > 0 && flags[len(flags)-1].Load()
}

// TrackMessages observes publication attempts without materializing snapshots.
// Used by model adapters to avoid duplicate fallbacks and unsafe retries.
func TrackMessages(ctx context.Context) (context.Context, *atomic.Bool) {
	flag := &atomic.Bool{}
	existing, _ := ctx.Value(trackingKey{}).([]*atomic.Bool)
	flags := append(append([]*atomic.Bool(nil), existing...), flag)
	return context.WithValue(ctx, trackingKey{}, flags), flag
}

// NewID provides an opaque identity safe across runs and persisted conversations.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// EnsureMessageIDs assigns missing IDs in a caller-owned slice. Existing IDs survive copies.
func EnsureMessageIDs(messages []schema.Message) {
	for i := range messages {
		if messages[i].ID == "" {
			messages[i].ID = NewID()
		}
	}
}
func Enabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	s, _ := ctx.Value(scopeKey{}).(*scope)
	return s != nil
}
func WithNode(ctx context.Context, id string) context.Context {
	s, _ := ctx.Value(scopeKey{}).(*scope)
	if s == nil {
		return ctx
	}
	next := *s
	next.meta.NodeID = id
	return context.WithValue(ctx, scopeKey{}, &next)
}
func WithNamespace(ctx context.Context, name string) context.Context {
	s, _ := ctx.Value(scopeKey{}).(*scope)
	if s == nil {
		return ctx
	}
	next := *s
	if name != "" {
		next.meta.Namespace = append(append([]string(nil), s.meta.Namespace...), name)
	}
	// 匿名子作用域不产生命名层级，但仍脱离外层节点归属。
	next.meta.NodeID = ""
	return context.WithValue(ctx, scopeKey{}, &next)
}
func publish(ctx context.Context, p payload) error {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(scopeKey{}).(*scope)
	if s == nil {
		return nil
	}
	if p.lifecycle != nil && !s.lifecycle {
		return nil
	}
	p.meta = s.meta
	p.meta.Namespace = append([]string(nil), s.meta.Namespace...)
	return s.publish(ctx, p)
}
func PublishMessage(ctx context.Context, m MessageUpdate) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if !Enabled(ctx) {
		return nil
	}
	if m.Delta != nil {
		d := *m.Delta
		if err := d.Validate(); err != nil {
			return err
		}
		if d.MessageID != m.MessageID {
			return errors.New("delta message ID mismatch")
		}
		// Clone only the fragment, never the accumulated message.
		wrapper := schema.Message{Role: schema.RoleAssistant, ResponseMeta: d.Meta}
		if d.Block != nil {
			wrapper.ContentBlocks = []schema.ContentBlock{*d.Block}
		}
		copied, err := schema.CloneMessagesStrict([]schema.Message{wrapper})
		if err != nil {
			return err
		}
		d.Meta = copied[0].ResponseMeta
		if d.Block != nil {
			d.Block = &copied[0].ContentBlocks[0]
		}
		m.Delta = &d
	} else {
		if err := m.Snapshot.Validate(); err != nil {
			return err
		}
		v, err := schema.CloneMessagesStrict([]schema.Message{*m.Snapshot})
		if err != nil {
			return err
		}
		m.Snapshot = &v[0]
	}
	if err := publish(ctx, payload{message: &m}); err != nil {
		return err
	}
	flags, _ := ctx.Value(trackingKey{}).([]*atomic.Bool)
	for _, flag := range flags {
		flag.Store(true)
	}
	return nil
}

// PublishSnapshots is the fallback for models with only complete responses.
func PublishSnapshots(ctx context.Context, messages []schema.Message) error {
	EnsureMessageIDs(messages)
	if !Enabled(ctx) {
		return nil
	}
	for _, m := range messages {
		if err := PublishMessage(ctx, MessageUpdate{MessageID: m.ID, Snapshot: &m}); err != nil {
			return err
		}
	}
	return nil
}
func PublishActivity(ctx context.Context, a ActivityUpdate) error {
	if err := a.Validate(); err != nil {
		return err
	}
	return publish(ctx, payload{activity: &a})
}
func PublishLifecycle(ctx context.Context, l Lifecycle) error {
	if err := l.Validate(); err != nil {
		return err
	}
	l.Interrupts = append([]control.InterruptRecord(nil), l.Interrupts...)
	for i := range l.Interrupts {
		l.Interrupts[i] = l.Interrupts[i].Clone()
	}
	return publish(ctx, payload{lifecycle: &l})
}

type Activity struct {
	ctx       context.Context
	id, label string
	mu        sync.Mutex
	ended     bool
}

func StartActivity(ctx context.Context, label string) *Activity {
	a := &Activity{ctx: ctx, id: NewID(), label: label}
	_ = PublishActivity(ctx, ActivityUpdate{ID: a.id, Status: "started", Label: label})
	return a
}
func (a *Activity) Update(label string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ended {
		a.label = label
		_ = PublishActivity(a.ctx, ActivityUpdate{ID: a.id, Status: "updated", Label: label})
	}
}
func (a *Activity) End(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ended {
		return
	}
	a.ended = true
	v := ActivityUpdate{ID: a.id, Status: "completed", Label: a.label}
	if err != nil {
		v.Status = "failed"
		v.Error = err.Error()
		if errors.Is(err, context.Canceled) {
			v.Status = "canceled"
		}
	}
	_ = PublishActivity(a.ctx, v)
}

// Run owns a bounded, lossless event queue. The callback must stop its producers
// before returning. Result ownership is transferred to the consumer.
func Run[O any](ctx context.Context, fn func(context.Context) (O, error), configs ...Config) (*Reader[O], error) {
	if ctx == nil || fn == nil {
		return nil, errors.New("run requires context and function")
	}
	cfg := Config{Buffer: 64}
	if len(configs) > 1 {
		return nil, errors.New("at most one stream config")
	}
	if len(configs) == 1 {
		cfg = configs[0]
	}
	if cfg.Buffer < 0 {
		return nil, errors.New("negative event buffer")
	}
	execution, cancel := context.WithCancel(ctx)
	reader, writer := runnable.Pipe[Event[O]](context.WithoutCancel(ctx))
	queue := make(chan payload, cfg.Buffer)
	completed := make(chan struct{})
	var output O
	var terminal error
	var mu sync.Mutex
	closed := false
	messageStates := map[string]*messageProtocol{}
	s := &scope{meta: Meta{RunID: NewID()}, lifecycle: cfg.Lifecycle}
	s.publish = func(call context.Context, p payload) error {
		mu.Lock()
		defer mu.Unlock()
		if closed {
			return errors.New("run event publisher closed")
		}
		select {
		case <-execution.Done():
			return execution.Err()
		default:
		}
		if err := call.Err(); err != nil {
			return err
		}
		var prepared *messageProtocol
		if p.message != nil {
			local := map[string]*messageProtocol{}
			if old := messageStates[p.message.MessageID]; old != nil {
				copy := *old
				local[p.message.MessageID] = &copy
			}
			if err := checkMessageProtocol(local, *p.message); err != nil {
				return err
			}
			prepared = local[p.message.MessageID]
		}
		select {
		case queue <- p:
			if p.message != nil {
				messageStates[p.message.MessageID] = prepared
			}
			return nil
		case <-execution.Done():
			return execution.Err()
		case <-call.Done():
			return call.Err()
		}
	}
	execution = context.WithValue(execution, scopeKey{}, s)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				terminal = fmt.Errorf("stream run panicked: %v", v)
			}
			mu.Lock()
			closed = true
			mu.Unlock()
			close(completed)
		}()
		if execution.Err() != nil {
			terminal = execution.Err()
			return
		}
		output, terminal = fn(execution)
		if terminal == nil {
			terminal = execution.Err()
		}
	}()
	go func() {
		defer cancel()
		defer writer.Close()
		var sequence uint64
		pending := newPendingEvents()
		send := func(p payload) error {
			sequence++
			p.meta.Sequence = sequence
			e := Event[O]{Meta: p.meta, Message: p.message, Activity: p.activity, Lifecycle: p.lifecycle}
			switch {
			case p.message != nil:
				e.Kind = KindMessage
			case p.activity != nil:
				e.Kind = KindActivity
			default:
				e.Kind = KindLifecycle
			}
			if err := e.Validate(); err != nil {
				return err
			}
			pending.observe(p)
			if err := writer.Send(e); err != nil {
				return err
			}
			return nil
		}
		for {
			select {
			case <-reader.Context().Done():
				return
			case p := <-queue:
				if send(p) != nil {
					return
				}
			case <-completed:
				for {
					select {
					case p := <-queue:
						if send(p) != nil {
							return
						}
					default:
						if err := pending.finish(terminal, send); err != nil {
							_ = writer.CloseWithError(err)
							return
						}
						if terminal != nil {
							// 等待输入是业务控制信息，默认关闭调试生命周期时也必须可见。
							if suspended, ok := control.AsSuspension(terminal); ok && !cfg.Lifecycle {
								if err := send(payload{meta: s.meta, lifecycle: &Lifecycle{Scope: ScopeGraph, Phase: PhaseInterrupted, Name: "run", Interrupts: suspended.PendingInterrupts()}}); err != nil {
									return
								}
							}
							_ = writer.CloseWithError(terminal)
							return
						}
						sequence++
						_ = writer.Send(Event[O]{Kind: KindResult, Meta: Meta{RunID: s.meta.RunID, Sequence: sequence}, Result: &output})
						return
					}
				}
			}
		}
	}()
	return &Reader[O]{reader: reader, cancel: cancel}, nil
}
