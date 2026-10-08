package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/goharness/goharness/pkg/control"
)

type pendingLifecycle struct {
	payload payload
	started time.Time
}
type pendingEvents struct {
	activities map[string]payload
	lifecycles map[string]pendingLifecycle
}

func newPendingEvents() *pendingEvents {
	return &pendingEvents{activities: map[string]payload{}, lifecycles: map[string]pendingLifecycle{}}
}
func lifecycleKey(p payload) string {
	ns, _ := json.Marshal(p.meta.Namespace)
	step := p.lifecycle.Step
	if p.lifecycle.Scope == ScopeGraph {
		step = 0
	}
	return fmt.Sprintf("%s/%s/%s/%s/%d", ns, p.meta.NodeID, p.lifecycle.Scope, p.lifecycle.Name, step)
}
func (p *pendingEvents) observe(v payload) {
	if v.activity == nil && v.lifecycle == nil {
		return
	}
	// Never retain data whose ownership is transferred to the consumer.
	v.meta.Namespace = append([]string(nil), v.meta.Namespace...)
	if v.activity != nil {
		copy := *v.activity
		v.activity = &copy
	}
	if v.lifecycle != nil {
		copy := *v.lifecycle
		v.lifecycle = &copy
	}
	if a := v.activity; a != nil {
		switch a.Status {
		case ActivityStarted, ActivityUpdated:
			p.activities[a.ID] = v
		default:
			delete(p.activities, a.ID)
		}
	}
	if l := v.lifecycle; l != nil {
		key := lifecycleKey(v)
		if l.Phase == PhaseStart {
			p.lifecycles[key] = pendingLifecycle{v, time.Now()}
		} else {
			delete(p.lifecycles, key)
		}
	}
}

// Close outstanding UI state before the run's result/error. This executes in
// the delivery goroutine, so parent cancellation cannot drop terminal notices.
func (p *pendingEvents) finish(err error, send func(payload) error) error {
	activityStatus, phase := ActivityCanceled, PhaseCanceled
	reason := "run completed before operation ended"
	if err != nil {
		reason = err.Error()
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			activityStatus = ActivityFailed
			phase = PhaseError
		}
	}
	suspension, suspended := control.AsSuspension(err)
	if suspended {
		activityStatus = ActivityCanceled
		phase = PhaseInterrupted
	}
	keys := make([]string, 0, len(p.activities))
	for k := range p.activities {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := p.activities[key]
		a := *v.activity
		a.Status = activityStatus
		a.Error = reason
		v.activity = &a
		if err := send(v); err != nil {
			return err
		}
	}
	keys = keys[:0]
	for k := range p.lifecycles {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := p.lifecycles[keys[i]].payload, p.lifecycles[keys[j]].payload
		if left.lifecycle.Scope != right.lifecycle.Scope {
			return left.lifecycle.Scope == ScopeNode
		}
		if len(left.meta.Namespace) != len(right.meta.Namespace) {
			return len(left.meta.Namespace) > len(right.meta.Namespace)
		}
		return keys[i] < keys[j]
	})
	for _, key := range keys {
		pending := p.lifecycles[key]
		v := pending.payload
		l := *v.lifecycle
		l.Phase = phase
		l.Duration = time.Since(pending.started)
		l.Error = reason
		if suspended {
			l.Error = ""
			if l.Scope == ScopeGraph {
				l.Interrupts = suspension.PendingInterrupts()
			}
		}
		v.lifecycle = &l
		if err := send(v); err != nil {
			return err
		}
	}
	return nil
}
