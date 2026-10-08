// Package stream provides the public run event protocol, independently of Graph and Agent.
package stream

import (
	"fmt"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/schema"
)

type Kind string

const (
	KindMessage   Kind = "message"
	KindActivity  Kind = "activity"
	KindLifecycle Kind = "lifecycle"
	KindResult    Kind = "result"
)

type Meta struct {
	RunID     string   `json:"run_id"`
	Namespace []string `json:"namespace,omitempty"`
	NodeID    string   `json:"node_id,omitempty"`
	Sequence  uint64   `json:"sequence"`
}
type Event[O any] struct {
	Kind      Kind            `json:"kind"`
	Meta      Meta            `json:"meta"`
	Message   *MessageUpdate  `json:"message,omitempty"`
	Activity  *ActivityUpdate `json:"activity,omitempty"`
	Lifecycle *Lifecycle      `json:"lifecycle,omitempty"`
	Result    *O              `json:"result,omitempty"`
}

// MessageUpdate contains either an explicit delta or one complete snapshot.
type MessageUpdate struct {
	MessageID string               `json:"message_id"`
	Delta     *schema.MessageDelta `json:"delta,omitempty"`
	Snapshot  *schema.Message      `json:"snapshot,omitempty"`
}
type ActivityUpdate struct {
	ID     string         `json:"id"`
	Status ActivityStatus `json:"status"`
	Label  string         `json:"label,omitempty"`
	Error  string         `json:"error,omitempty"`
}
type Lifecycle struct {
	Interrupts []control.InterruptRecord `json:"interrupts,omitempty"`
	Scope      Scope                     `json:"scope"`
	Phase      Phase                     `json:"phase"`
	Name       string                    `json:"name"`
	Step       int                       `json:"step,omitempty"`
	Duration   time.Duration             `json:"duration,omitempty"`
	Error      string                    `json:"error,omitempty"`
}

type ActivityStatus string

const (
	ActivityStarted   ActivityStatus = "started"
	ActivityUpdated   ActivityStatus = "updated"
	ActivityCompleted ActivityStatus = "completed"
	ActivityFailed    ActivityStatus = "failed"
	ActivityCanceled  ActivityStatus = "canceled"
)

type Scope string

const (
	ScopeGraph Scope = "graph"
	ScopeNode  Scope = "node"
)

type Phase string

const (
	PhaseStart       Phase = "start"
	PhaseEnd         Phase = "end"
	PhaseError       Phase = "error"
	PhaseCanceled    Phase = "canceled"
	PhaseInterrupted Phase = "interrupted"
)

func (m MessageUpdate) Validate() error {
	if m.MessageID == "" || (m.Delta == nil) == (m.Snapshot == nil) {
		return fmt.Errorf("message requires ID and exactly one payload")
	}
	if m.Delta != nil {
		if m.Delta.MessageID != m.MessageID {
			return fmt.Errorf("delta ID mismatch")
		}
		return m.Delta.Validate()
	}
	if m.Snapshot.ID != m.MessageID {
		return fmt.Errorf("snapshot ID mismatch")
	}
	return m.Snapshot.Validate()
}
func (a ActivityUpdate) Validate() error {
	if a.ID == "" {
		return fmt.Errorf("activity ID required")
	}
	switch a.Status {
	case ActivityStarted, ActivityUpdated, ActivityCompleted:
		if a.Error != "" {
			return fmt.Errorf("non-failure activity contains error")
		}
	case ActivityFailed, ActivityCanceled:
	default:
		return fmt.Errorf("invalid activity status %q", a.Status)
	}
	return nil
}
func (l Lifecycle) Validate() error {
	if len(l.Interrupts) > 0 && l.Phase != PhaseInterrupted {
		return fmt.Errorf("interrupt records require interrupted phase")
	}
	for _, record := range l.Interrupts {
		if err := record.Validate(); err != nil {
			return err
		}
		if record.Answered {
			return fmt.Errorf("lifecycle only carries pending interrupts")
		}
	}
	if l.Scope != ScopeGraph && l.Scope != ScopeNode {
		return fmt.Errorf("invalid lifecycle scope")
	}
	if l.Name == "" || l.Step < 0 || l.Duration < 0 {
		return fmt.Errorf("invalid lifecycle metadata")
	}
	switch l.Phase {
	case PhaseStart, PhaseEnd, PhaseInterrupted:
		if l.Error != "" {
			return fmt.Errorf("successful lifecycle contains error")
		}
	case PhaseError, PhaseCanceled:
	default:
		return fmt.Errorf("invalid lifecycle phase")
	}
	return nil
}

// Validate checks the tagged union, including payload/Kind agreement.
func (e Event[O]) Validate() error {
	n := 0
	for _, present := range []bool{e.Message != nil, e.Activity != nil, e.Lifecycle != nil, e.Result != nil} {
		if present {
			n++
		}
	}
	if n != 1 {
		return fmt.Errorf("event requires exactly one payload")
	}
	switch e.Kind {
	case KindMessage:
		if e.Message != nil {
			return e.Message.Validate()
		}
	case KindActivity:
		if e.Activity != nil {
			return e.Activity.Validate()
		}
	case KindLifecycle:
		if e.Lifecycle != nil {
			return e.Lifecycle.Validate()
		}
	case KindResult:
		if e.Result != nil {
			return nil
		}
	}
	return fmt.Errorf("event kind does not match payload")
}
