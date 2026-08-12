package schema

// MessageEventKind identifies the data-plane shape emitted by an Agent.
// Graph lifecycle and checkpoint events are intentionally not represented
// here; they travel through graph.EventSink/callbacks.
type MessageEventKind string

const (
	MessageEventDelta    MessageEventKind = "delta"
	MessageEventSnapshot MessageEventKind = "snapshot"
	MessageEventDone     MessageEventKind = "done"
)

// MessageEvent carries either provider-neutral message deltas or cumulative
// snapshots. Consumers can choose the representation without observing Graph
// node events.
type MessageEvent struct {
	Kind     MessageEventKind
	Messages []Message
}

func (e MessageEvent) Clone() MessageEvent {
	e.Messages = CloneMessages(e.Messages)
	return e
}
