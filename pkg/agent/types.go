package agent

import (
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// Agent is the complete public Agent contract: one final result, one typed
// message data stream, and stable diagnostic naming.
type Agent interface {
	runnable.NamedRunnable[[]schema.Message, []schema.Message]
}

func appendMessages(left, right []schema.Message) []schema.Message {
	combined := make([]schema.Message, 0, len(left)+len(right))
	combined = append(combined, schema.CloneMessages(left)...)
	combined = append(combined, schema.CloneMessages(right)...)
	return combined
}
