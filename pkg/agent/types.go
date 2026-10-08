package agent

import (
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// Agent is the complete public Agent contract: one final result, one typed
// message data stream, and stable diagnostic naming.
type Agent interface {
	runnable.NamedRunnable[[]schema.Message, []schema.Message]
}
