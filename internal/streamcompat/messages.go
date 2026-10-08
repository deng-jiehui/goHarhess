// Package streamcompat contains the deprecated message callback bridge.
// Public run streams never subscribe to or depend on this bridge.
package streamcompat

import (
	"context"
	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/schema"
)

func Snapshot(ctx context.Context, messages []schema.Message) {
	if !callback.HasStreamCallbacks[schema.MessageEvent](ctx) {
		return
	}
	callback.EmitStream(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: schema.CloneMessages(messages)})
}
