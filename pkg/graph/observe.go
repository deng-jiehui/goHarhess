package graph

import (
	"context"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/observe"
)

// emitInterruptEvent 发射一次挂起的瞬时观测事件，附加在挂起发生时的
// span 上，Attrs 携带待答问题 ID 列表与恢复位置。恢复后仍待答的重发
// 也走这里——对观测者它是同一逻辑中断的再次浮现。
func emitInterruptEvent(ctx context.Context, graph string, point InterruptPoint, reason, resumeNode string, step int, pending []control.InterruptRecord) {
	if !observe.Active(ctx) {
		return
	}
	ids := make([]string, 0, len(pending))
	for _, record := range pending {
		ids = append(ids, record.ID)
	}
	observe.Emit(ctx, observe.Event{
		Kind:  observe.KindInterrupt,
		Phase: observe.PhaseEvent,
		Name:  graph,
		Time:  time.Now(),
		Attrs: map[string]any{
			"graph.name":            graph,
			"interrupt.point":       string(point),
			"interrupt.reason":      reason,
			"interrupt.resume_node": resumeNode,
			"interrupt.step":        step,
			"interrupt.pending":     len(pending),
			"interrupt.ids":         ids,
		},
	})
}
