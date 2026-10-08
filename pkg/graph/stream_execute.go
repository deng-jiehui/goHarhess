package graph

import (
	"context"

	"github.com/goharness/goharness/pkg/runnable"
)

// streamCarrier 是边上传递的数据：完整值或尚未消费完的输出流，二者只有其一。
type streamCarrier struct {
	value  erasedValue
	stream *erasedStream
}

// pendingStreamNode 记录输出流尚未被下游消费完的节点，其结束事件延后到流物化时发布。
type pendingStreamNode struct {
	ctx  context.Context
	id   string
	step int
}

// runNode 是调度循环中唯一与执行模式相关的步骤：按模式与节点能力选择一种执行形态。
// 它不做中断、落盘或事件判断，这些控制语义统一由 executeRuntime 负责。
// 调用前调度循环已保证：输入为流时节点能直接消费它（见 consumesStream）。
func runNode(ctx context.Context, node runtimeNode, in streamCarrier, input any, options []runnable.Option, mode execMode) (streamCarrier, string, error) {
	switch {
	case mode == execModeStream && in.stream != nil && node.transform != nil:
		stream, err := node.transform(ctx, *in.stream, options)
		if err != nil {
			return streamCarrier{}, "node.transform", err
		}
		return streamCarrier{stream: &stream}, "", nil
	case mode == execModeStream && in.stream != nil && node.collect != nil:
		value, err := node.collect(ctx, *in.stream, options)
		if err != nil {
			return streamCarrier{}, "node.collect", err
		}
		return streamCarrier{value: erasedValue{typ: node.outputType, value: value}}, "", nil
	case mode == execModeStream && node.stream != nil:
		stream, err := node.stream(ctx, input, options)
		if err != nil {
			return streamCarrier{}, "node.stream", err
		}
		return streamCarrier{stream: &stream}, "", nil
	default:
		value, err := node.invoke(ctx, input, options)
		if err != nil {
			return streamCarrier{}, "node.invoke", err
		}
		return streamCarrier{value: erasedValue{typ: node.outputType, value: value}}, "", nil
	}
}

// consumesStream 报告节点能否直接消费上游输出流；不能时调度循环先物化。
// PreHandler 从 State 重建输入、并行节点以 State 为输入，二者都是物化屏障。
func consumesStream(node runtimeNode, mode execMode) bool {
	return mode == execModeStream && node.pre == nil && node.parallel == nil && (node.transform != nil || node.collect != nil)
}

func materializeStream(ctx context.Context, carrier *streamCarrier, pending *[]pendingStreamNode) error {
	if err := collectStream(ctx, carrier); err != nil {
		return err
	}
	finishPendingStreamNodes(pending)
	return nil
}

func collectStream(ctx context.Context, carrier *streamCarrier) error {
	if carrier == nil || carrier.stream == nil {
		return nil
	}
	stream := carrier.stream
	value, err := stream.collect(ctx)
	if err != nil {
		return err
	}
	carrier.value = erasedValue{typ: stream.elemType, value: value}
	carrier.stream = nil
	return nil
}

// discardStream 关闭尚未消费完的输出流，使上游节点尽快结束。
func discardStream(carrier *streamCarrier) {
	if carrier == nil || carrier.stream == nil {
		return
	}
	if carrier.stream.close != nil {
		_ = carrier.stream.close()
	}
	carrier.stream = nil
}

func finishPendingStreamNodes(pending *[]pendingStreamNode) {
	for _, node := range *pending {
		finishStreamNode(node.ctx, node.id, node.step)
	}
	*pending = (*pending)[:0]
}

func finishPendingBeforeCurrent(pending *[]pendingStreamNode, id string, step int) {
	for len(*pending) > 0 {
		node := (*pending)[0]
		if node.id == id && node.step == step {
			return
		}
		finishStreamNode(node.ctx, node.id, node.step)
		*pending = (*pending)[1:]
	}
}

func removePendingCurrent(pending *[]pendingStreamNode, id string, step int) {
	if len(*pending) == 0 {
		return
	}
	node := (*pending)[0]
	if node.id == id && node.step == step {
		*pending = (*pending)[1:]
	}
}

func finishStreamNode(ctx context.Context, id string, step int) {
	publish(ctx, Event{Type: EventNodeOutput, NodeID: id, Step: step})
	publish(ctx, Event{Type: EventNodeEnd, NodeID: id, Step: step})
}

func hasConditionalEdge(from string, edges []runtimeEdge) bool {
	for _, edge := range edges {
		if edge.from == from && edge.condition != nil {
			return true
		}
	}
	return false
}
