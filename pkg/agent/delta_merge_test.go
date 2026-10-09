package agent_test

import (
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

// testDeltaMerger 模拟增量流式 Provider 的消息合并：mock 模型按事件序吐出
// 消息片段，按消息下标与 Block 下标拼接出累积快照，供 Stream 消费者断言。
type testDeltaMerger struct {
	merge    bool
	messages []schema.Message
}

func newTestDeltaMerger(mergeDeltas bool) *testDeltaMerger {
	return &testDeltaMerger{merge: mergeDeltas}
}

func (m *testDeltaMerger) apply(response llm.Response) ([]schema.Message, error) {
	if !m.merge {
		m.messages = schema.CloneMessages(response.Messages)
		return schema.CloneMessages(m.messages), nil
	}
	for index, delta := range response.Messages {
		if index >= len(m.messages) {
			m.messages = append(m.messages, schema.CloneMessages([]schema.Message{delta})[0])
			continue
		}
		merged, err := mergeTestMessageDelta(m.messages[index], delta)
		if err != nil {
			return nil, err
		}
		m.messages[index] = merged
	}
	return schema.CloneMessages(m.messages), nil
}

func mergeTestMessageDelta(current, delta schema.Message) (schema.Message, error) {
	merged := schema.CloneMessages([]schema.Message{current})[0]
	delta = schema.CloneMessages([]schema.Message{delta})[0]
	if delta.Role != "" {
		merged.Role = delta.Role
	}
	for index, block := range delta.ContentBlocks {
		if block.Type == "" {
			continue
		}
		if index >= len(merged.ContentBlocks) {
			for len(merged.ContentBlocks) < index {
				merged.ContentBlocks = append(merged.ContentBlocks, schema.ContentBlock{})
			}
			merged.ContentBlocks = append(merged.ContentBlocks, block)
			continue
		}
		if merged.ContentBlocks[index].Type == "" {
			merged.ContentBlocks[index] = block
			continue
		}
		if merged.ContentBlocks[index].Type != block.Type {
			return schema.Message{}, fmt.Errorf("content block %d changed type from %q to %q", index, merged.ContentBlocks[index].Type, block.Type)
		}
		mergeTestBlockDelta(&merged.ContentBlocks[index], block)
	}
	if len(delta.Extra) > 0 {
		if merged.Extra == nil {
			merged.Extra = map[string]any{}
		}
		for key, value := range delta.Extra {
			merged.Extra[key] = value
		}
	}
	return merged, nil
}

func mergeTestBlockDelta(current *schema.ContentBlock, delta schema.ContentBlock) {
	switch current.Type {
	case schema.ContentBlockTypeText:
		if current.Text != nil && delta.Text != nil {
			current.Text.Text = appendTestStringDelta(current.Text.Text, delta.Text.Text)
		}
	case schema.ContentBlockTypeReasoning:
		if current.Reasoning != nil && delta.Reasoning != nil {
			current.Reasoning.Text = appendTestStringDelta(current.Reasoning.Text, delta.Reasoning.Text)
			current.Reasoning.Signature = appendTestStringDelta(current.Reasoning.Signature, delta.Reasoning.Signature)
		}
	case schema.ContentBlockTypeFunctionToolCall:
		if current.FunctionToolCall != nil && delta.FunctionToolCall != nil {
			current.FunctionToolCall.ID = appendTestStringDelta(current.FunctionToolCall.ID, delta.FunctionToolCall.ID)
			current.FunctionToolCall.Name = appendTestStringDelta(current.FunctionToolCall.Name, delta.FunctionToolCall.Name)
			current.FunctionToolCall.Arguments = appendTestStringDelta(current.FunctionToolCall.Arguments, delta.FunctionToolCall.Arguments)
		}
	}
}

func appendTestStringDelta(current, delta string) string {
	if delta == "" {
		return current
	}
	if current == "" {
		return delta
	}
	if current == delta || strings.HasPrefix(delta, current) {
		return delta
	}
	if strings.HasPrefix(current, delta) {
		return current
	}
	return current + delta
}
