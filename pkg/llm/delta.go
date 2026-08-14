package llm

import (
	"fmt"
	"strings"

	"goharness/pkg/schema"
)

type DeltaAccumulator struct {
	mergeDeltas bool
	messages    []schema.Message
}

const openAIAppendBlocksExtraKey = "__goharness_openai_append_blocks"

func NewDeltaAccumulator(mergeDeltas bool) *DeltaAccumulator {
	return &DeltaAccumulator{mergeDeltas: mergeDeltas}
}

func (a *DeltaAccumulator) Apply(response Response) ([]schema.Message, error) {
	if !a.mergeDeltas {
		a.messages = schema.CloneMessages(response.Messages)
		for index := range a.messages {
			consumeAppendBlockCount(&a.messages[index])
		}
		return schema.CloneMessages(a.messages), nil
	}
	for index, delta := range response.Messages {
		if index >= len(a.messages) {
			cloned := schema.CloneMessages([]schema.Message{delta})[0]
			consumeAppendBlockCount(&cloned)
			a.messages = append(a.messages, cloned)
			continue
		}
		merged, err := mergeMessageDelta(a.messages[index], delta)
		if err != nil {
			return nil, err
		}
		a.messages[index] = merged
	}
	return schema.CloneMessages(a.messages), nil
}

func (a *DeltaAccumulator) Messages() []schema.Message {
	return schema.CloneMessages(a.messages)
}

func mergeMessageDelta(current, delta schema.Message) (schema.Message, error) {
	merged := schema.CloneMessages([]schema.Message{current})[0]
	delta = schema.CloneMessages([]schema.Message{delta})[0]
	appendCount := consumeAppendBlockCount(&delta)
	if delta.Role != "" {
		merged.Role = delta.Role
	}
	if appendCount > len(delta.ContentBlocks) {
		return schema.Message{}, fmt.Errorf("invalid append block count %d for %d content blocks", appendCount, len(delta.ContentBlocks))
	}
	if appendCount > 0 {
		merged.ContentBlocks = append(merged.ContentBlocks, delta.ContentBlocks[:appendCount]...)
		delta.ContentBlocks = delta.ContentBlocks[appendCount:]
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
		mergeContentBlockDelta(&merged.ContentBlocks[index], block)
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

func consumeAppendBlockCount(message *schema.Message) int {
	if message.Extra == nil {
		return 0
	}
	value, ok := message.Extra[openAIAppendBlocksExtraKey]
	if !ok {
		return 0
	}
	delete(message.Extra, openAIAppendBlocksExtraKey)
	if len(message.Extra) == 0 {
		message.Extra = nil
	}
	count, _ := value.(int)
	return count
}

func mergeContentBlockDelta(current *schema.ContentBlock, delta schema.ContentBlock) {
	switch current.Type {
	case schema.ContentBlockTypeText:
		if current.Text != nil && delta.Text != nil {
			current.Text.Text = appendStringDelta(current.Text.Text, delta.Text.Text)
		}
	case schema.ContentBlockTypeReasoning:
		if current.Reasoning != nil && delta.Reasoning != nil {
			current.Reasoning.Text = appendStringDelta(current.Reasoning.Text, delta.Reasoning.Text)
			current.Reasoning.Signature = appendStringDelta(current.Reasoning.Signature, delta.Reasoning.Signature)
		}
	case schema.ContentBlockTypeFunctionToolCall:
		if current.FunctionToolCall != nil && delta.FunctionToolCall != nil {
			current.FunctionToolCall.ID = appendStringDelta(current.FunctionToolCall.ID, delta.FunctionToolCall.ID)
			current.FunctionToolCall.Name = appendStringDelta(current.FunctionToolCall.Name, delta.FunctionToolCall.Name)
			current.FunctionToolCall.Arguments = appendStringDelta(current.FunctionToolCall.Arguments, delta.FunctionToolCall.Arguments)
		}
	}
}

func appendStringDelta(current, delta string) string {
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
