package llm

import (
	"bytes"
	"encoding/json"
	"strings"

	"goharness/pkg/schema"
)

type DeltaAccumulator struct {
	mergeDeltas bool
	messages    []schema.Message
}

func NewDeltaAccumulator(mergeDeltas bool) *DeltaAccumulator {
	return &DeltaAccumulator{mergeDeltas: mergeDeltas}
}

func (a *DeltaAccumulator) Apply(event Event) ([]schema.Message, error) {
	if !a.mergeDeltas {
		a.messages = schema.CloneMessages(event.Messages)
		return schema.CloneMessages(a.messages), nil
	}
	for index, delta := range event.Messages {
		if index >= len(a.messages) {
			a.messages = append(a.messages, schema.CloneMessages([]schema.Message{delta})[0])
			continue
		}
		a.messages[index] = mergeMessageDelta(a.messages[index], delta)
	}
	return schema.CloneMessages(a.messages), nil
}

func (a *DeltaAccumulator) Messages() []schema.Message {
	return schema.CloneMessages(a.messages)
}

func mergeMessageDelta(current, delta schema.Message) schema.Message {
	merged := schema.CloneMessages([]schema.Message{current})[0]
	if delta.Role != "" {
		merged.Role = delta.Role
	}
	merged.Content = mergeContentDelta(merged.Content, delta.Content)
	merged.ReasoningContent = mergeContentDelta(merged.ReasoningContent, delta.ReasoningContent)
	merged.Name = appendStringDelta(merged.Name, delta.Name)
	if delta.FunctionCall != nil {
		if merged.FunctionCall == nil {
			call := *delta.FunctionCall
			merged.FunctionCall = &call
		} else {
			merged.FunctionCall.Name = appendStringDelta(merged.FunctionCall.Name, delta.FunctionCall.Name)
			merged.FunctionCall.Arguments = appendStringDelta(merged.FunctionCall.Arguments, delta.FunctionCall.Arguments)
		}
	}
	if len(delta.Extra) > 0 {
		if merged.Extra == nil {
			merged.Extra = map[string]any{}
		}
		for key, value := range delta.Extra {
			merged.Extra[key] = value
		}
	}
	return merged
}

func mergeContentDelta(current, delta schema.Content) schema.Content {
	if delta.IsZero() {
		return current
	}
	if current.IsZero() {
		return delta
	}
	if !current.IsMultimodal() && !delta.IsMultimodal() {
		return schema.TextContent(appendStringDelta(current.String(), delta.String()))
	}
	if contentItemsEqual(current.Items(), delta.Items()) {
		return current
	}
	return current.Append(delta)
}

func contentItemsEqual(left, right []schema.ContentItem) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
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
