package llm

import (
	"context"
	"fmt"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

// openAIAppendBlocksExtraKey 是适配器写入 Message.Extra 的私有标记：
// 声明本事件中有多少个尾部 Block 是新增（而非对既有槽位的增量）。
const openAIAppendBlocksExtraKey = "__goharness_openai_append_blocks"

// wireAccumulator translates provider slots to stable public block indexes.
type wireAccumulator struct {
	ctx         context.Context
	accumulator *schema.MessageAccumulator
	ids         []string
	slots       []map[int]int
	kinds       []map[int]schema.ContentBlockType
	counts      []int
	refusals    []string
}

func newWireAccumulator(ctx context.Context) *wireAccumulator {
	return &wireAccumulator{ctx: ctx, accumulator: schema.NewMessageAccumulator()}
}
func (a *wireAccumulator) emit(d schema.MessageDelta) error {
	if err := a.accumulator.Apply(d); err != nil {
		return err
	}
	return stream.PublishMessage(a.ctx, stream.MessageUpdate{MessageID: d.MessageID, Delta: &d})
}
func (a *wireAccumulator) apply(messages []schema.Message) error {
	for i, m := range messages {
		if i >= len(a.ids) {
			id := stream.NewID()
			a.ids = append(a.ids, id)
			a.slots = append(a.slots, map[int]int{})
			a.kinds = append(a.kinds, map[int]schema.ContentBlockType{})
			a.counts = append(a.counts, 0)
			a.refusals = append(a.refusals, "")
			role := m.Role
			if role == "" {
				role = schema.RoleAssistant
			}
			if err := a.emit(schema.MessageDelta{MessageID: id, Op: schema.MessageStart, Role: role}); err != nil {
				return err
			}
		}
		// 拒绝原因按消息身份累计；它不进入 ContentBlocks，最终消息统一带上。
		if refusal, _ := m.Extra["llm.refusal"].(string); refusal != "" {
			a.refusals[i] += refusal
		}
		appendCount, _ := m.Extra[openAIAppendBlocksExtraKey].(int)
		if appendCount < 0 || appendCount > len(m.ContentBlocks) {
			return fmt.Errorf("invalid appended block count")
		}
		for slot, b := range m.ContentBlocks {
			if b.Type == "" {
				continue
			}
			index, exists := a.slots[i][slot]
			appendOnly := slot < appendCount
			if appendOnly {
				exists = false
			}
			if exists && a.kinds[i][slot] != b.Type {
				return fmt.Errorf("provider block %d changed type", slot)
			}
			if !exists {
				index = a.counts[i]
				a.counts[i]++
				if !appendOnly {
					a.slots[i][slot] = index
					a.kinds[i][slot] = b.Type
				}
				initial := b
				if !appendOnly {
					switch b.Type {
					case schema.ContentBlockTypeText:
						if b.Text == nil {
							return fmt.Errorf("nil text block")
						}
						v := *b.Text
						v.Text = ""
						initial.Text = &v
					case schema.ContentBlockTypeReasoning:
						if b.Reasoning == nil {
							return fmt.Errorf("nil reasoning block")
						}
						v := *b.Reasoning
						v.Text = ""
						v.Signature = ""
						initial.Reasoning = &v
					case schema.ContentBlockTypeFunctionToolCall:
						if b.FunctionToolCall == nil {
							return fmt.Errorf("nil tool block")
						}
						v := *b.FunctionToolCall
						v.ID = ""
						v.Name = ""
						v.Arguments = ""
						initial.FunctionToolCall = &v
					}
				}
				if err := a.emit(schema.MessageDelta{MessageID: a.ids[i], Op: schema.BlockStart, BlockIndex: index, Block: &initial}); err != nil {
					return err
				}
			}
			if appendOnly {
				continue
			}
			d := schema.MessageDelta{MessageID: a.ids[i], Op: schema.BlockAppend, BlockIndex: index}
			switch b.Type {
			case schema.ContentBlockTypeText:
				d.Text = b.Text.Text
			case schema.ContentBlockTypeReasoning:
				d.Text = b.Reasoning.Text
				d.Signature = b.Reasoning.Signature
			case schema.ContentBlockTypeFunctionToolCall:
				d.ToolID = b.FunctionToolCall.ID
				d.ToolName = b.FunctionToolCall.Name
				d.Arguments = b.FunctionToolCall.Arguments
			default:
				if exists {
					return fmt.Errorf("provider repeated non-streamable block")
				}
				continue
			}
			if err := a.emit(d); err != nil {
				return err
			}
		}
	}
	return nil
}
func (a *wireAccumulator) messages() []schema.Message {
	out := make([]schema.Message, 0, len(a.ids))
	for i, id := range a.ids {
		m, _ := a.accumulator.Message(id)
		if i < len(a.refusals) && a.refusals[i] != "" {
			extra := make(map[string]any, len(m.Extra)+1)
			for key, value := range m.Extra {
				extra[key] = value
			}
			extra["llm.refusal"] = a.refusals[i]
			m.Extra = extra
		}
		out = append(out, m)
	}
	return out
}
func (a *wireAccumulator) finish(meta *schema.ResponseMeta) ([]schema.Message, error) {
	for _, id := range a.ids {
		if err := a.emit(schema.MessageDelta{MessageID: id, Op: schema.MessageMetadata, Meta: meta}); err != nil {
			return nil, err
		}
		if err := a.emit(schema.MessageDelta{MessageID: id, Op: schema.MessageEnd}); err != nil {
			return nil, err
		}
	}
	return a.messages(), nil
}
