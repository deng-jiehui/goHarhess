package stream

import (
	"fmt"
	"github.com/goharness/goharness/pkg/schema"
)

// messageProtocol validates ordering without retaining full message contents.
type messageProtocol struct {
	blocks []schema.ContentBlockType
	ended  bool
}

func checkMessageProtocol(states map[string]*messageProtocol, m MessageUpdate) error {
	state := states[m.MessageID]
	if m.Snapshot != nil {
		states[m.MessageID] = &messageProtocol{ended: true}
		return nil
	}
	d := m.Delta
	if d.Op == schema.MessageStart {
		if state != nil {
			return fmt.Errorf("message %q already exists", m.MessageID)
		}
		states[m.MessageID] = &messageProtocol{}
		return nil
	}
	if state == nil || state.ended {
		return fmt.Errorf("message %q is absent or completed", m.MessageID)
	}
	switch d.Op {
	case schema.BlockStart:
		if d.BlockIndex != len(state.blocks) {
			return fmt.Errorf("block_start must use next index")
		}
		state.blocks = append(state.blocks, d.Block.Type)
	case schema.BlockAppend:
		if d.BlockIndex < 0 || d.BlockIndex >= len(state.blocks) {
			return fmt.Errorf("unknown block index")
		}
		switch state.blocks[d.BlockIndex] {
		case schema.ContentBlockTypeText:
			if d.Signature != "" || d.ToolID != "" || d.ToolName != "" || d.Arguments != "" {
				return fmt.Errorf("invalid text fragment")
			}
		case schema.ContentBlockTypeReasoning:
			if d.ToolID != "" || d.ToolName != "" || d.Arguments != "" {
				return fmt.Errorf("invalid reasoning fragment")
			}
		case schema.ContentBlockTypeFunctionToolCall:
			if d.Text != "" || d.Signature != "" {
				return fmt.Errorf("invalid tool fragment")
			}
		default:
			return fmt.Errorf("block does not accept fragments")
		}
	case schema.MessageEnd:
		state.ended = true
	}
	return nil
}
