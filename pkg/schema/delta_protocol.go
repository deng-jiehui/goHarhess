package schema

import (
	"fmt"
	"strings"
)

// DeltaOp describes an explicit operation; repeated fragments are never deduplicated.
type DeltaOp string

const (
	MessageStart    DeltaOp = "message_start"
	BlockStart      DeltaOp = "block_start"
	BlockAppend     DeltaOp = "block_append"
	MessageMetadata DeltaOp = "message_metadata"
	MessageEnd      DeltaOp = "message_end"
)

// MessageDelta constructs a Message. Block indexes are stable, dense and zero-based.
// Tool arguments may be incomplete JSON until MessageEnd.
type MessageDelta struct {
	MessageID  string        `json:"message_id"`
	Op         DeltaOp       `json:"op"`
	Role       Role          `json:"role,omitempty"`
	BlockIndex int           `json:"block_index,omitempty"`
	Block      *ContentBlock `json:"block,omitempty"`
	Text       string        `json:"text,omitempty"`
	Signature  string        `json:"signature,omitempty"`
	Arguments  string        `json:"arguments,omitempty"`
	ToolID     string        `json:"tool_id,omitempty"`
	ToolName   string        `json:"tool_name,omitempty"`
	Meta       *ResponseMeta `json:"meta,omitempty"`
}

type accumulatedMessage struct {
	message Message
	ended   bool
	buffers []*blockBuffers
}
type blockBuffers struct{ text, signature, arguments, id, name strings.Builder }

// MessageAccumulator is owned by one consumer. Apply does not clone the accumulated text.
type MessageAccumulator struct {
	messages map[string]*accumulatedMessage
}

func NewMessageAccumulator() *MessageAccumulator {
	return &MessageAccumulator{messages: make(map[string]*accumulatedMessage)}
}
func (a *MessageAccumulator) Apply(d MessageDelta) error {
	if err := d.Validate(); err != nil {
		return err
	}
	if d.MessageID == "" {
		return fmt.Errorf("message ID is required")
	}
	if a.messages == nil {
		a.messages = make(map[string]*accumulatedMessage)
	}
	m := a.messages[d.MessageID]
	if d.Op == MessageStart {
		if m != nil {
			return fmt.Errorf("message %q already started", d.MessageID)
		}
		v := Message{ID: d.MessageID, Role: d.Role}
		if err := v.Validate(); err != nil {
			return err
		}
		a.messages[d.MessageID] = &accumulatedMessage{message: v}
		return nil
	}
	if m == nil || m.ended {
		return fmt.Errorf("message %q is absent or completed", d.MessageID)
	}
	switch d.Op {
	case BlockStart:
		if d.Block == nil || d.BlockIndex != len(m.message.ContentBlocks) {
			return fmt.Errorf("block start requires next index and payload")
		}
		b, err := cloneContentBlock(*d.Block, true, "delta.block", nil)
		if err != nil {
			return err
		}
		// An incomplete tool call is legal while it is being constructed.
		check := b
		if b.Type == ContentBlockTypeFunctionToolCall && b.FunctionToolCall != nil {
			call := *b.FunctionToolCall
			call.ID = "pending"
			call.Name = "pending"
			call.Arguments = "{}"
			check.FunctionToolCall = &call
		}
		if err := check.Validate(); err != nil {
			return err
		}
		m.message.ContentBlocks = append(m.message.ContentBlocks, b)
		buffer := &blockBuffers{}
		if b.Text != nil {
			buffer.text.WriteString(b.Text.Text)
		}
		if b.Reasoning != nil {
			buffer.text.WriteString(b.Reasoning.Text)
			buffer.signature.WriteString(b.Reasoning.Signature)
		}
		if b.FunctionToolCall != nil {
			buffer.id.WriteString(b.FunctionToolCall.ID)
			buffer.name.WriteString(b.FunctionToolCall.Name)
			buffer.arguments.WriteString(b.FunctionToolCall.Arguments)
		}
		m.buffers = append(m.buffers, buffer)
	case BlockAppend:
		if d.BlockIndex < 0 || d.BlockIndex >= len(m.message.ContentBlocks) {
			return fmt.Errorf("unknown block index %d", d.BlockIndex)
		}
		b := &m.message.ContentBlocks[d.BlockIndex]
		buffer := m.buffers[d.BlockIndex]
		switch b.Type {
		case ContentBlockTypeText:
			if d.Arguments != "" || d.ToolID != "" || d.ToolName != "" || d.Signature != "" {
				return fmt.Errorf("invalid text delta")
			}
			buffer.text.WriteString(d.Text)
			b.Text.Text = buffer.text.String()
		case ContentBlockTypeReasoning:
			if d.Arguments != "" || d.ToolID != "" || d.ToolName != "" {
				return fmt.Errorf("invalid reasoning delta")
			}
			buffer.text.WriteString(d.Text)
			b.Reasoning.Text = buffer.text.String()
			buffer.signature.WriteString(d.Signature)
			b.Reasoning.Signature = buffer.signature.String()
		case ContentBlockTypeFunctionToolCall:
			if d.Text != "" || d.Signature != "" {
				return fmt.Errorf("invalid tool delta")
			}
			buffer.id.WriteString(d.ToolID)
			b.FunctionToolCall.ID = buffer.id.String()
			buffer.name.WriteString(d.ToolName)
			b.FunctionToolCall.Name = buffer.name.String()
			buffer.arguments.WriteString(d.Arguments)
			b.FunctionToolCall.Arguments = buffer.arguments.String()
		default:
			return fmt.Errorf("block %q does not accept appended fragments", b.Type)
		}
	case MessageMetadata:
		meta, err := cloneResponseMeta(d.Meta, true, "delta.meta", nil)
		if err != nil {
			return err
		}
		if err := meta.validate(); err != nil {
			return err
		}
		m.message.ResponseMeta = meta
	case MessageEnd:
		if err := m.message.Validate(); err != nil {
			return err
		}
		m.ended = true
	default:
		return fmt.Errorf("unknown delta operation %q", d.Op)
	}
	return nil
}

// Validate checks the operation envelope, not the accumulated message state.
func (d MessageDelta) Validate() error {
	if d.MessageID == "" {
		return fmt.Errorf("message ID is required")
	}
	appendFields := d.Text != "" || d.Signature != "" || d.Arguments != "" || d.ToolID != "" || d.ToolName != ""
	if d.Op != BlockAppend && appendFields {
		return fmt.Errorf("append fields require block_append")
	}
	if d.Op != BlockStart && d.Block != nil {
		return fmt.Errorf("block payload requires block_start")
	}
	if d.Op != MessageMetadata && d.Meta != nil {
		return fmt.Errorf("metadata requires message_metadata")
	}
	if d.Op != MessageStart && d.Role != "" {
		return fmt.Errorf("role requires message_start")
	}
	if d.Op != BlockStart && d.Op != BlockAppend && d.BlockIndex != 0 {
		return fmt.Errorf("block index requires block operation")
	}
	switch d.Op {
	case MessageStart:
		return (Message{Role: d.Role}).Validate()
	case BlockStart:
		if d.Block == nil || d.BlockIndex < 0 {
			return fmt.Errorf("block_start requires payload and nonnegative index")
		}
		check := *d.Block
		if check.Type == ContentBlockTypeFunctionToolCall && check.FunctionToolCall != nil {
			call := *check.FunctionToolCall
			call.ID = "pending"
			call.Name = "pending"
			call.Arguments = "{}"
			check.FunctionToolCall = &call
		}
		return check.Validate()
	case BlockAppend:
		if d.BlockIndex < 0 {
			return fmt.Errorf("negative block index")
		}
	case MessageMetadata:
		if d.Meta == nil {
			return fmt.Errorf("metadata payload required")
		}
		return d.Meta.validate()
	case MessageEnd:
	default:
		return fmt.Errorf("unknown delta operation %q", d.Op)
	}
	return nil
}

// Message returns an isolated snapshot, including partially constructed messages.
func (a *MessageAccumulator) Message(id string) (Message, bool) {
	m := a.messages[id]
	if m == nil {
		return Message{}, false
	}
	return CloneMessages([]Message{m.message})[0], true
}
