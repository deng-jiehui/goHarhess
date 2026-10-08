package stream

import (
	"context"
	"errors"
	"sync"

	"github.com/goharness/goharness/pkg/schema"
)

// MessageWriter owns message identity, block indexes and incremental assembly.
// It is safe for concurrent callers. A publication failure is terminal.
type MessageWriter struct {
	mu          sync.Mutex
	ctx         context.Context
	id          string
	accumulator *schema.MessageAccumulator
	blocks      int
	textBlock   int
	ended       bool
	err         error
}

func StartMessage(ctx context.Context, role schema.Role) (*MessageWriter, error) {
	if ctx == nil {
		return nil, errors.New("message context is nil")
	}
	w := &MessageWriter{ctx: ctx, id: NewID(), accumulator: schema.NewMessageAccumulator(), textBlock: -1}
	if err := w.apply(schema.MessageDelta{Op: schema.MessageStart, Role: role}); err != nil {
		return nil, err
	}
	return w, nil
}
func (w *MessageWriter) ID() string { return w.id }
func (w *MessageWriter) apply(d schema.MessageDelta) error {
	if w.err != nil {
		return w.err
	}
	if w.ended {
		return errors.New("message already completed")
	}
	d.MessageID = w.id
	if err := w.accumulator.Apply(d); err != nil {
		return err
	}
	if err := PublishMessage(w.ctx, MessageUpdate{MessageID: w.id, Delta: &d}); err != nil {
		w.err = err
		return err
	}
	return nil
}
func (w *MessageWriter) addBlock(block schema.ContentBlock) (int, error) {
	index := w.blocks
	if err := w.apply(schema.MessageDelta{Op: schema.BlockStart, BlockIndex: index, Block: &block}); err != nil {
		return 0, err
	}
	w.blocks++
	return index, nil
}

// AppendText lazily opens a text block. AddBlock starts a new content boundary.
func (w *MessageWriter) AppendText(text string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.textBlock < 0 {
		index, err := w.addBlock(schema.NewTextBlock(""))
		if err != nil {
			return err
		}
		w.textBlock = index
	}
	return w.apply(schema.MessageDelta{Op: schema.BlockAppend, BlockIndex: w.textBlock, Text: text})
}
func (w *MessageWriter) AddBlock(block schema.ContentBlock) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	index, err := w.addBlock(block)
	if err == nil {
		w.textBlock = -1
	}
	return index, err
}

// Append applies one typed fragment to a previously opened block.
func (w *MessageWriter) Append(index int, delta schema.MessageDelta) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if delta.MessageID != "" && delta.MessageID != w.id {
		return errors.New("foreign message ID")
	}
	if delta.Op != "" && delta.Op != schema.BlockAppend {
		return errors.New("Append only accepts block_append")
	}
	delta.Op = schema.BlockAppend
	delta.BlockIndex = index
	return w.apply(delta)
}
func (w *MessageWriter) SetMetadata(meta *schema.ResponseMeta) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.apply(schema.MessageDelta{Op: schema.MessageMetadata, Meta: meta})
}

// End validates and publishes completion, returning the same message identity.
// Successful repeated End calls return isolated copies without republishing.
func (w *MessageWriter) End() (schema.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return schema.Message{}, w.err
	}
	if !w.ended {
		if err := w.apply(schema.MessageDelta{Op: schema.MessageEnd}); err != nil {
			return schema.Message{}, err
		}
		w.ended = true
	}
	m, _ := w.accumulator.Message(w.id)
	return m, nil
}
