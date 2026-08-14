package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// GraphStateCheckpoint is the portable representation of a typed graph State.
// Data is owned by the checkpoint and copied at API boundaries.
type GraphStateCheckpoint struct {
	CodecID string `json:"codec_id"`
	Version int    `json:"version"`
	Data    []byte `json:"data"`
}

func (c GraphStateCheckpoint) Clone() GraphStateCheckpoint {
	c.Data = append([]byte(nil), c.Data...)
	return c
}

func (c GraphStateCheckpoint) Validate() error {
	if strings.TrimSpace(c.CodecID) == "" {
		return errors.New("graph state checkpoint codec ID cannot be empty")
	}
	if c.Version <= 0 {
		return fmt.Errorf("graph state checkpoint version must be positive, got %d", c.Version)
	}
	return nil
}

// CursorFrame is a portable Graph execution frame without runtime objects.
type CursorFrame struct {
	Graph     string `json:"graph"`
	Node      string `json:"node"`
	Step      int    `json:"step"`
	Iteration int    `json:"iteration,omitempty"`
}

type CursorStack []CursorFrame

func (s CursorStack) Clone() CursorStack {
	return append(CursorStack(nil), s...)
}

func (s CursorStack) Validate() error {
	for index, frame := range s {
		if strings.TrimSpace(frame.Graph) == "" {
			return fmt.Errorf("cursor frame %d graph cannot be empty", index)
		}
		if strings.TrimSpace(frame.Node) == "" {
			return fmt.Errorf("cursor frame %d node cannot be empty", index)
		}
		if frame.Step < 0 || frame.Iteration < 0 {
			return fmt.Errorf("cursor frame %d counters cannot be negative", index)
		}
	}
	return nil
}

type cursorStackContextKey struct{}

// WithCursorStack makes a defensive cursor stack available to nested nodes.
func WithCursorStack(ctx context.Context, stack CursorStack) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, cursorStackContextKey{}, stack.Clone())
}

// CursorStackFromContext returns an independent cursor stack for the current run.
func CursorStackFromContext(ctx context.Context) CursorStack {
	if ctx == nil {
		return nil
	}
	stack, _ := ctx.Value(cursorStackContextKey{}).(CursorStack)
	return stack.Clone()
}
