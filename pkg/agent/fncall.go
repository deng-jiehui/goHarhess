package agent

import (
	"context"
	"fmt"
	"reflect"

	"goharness/pkg/schema"
)

type FnCallAgent struct {
	*Base
}

func NewFnCallAgent(cfg BaseConfig) (*FnCallAgent, error) {
	base, err := NewBase(cfg)
	if err != nil {
		return nil, err
	}
	return &FnCallAgent{Base: base}, nil
}

func (a *FnCallAgent) Run(ctx context.Context, messages []schema.Message, opts ...RunOption) (Stream, error) {
	runOpts := resolveRunOptions(opts...)
	prepared := a.prepareMessages(messages, &runOpts)
	return a.runPrepared(ctx, prepared, runOpts)
}

func (a *FnCallAgent) runPrepared(ctx context.Context, messages []schema.Message, opts RunOptions) (Stream, error) {
	if a.model == nil {
		return nil, ErrMissingModel
	}

	out := make(chan Chunk)
	go func() {
		defer close(out)

		working := schema.CloneMessages(messages)
		response := []schema.Message{}
		var lastEmitted []schema.Message

		emit := func(messages []schema.Message) bool {
			if len(messages) == 0 {
				return true
			}
			named := a.withAgentName(messages)
			if reflect.DeepEqual(named, lastEmitted) {
				return true
			}
			lastEmitted = schema.CloneMessages(named)
			select {
			case <-ctx.Done():
				out <- Chunk{Err: ctx.Err()}
				return false
			case out <- Chunk{Messages: named}:
				return true
			}
		}

		for remaining := opts.MaxLLMCalls; remaining > 0; remaining-- {
			llmStream, err := a.callLLM(ctx, working, a.toolDefinitions(), opts)
			if err != nil {
				out <- Chunk{Err: err}
				return
			}

			var output []schema.Message
			for chunk := range llmStream {
				if chunk.Err != nil {
					out <- Chunk{Err: chunk.Err}
					return
				}
				output = schema.CloneMessages(chunk.Messages)
				if len(output) > 0 && !emit(appendMessages(response, output)) {
					return
				}
			}
			if len(output) == 0 {
				break
			}

			response = append(response, output...)
			working = append(working, output...)

			usedTool := false
			for _, msg := range output {
				useTool, toolName, toolArgs, _ := DetectTool(msg)
				if !useTool {
					continue
				}
				toolContent, err := a.callTool(ctx, toolName, toolArgs, working, opts)
				if err != nil {
					toolContent = schema.TextContent(fmt.Sprintf("An error occurred when calling tool %q: %v", toolName, err))
				}

				fnMsg := schema.FunctionMessage(toolName, toolContent)
				fnMsg.Extra = map[string]any{"function_id": msg.FunctionID("1")}
				working = append(working, fnMsg)
				response = append(response, fnMsg)
				usedTool = true

				if !emit(response) {
					return
				}
			}

			if !usedTool {
				break
			}
		}

		emit(response)
	}()
	return out, nil
}

func DetectTool(message schema.Message) (bool, string, string, string) {
	if message.FunctionCall == nil {
		return false, "", "", message.Text()
	}
	return true, message.FunctionCall.Name, message.FunctionCall.Arguments, message.Text()
}

func appendMessages(left []schema.Message, right []schema.Message) []schema.Message {
	combined := make([]schema.Message, 0, len(left)+len(right))
	combined = append(combined, schema.CloneMessages(left)...)
	combined = append(combined, schema.CloneMessages(right)...)
	return combined
}
