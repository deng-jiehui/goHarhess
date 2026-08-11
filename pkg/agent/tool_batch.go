package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"goharness/internal/toolruntime"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

type toolCallRequest struct {
	Name       string
	Arguments  string
	FunctionID string
}

type toolCallResult struct {
	Request toolCallRequest
	Content schema.Content
	Err     error
}

type toolTask struct {
	request  toolCallRequest
	messages []schema.Message
	done     chan struct{}
	result   toolCallResult
}

type toolTaskBatch struct {
	ctx      context.Context
	base     *Base
	opts     RunOptions
	executor *toolruntime.Scope

	mu    sync.Mutex
	tasks map[int]*toolTask
}

func (b *Base) newToolTaskBatch(ctx context.Context, opts RunOptions) *toolTaskBatch {
	maxParallel := opts.MaxParallelTools
	if maxParallel <= 0 {
		maxParallel = 1
	}
	return &toolTaskBatch{
		ctx:      ctx,
		base:     b,
		opts:     opts,
		executor: b.toolRuntime.NewScope(maxParallel, opts.ToolCallTimeout),
		tasks:    map[int]*toolTask{},
	}
}

func (b *Base) isToolCallReady(call toolCallRequest) bool {
	if !b.isToolCallComplete(call) {
		return false
	}
	for name := range b.tools {
		if name != call.Name && len(name) > len(call.Name) && name[:len(call.Name)] == call.Name {
			return false
		}
	}
	return true
}

func (b *Base) isToolCallComplete(call toolCallRequest) bool {
	if call.Name == "" {
		return false
	}
	if _, exists := b.tools[call.Name]; !exists {
		return false
	}
	_, err := tool.ParseArgsStrict(call.Arguments)
	return err == nil
}

func (b *toolTaskBatch) Submit(index int, call toolCallRequest, messages []schema.Message) bool {
	clonedMessages, cloneErr := schema.CloneMessagesStrict(messages)
	task := &toolTask{
		request:  call,
		messages: clonedMessages,
		done:     make(chan struct{}),
		result:   toolCallResult{Request: call, Err: cloneErr},
	}
	b.mu.Lock()
	if _, exists := b.tasks[index]; exists {
		b.mu.Unlock()
		return false
	}
	b.tasks[index] = task
	b.mu.Unlock()
	if cloneErr != nil {
		close(task.done)
		return true
	}
	go b.execute(task)
	return true
}

func (b *toolTaskBatch) execute(task *toolTask) {
	defer close(task.done)
	content, err := b.base.callTool(
		b.ctx,
		b.executor,
		task.request.Name,
		task.request.Arguments,
		task.messages,
		b.opts,
	)
	task.result = toolCallResult{Request: task.request, Content: content, Err: err}
}

func (b *toolTaskBatch) ResolveIncomplete(index int, result toolCallResult) {
	task := &toolTask{
		request: result.Request,
		done:    make(chan struct{}),
		result:  result,
	}
	close(task.done)
	b.mu.Lock()
	if _, exists := b.tasks[index]; !exists {
		b.tasks[index] = task
	}
	b.mu.Unlock()
}

func (b *toolTaskBatch) Join(calls []toolCallRequest) []toolCallResult {
	b.mu.Lock()
	tasks := make([]*toolTask, len(calls))
	for index := range calls {
		tasks[index] = b.tasks[index]
	}
	b.mu.Unlock()
	results := make([]toolCallResult, len(tasks))
	for index, task := range tasks {
		if task == nil {
			results[index] = toolCallResult{Err: errors.New("tool call task was not submitted")}
			continue
		}
		<-task.done
		results[index] = task.result
		results[index].Request = calls[index]
	}
	return results
}

func (b *Base) submitReadyToolCalls(
	batch *toolTaskBatch,
	calls []toolCallRequest,
	messages []schema.Message,
) {
	for index, call := range calls {
		if b.isToolCallReady(call) {
			batch.Submit(index, call, messages)
		}
	}
}

func (b *Base) resolveUnsubmittedToolCalls(
	batch *toolTaskBatch,
	calls []toolCallRequest,
	messages []schema.Message,
) {
	for index, call := range calls {
		if b.isToolCallComplete(call) {
			batch.Submit(index, call, messages)
		} else {
			batch.ResolveIncomplete(index, b.unresolvedToolCallResult(call))
		}
	}
}

func (b *Base) unresolvedToolCallResult(call toolCallRequest) toolCallResult {
	if call.Name == "" {
		return toolCallResult{Request: call, Err: errors.New("tool call name is incomplete")}
	}
	if _, exists := b.tools[call.Name]; !exists {
		return toolCallResult{
			Request: call,
			Content: schema.TextContent(fmt.Sprintf("Tool %s does not exist.", call.Name)),
		}
	}
	if _, err := tool.ParseArgsStrict(call.Arguments); err != nil {
		return toolCallResult{Request: call, Err: fmt.Errorf("tool call arguments are incomplete: %w", err)}
	}
	return toolCallResult{Request: call, Err: errors.New("tool call is incomplete")}
}
