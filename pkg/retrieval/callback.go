package retrieval

import (
	"context"
	"errors"

	"github.com/goharness/goharness/pkg/callback"
)

type Operation string

const (
	OperationChunk         Operation = "chunk"
	OperationEmbedding     Operation = "embedding"
	OperationVectorStore   Operation = "vector_store"
	OperationTextSearch    Operation = "text_search"
	OperationRerank        Operation = "rerank"
	OperationHybridSearch  Operation = "hybrid_search"
	OperationDocumentIndex Operation = "document_index"
)

type CallbackInput struct {
	Operation  Operation
	Document   *Document
	Embedding  *EmbeddingRequest
	Vector     *VectorRequest
	TextSearch *TextSearchRequest
	Rerank     *RerankRequest
	Hybrid     *HybridQuery
	Documents  []Document
}

type CallbackOutput struct {
	Operation Operation
	Chunks    []Chunk
	Embedding *EmbeddingResponse
	Vector    *VectorResponse
	Hits      []SearchHit
	Reranked  []RerankResult
	Index     *IndexStats
}

type CallbackFailure struct {
	Input CallbackInput
	Err   error
}

type Callback interface {
	callback.Handler
	OnRetrieverStart(context.Context, CallbackInput) error
	OnRetrieverEnd(context.Context, CallbackOutput) error
	OnRetrieverError(context.Context, CallbackFailure) error
}

type CallbackFuncs struct {
	CallbackName string
	Start        func(context.Context, CallbackInput) error
	End          func(context.Context, CallbackOutput) error
	Error        func(context.Context, CallbackFailure) error
}

func (f CallbackFuncs) Name() string { return f.CallbackName }
func (f CallbackFuncs) OnRetrieverStart(ctx context.Context, input CallbackInput) error {
	if f.Start == nil {
		return nil
	}
	return f.Start(ctx, input.Clone())
}
func (f CallbackFuncs) OnRetrieverEnd(ctx context.Context, output CallbackOutput) error {
	if f.End == nil {
		return nil
	}
	return f.End(ctx, output.Clone())
}
func (f CallbackFuncs) OnRetrieverError(ctx context.Context, failure CallbackFailure) error {
	if f.Error == nil {
		return nil
	}
	failure.Input = failure.Input.Clone()
	return f.Error(ctx, failure)
}

func (i CallbackInput) Validate() error {
	count := 0
	for _, active := range []bool{i.Document != nil, i.Embedding != nil, i.Vector != nil, i.TextSearch != nil, i.Rerank != nil, i.Hybrid != nil, i.Documents != nil} {
		if active {
			count++
		}
	}
	if i.Operation == "" || count != 1 {
		return errors.New("retrieval callback input requires one operation-specific value")
	}
	return nil
}

func (i CallbackInput) Clone() CallbackInput {
	return cloneCallbackValue(i).(CallbackInput)
}

func (o CallbackOutput) Clone() CallbackOutput {
	return cloneCallbackValue(o).(CallbackOutput)
}

func cloneCallbackValue(value any) any { return callback.CloneValue(value) }

func BeginCallback(ctx context.Context, input CallbackInput) (context.Context, callback.Invocation) {
	operationCtx, invocation := callback.Begin(ctx, "retrieval", string(input.Operation), nil)
	_ = callback.Dispatch[Callback](operationCtx, invocation.WithPhase(callback.PhaseStart), func(ctx context.Context, handler Callback) error {
		return handler.OnRetrieverStart(ctx, input.Clone())
	})
	return operationCtx, invocation
}

func EndCallback(ctx context.Context, invocation callback.Invocation, output CallbackOutput, err error) {
	if err != nil {
		failure := CallbackFailure{Input: CallbackInput{Operation: output.Operation}, Err: err}
		_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseError), func(ctx context.Context, handler Callback) error {
			return handler.OnRetrieverError(ctx, failure)
		})
		return
	}
	_ = callback.Dispatch[Callback](ctx, invocation.WithPhase(callback.PhaseEnd), func(ctx context.Context, handler Callback) error {
		return handler.OnRetrieverEnd(ctx, output.Clone())
	})
}
