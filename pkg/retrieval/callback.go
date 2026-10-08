package retrieval

import (
	"context"
	"errors"

	"github.com/goharness/goharness/pkg/observe"
)

// kindRetrieval 是 retrieval 的观测事件类别：阶段三随包移到 extensions，
// 保持自定义 Kind 不归入核心枚举。
const kindRetrieval = observe.Kind("retrieval")

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

// CallbackInput/CallbackOutput/CallbackFailure 是观测事件的原始载荷：
// 经 observe.CapturePayload 脱敏后才进入 Event.Payload。
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

func cloneCallbackValue(value any) any { return observe.CloneValue(value) }

// BeginCallback 开启一次检索子操作并发射 kind=retrieval 的 start 事件。
func BeginCallback(ctx context.Context, input CallbackInput) (context.Context, observe.Invocation) {
	if !observe.Active(ctx) {
		return ctx, observe.Invocation{}
	}
	operationCtx, invocation := observe.Begin(ctx, "retrieval", string(input.Operation), nil)
	observe.EmitPayload(operationCtx, observe.EventOf(invocation.WithPhase(observe.PhaseStart), kindRetrieval, nil), input)
	return operationCtx, invocation
}

// EndCallback 收尾检索子操作：失败发 error 事件，成功发 end 事件。
func EndCallback(ctx context.Context, invocation observe.Invocation, output CallbackOutput, err error) {
	if invocation.Component == "" {
		return
	}
	if err != nil {
		failure := CallbackFailure{Input: CallbackInput{Operation: output.Operation}, Err: err}
		event := observe.EventOf(invocation.WithPhase(observe.PhaseError), kindRetrieval, nil)
		event.Err = err
		observe.EmitPayload(ctx, event, failure)
		return
	}
	observe.EmitPayload(ctx, observe.EventOf(invocation.WithPhase(observe.PhaseEnd), kindRetrieval, nil), output)
}
