package retrieval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// Limiter 是嵌入等外部调用的限流器：每个成员批次调用前等待。
// *rate.Limiter、令牌桶或自实现都满足该接口。
type Limiter interface {
	Wait(context.Context) error
}

// IngestorConfig 配置入库流水线。Store 建议用 Scoped 包装以钉住租户；
// Checkpoints 设置后流水线按批次落检查点：失败时已完成并写入检查点
// 的批次在恢复时复用、不会重复嵌入；失败瞬间仍在执行的兄弟批次会被
// 取消，恢复时重做（至少一次语义）。
type IngestorConfig struct {
	Loader    runnable.Runnable[Source, []Document]
	Chunker   runnable.Runnable[Document, []Chunk]
	Embedder  runnable.Runnable[EmbeddingRequest, EmbeddingResponse]
	Store     runnable.Runnable[VectorRequest, VectorResponse]
	Namespace string

	EmbeddingModel      string
	EmbeddingDimensions int
	EmbeddingRaw        map[string]any
	EmbeddingBatchSize  int
	Limiter             Limiter
	// MaxConcurrency 是同时执行的嵌入批次数上限；0 表示默认值 4，
	// 负数返回配置错误。嵌入服务通常有并发与速率限制：Limiter 管
	// 速率，MaxConcurrency 管并行度。设为 1 时批次按序执行，恢复
	// 语义最确定。
	MaxConcurrency int
	Checkpoints    state.Store
}

// defaultMaxConcurrency 是 MaxConcurrency 缺省时的批次并行度。
const defaultMaxConcurrency = 4

// IngestRequest 是一次入库：一篇文档一个运行，检查点键形如
// t/<租户>/ingest/<ID>（无租户作用域时为 ingest/<ID>）。
type IngestRequest struct {
	ID     string
	Source Source
}

// IngestStats 汇报一次入库的结果。
type IngestStats struct {
	Documents        int  `json:"documents"`
	Chunks           int  `json:"chunks"`
	EmbeddingBatches int  `json:"embedding_batches"`
	Upserted         int  `json:"upserted"`
	Removed          int  `json:"removed"`
	Unchanged        bool `json:"unchanged"` // 同版本重复入库，全部跳过
}

type ingestDoc struct {
	ID       string `json:"id"`
	Text     string `json:"text"`
	Metadata string `json:"metadata,omitempty"` // 预编码 JSON，保证状态冻结稳定
}

// ingestChunk 是分派成员与检查点里流转的分块形态：只用 JSON 往返稳定的
// 类型，文档元数据经预编码字符串携带。
type ingestChunk struct {
	ID       string `json:"id"`
	Doc      int    `json:"doc"`
	Index    int    `json:"index"`
	Text     string `json:"text"`
	Start    int    `json:"start"`
	End      int    `json:"end"`
	Tokens   int    `json:"tokens"`
	Metadata string `json:"metadata,omitempty"`
}

// ingestBatch 是分派成员的冻结输入：携带本批分块与记录构建所需的
// 全部上下文，全部字段 JSON 往返稳定。
type ingestBatch struct {
	DocID     string        `json:"doc_id"`
	Version   string        `json:"version"`
	Namespace string        `json:"namespace"`
	Chunks    []ingestChunk `json:"chunks"`
}

// ingestState 是流水线图的状态；所有字段必须 JSON 往返稳定。
type ingestState struct {
	DocID      string        `json:"doc_id"`
	SourceText string        `json:"source_text"`
	SourceName string        `json:"source_name,omitempty"`
	SourceMIME string        `json:"source_mime,omitempty"`
	SourceMeta string        `json:"source_meta,omitempty"`
	Version    string        `json:"version"`
	Namespace  string        `json:"namespace,omitempty"`
	Docs       []ingestDoc   `json:"docs,omitempty"`
	Chunks     []ingestChunk `json:"chunks,omitempty"`
	Stale      []string      `json:"stale,omitempty"`
	Stats      IngestStats   `json:"stats"`
	Unchanged  bool          `json:"unchanged"`
}

// Ingestor 是一条以图运行的入库流水线：
// 加载 → 分块 → 按嵌入批次分派（嵌入→upsert）→ 删除旧版本分块。
// 先写新块再删旧块，两步之间新旧并存，无检索空窗。
type Ingestor struct {
	cfg   IngestorConfig
	graph *graph.CompiledGraph[ingestState, *ingestState]
}

func NewIngestor(cfg IngestorConfig) (*Ingestor, error) {
	if cfg.Loader == nil {
		return nil, errors.New("ingestor requires a loader")
	}
	if cfg.Chunker == nil {
		return nil, errors.New("ingestor requires a chunker")
	}
	if cfg.Embedder == nil {
		return nil, errors.New("ingestor requires an embedder")
	}
	if cfg.Store == nil {
		return nil, errors.New("ingestor requires a vector store")
	}
	if cfg.EmbeddingBatchSize == 0 {
		cfg.EmbeddingBatchSize = DefaultEmbeddingBatchSize
	}
	if cfg.EmbeddingBatchSize < 1 || cfg.EmbeddingBatchSize > maxEmbeddingBatchSize {
		return nil, errors.New("ingestor embedding batch size out of range")
	}
	if cfg.EmbeddingDimensions < 0 {
		return nil, errors.New("ingestor embedding dimensions cannot be negative")
	}
	if cfg.MaxConcurrency < 0 {
		return nil, errors.New("ingestor max concurrency cannot be negative")
	}
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = defaultMaxConcurrency
	}
	ing := &Ingestor{cfg: cfg}
	compiled, err := ing.buildGraph()
	if err != nil {
		return nil, err
	}
	ing.graph = compiled
	return ing, nil
}

func (i *Ingestor) buildGraph() (*graph.CompiledGraph[ingestState, *ingestState], error) {
	g := graph.NewGraph[ingestState, *ingestState]("retrieval.ingest")
	if err := g.SetMaxConcurrency(i.cfg.MaxConcurrency); err != nil {
		return nil, err
	}
	if err := g.SetStateFactory(func(_ context.Context, in ingestState) (*ingestState, error) {
		state := in
		return &state, nil
	}); err != nil {
		return nil, err
	}
	// load：调用加载器把字节流归一化为文档集。
	load := runnable.Func[ingestState, ingestState](func(ctx context.Context, s ingestState, _ ...runnable.Option) (ingestState, error) {
		meta, err := decodeMetadata(s.SourceMeta)
		if err != nil {
			return s, fmt.Errorf("ingest source metadata: %w", err)
		}
		docs, err := i.cfg.Loader.Invoke(ctx, Source{
			Name:     s.SourceName,
			MIME:     s.SourceMIME,
			Body:     strings.NewReader(s.SourceText),
			Metadata: meta,
		})
		if err != nil {
			return s, err
		}
		for index, doc := range docs {
			raw, err := encodeMetadata(doc.Metadata)
			if err != nil {
				return s, fmt.Errorf("ingest document %d metadata: %w", index, err)
			}
			s.Docs = append(s.Docs, ingestDoc{ID: doc.ID, Text: doc.Text, Metadata: raw})
		}
		s.Stats.Documents = len(docs)
		return s, nil
	})
	if err := g.AddNode("load", load, graph.WithPostHandler(func(_ context.Context, s *ingestState, out ingestState) error {
		*s = out
		return nil
	})); err != nil {
		return nil, err
	}

	// prepare：分块、算版本、列出现有记录；同版本完整则标记跳过。
	prepare := runnable.Func[ingestState, ingestState](func(ctx context.Context, s ingestState, _ ...runnable.Option) (ingestState, error) {
		if s.DocID == "" {
			return s, errors.New("ingest document ID is required")
		}
		var chunks []ingestChunk
		sum := sha256.New()
		sum.Write([]byte(s.DocID))
		for di, doc := range s.Docs {
			sum.Write([]byte{0})
			sum.Write([]byte(doc.ID))
			sum.Write([]byte{0})
			sum.Write([]byte(doc.Text))
			out, err := i.cfg.Chunker.Invoke(ctx, Document{ID: doc.ID, Text: doc.Text})
			if err != nil {
				return s, fmt.Errorf("ingest chunk document %q: %w", doc.ID, err)
			}
			for _, c := range out {
				raw, err := encodeMetadata(c.Metadata)
				if err != nil {
					return s, fmt.Errorf("ingest chunk %q metadata: %w", c.ID, err)
				}
				chunks = append(chunks, ingestChunk{
					ID: c.ID, Doc: di, Index: c.Index, Text: c.Text,
					Start: c.Start, End: c.End, Tokens: c.Tokens, Metadata: raw,
				})
			}
		}
		s.Version = hex.EncodeToString(sum.Sum(nil)[:16])
		s.Namespace = i.namespace(ctx)
		s.Chunks = chunks
		s.Stats.Chunks = len(chunks)
		if len(chunks) == 0 {
			return s, nil
		}
		// 列出该文档全部现存分块：区分同版本（幂等跳过依据）与旧版本（待清理）。
		listed, err := i.cfg.Store.Invoke(ctx, VectorRequest{
			Operation: VectorListOperation,
			Namespace: s.Namespace,
			Filter:    Metadata{"_rag_document_id": s.DocID},
		})
		if err != nil {
			return s, fmt.Errorf("ingest list existing chunks: %w", err)
		}
		existing := map[string]bool{}
		for _, hit := range listed.Hits {
			if hit.Metadata["_rag_version"] == s.Version {
				existing[hit.ID] = true
			} else {
				s.Stale = append(s.Stale, hit.ID)
			}
		}
		s.Unchanged = len(existing) == len(chunks)
		if !s.Unchanged {
			s.Stats.EmbeddingBatches = (len(chunks) + i.cfg.EmbeddingBatchSize - 1) / i.cfg.EmbeddingBatchSize
		}
		return s, nil
	})
	if err := g.AddNode("prepare", prepare, graph.WithPostHandler(func(_ context.Context, s *ingestState, out ingestState) error {
		*s = out
		return nil
	})); err != nil {
		return nil, err
	}

	// batch：一个嵌入批次一个成员；嵌入→upsert，成员终态进检查点。
	if err := g.AddNode("batch", runnable.Func[ingestBatch, int](i.runBatch),
		graph.WithPostHandler(func(_ context.Context, s *ingestState, n int) error {
			s.Stats.Upserted += n
			return nil
		})); err != nil {
		return nil, err
	}
	if err := graph.AddDispatch(g, "prepare", []string{"batch"}, "cleanup",
		func(_ context.Context, s *ingestState) ([]graph.DispatchTask, error) {
			if s.Unchanged || len(s.Chunks) == 0 {
				return nil, nil
			}
			var tasks []graph.DispatchTask
			size := i.cfg.EmbeddingBatchSize
			for from := 0; from < len(s.Chunks); from += size {
				to := min(len(s.Chunks), from+size)
				tasks = append(tasks, graph.Send("batch", ingestBatch{
					DocID:     s.DocID,
					Version:   s.Version,
					Namespace: s.Namespace,
					Chunks:    s.Chunks[from:to],
				}))
			}
			return tasks, nil
		}); err != nil {
		return nil, err
	}

	// cleanup：汇合后删除旧版本分块（两步更新后半步）。
	cleanup := runnable.Func[ingestState, *ingestState](func(ctx context.Context, s ingestState, _ ...runnable.Option) (*ingestState, error) {
		out := s
		if len(out.Stale) > 0 {
			if _, err := i.cfg.Store.Invoke(ctx, VectorRequest{
				Operation: VectorDeleteOperation,
				Namespace: out.Namespace,
				IDs:       out.Stale,
			}); err != nil {
				return nil, fmt.Errorf("ingest cleanup stale chunks: %w", err)
			}
			out.Stats.Removed = len(out.Stale)
			out.Stale = nil
		}
		out.Stats.Unchanged = out.Unchanged
		return &out, nil
	})
	if err := g.AddNode("cleanup", cleanup,
		graph.WithPreHandler(func(_ context.Context, s *ingestState) (ingestState, error) { return *s, nil }),
		graph.WithPostHandler(func(_ context.Context, s *ingestState, out *ingestState) error {
			*s = *out
			return nil
		})); err != nil {
		return nil, err
	}

	for _, edge := range [][2]string{
		{graph.Start, "load"}, {"load", "prepare"}, {"batch", "cleanup"}, {"cleanup", graph.End},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			return nil, err
		}
	}
	// StepMerger 每次提交都会运行：顺序节点（无分支号）终态直接成为新
	// State；分派成员（Branch 非空）的终态是派发时刻的克隆+成员增量，
	// 只把成员写入的 Upserted 累加回主干。
	if err := graph.SetStepMerger(g, func(_ context.Context, base *ingestState, results []graph.StepResult[*ingestState]) error {
		for _, r := range results {
			if r.State == nil {
				continue
			}
			if r.Branch == "" {
				*base = *r.State
				continue
			}
			base.Stats.Upserted += r.State.Stats.Upserted
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return g.Compile()
}

// namespace 取本次入库的命名空间：静态配置优先，否则用租户作用域。
func (i *Ingestor) namespace(ctx context.Context) string {
	if i.cfg.Namespace != "" {
		return i.cfg.Namespace
	}
	if scope, ok := tenant.FromContext(ctx); ok {
		return scope.TenantID
	}
	return ""
}

// runBatch 是成员节点体：限流 → 嵌入 → upsert，返回写入条数。
func (i *Ingestor) runBatch(ctx context.Context, in ingestBatch, _ ...runnable.Option) (int, error) {
	if len(in.Chunks) == 0 {
		return 0, nil
	}
	if i.cfg.Limiter != nil {
		if err := i.cfg.Limiter.Wait(ctx); err != nil {
			return 0, err
		}
	}
	inputs := make([]string, len(in.Chunks))
	for index, chunk := range in.Chunks {
		inputs[index] = chunk.Text
	}
	response, err := i.cfg.Embedder.Invoke(ctx, EmbeddingRequest{
		Model:      i.cfg.EmbeddingModel,
		Inputs:     inputs,
		Dimensions: i.cfg.EmbeddingDimensions,
		Raw:        cloneStringAnyMap(i.cfg.EmbeddingRaw),
	})
	if err != nil {
		return 0, err
	}
	if err := validateEmbeddingBatch(0, response.Vectors, len(inputs), i.cfg.EmbeddingDimensions); err != nil {
		return 0, err
	}
	records := make([]VectorRecord, len(in.Chunks))
	for index, chunk := range in.Chunks {
		meta, err := decodeMetadata(chunk.Metadata)
		if err != nil {
			return 0, fmt.Errorf("ingest chunk %q metadata: %w", chunk.ID, err)
		}
		if meta == nil {
			meta = Metadata{}
		}
		meta["_rag_document_id"] = in.DocID
		meta["_rag_version"] = in.Version
		meta["_rag_chunk_index"] = chunk.Index
		meta["_rag_chunk_start"] = chunk.Start
		meta["_rag_chunk_end"] = chunk.End
		meta["_rag_chunk_tokens"] = chunk.Tokens
		records[index] = VectorRecord{
			ID:        chunk.ID,
			Vector:    append([]float32(nil), response.Vectors[index]...),
			Text:      chunk.Text,
			Metadata:  meta,
			Namespace: in.Namespace,
		}
	}
	_, err = i.cfg.Store.Invoke(ctx, VectorRequest{Operation: VectorUpsertOperation, Records: records})
	if err != nil {
		return 0, err
	}
	return len(records), nil
}

// checkpointKey 生成检查点会话键：有租户作用域时 t/<租户>/ingest/<ID>。
func (i *Ingestor) checkpointKey(ctx context.Context, docID string) string {
	if scope, ok := tenant.FromContext(ctx); ok {
		return "t/" + scope.TenantID + "/ingest/" + docID
	}
	return "ingest/" + docID
}

// Invoke 执行入库；设置 Checkpoints 时失败重跑自动从最近检查点恢复。
// 失败时，已完成并写入检查点的批次在恢复时复用，不会重复嵌入；失败
// 瞬间仍在执行的兄弟批次会被取消，恢复时重做（至少一次语义）。用
// MaxConcurrency 控制并行度，设为 1 时批次按序执行，已完成批次的
// 复用最确定。
func (i *Ingestor) Invoke(ctx context.Context, req IngestRequest, options ...runnable.Option) (IngestStats, error) {
	if ctx == nil {
		return IngestStats{}, errors.New("ingest context cannot be nil")
	}
	docID := strings.TrimSpace(req.ID)
	if docID == "" {
		return IngestStats{}, errors.New("ingest request ID is required")
	}
	if req.Source.Body == nil {
		return IngestStats{}, errors.New("ingest source body is required")
	}
	body, err := io.ReadAll(req.Source.Body)
	if err != nil {
		return IngestStats{}, fmt.Errorf("ingest read source: %w", err)
	}
	metaRaw, err := encodeMetadata(req.Source.Metadata)
	if err != nil {
		return IngestStats{}, fmt.Errorf("ingest source metadata: %w", err)
	}
	in := ingestState{
		DocID:      docID,
		SourceText: string(body),
		SourceName: req.Source.Name,
		SourceMIME: req.Source.MIME,
		SourceMeta: metaRaw,
	}
	opts := options
	if i.cfg.Checkpoints != nil {
		opts = append(append([]runnable.Option{}, options...),
			graph.WithCheckpoint(i.cfg.Checkpoints, i.checkpointKey(ctx, docID)))
	}
	out, err := i.graph.Invoke(ctx, in, opts...)
	if err != nil && i.cfg.Checkpoints != nil && errors.Is(err, state.ErrUnfinishedRun) {
		// 上次运行中途失败：同键恢复，已落检查点的批次直接复用。
		out, err = i.graph.Resume(ctx, opts...)
	}
	if err != nil {
		return IngestStats{}, err
	}
	return out.Stats, nil
}

func (i *Ingestor) Stream(ctx context.Context, input IngestRequest, options ...runnable.Option) (*runnable.StreamReader[IngestStats], error) {
	return runnable.StreamFromInvoke(ctx, input, i.Invoke, options...)
}

func (i *Ingestor) Collect(ctx context.Context, input *runnable.StreamReader[IngestRequest], options ...runnable.Option) (IngestStats, error) {
	return runnable.CollectFromInvoke(ctx, input, i.Invoke, options...)
}

func (i *Ingestor) Transform(ctx context.Context, input *runnable.StreamReader[IngestRequest], options ...runnable.Option) (*runnable.StreamReader[IngestStats], error) {
	return runnable.TransformFromInvoke(ctx, input, i.Invoke, options...)
}

var _ runnable.Runnable[IngestRequest, IngestStats] = (*Ingestor)(nil)

func (*Ingestor) NodeKind() runnable.NodeKind { return runnable.NodeKindRetriever }

func encodeMetadata(meta Metadata) (string, error) {
	if len(meta) == 0 {
		return "", nil
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodeMetadata(raw string) (Metadata, error) {
	if raw == "" {
		return nil, nil
	}
	meta := Metadata{}
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return nil, err
	}
	return meta, nil
}
