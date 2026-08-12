package retrieval

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"goharness/pkg/runnable"
)

const (
	DefaultEmbeddingBatchSize = 64
	DefaultUpsertBatchSize    = 100
	DefaultIndexConcurrency   = 4
	maxEmbeddingBatchSize     = 4096
	maxUpsertBatchSize        = 10_000
	maxIndexConcurrency       = 64
)

type IndexerConfig struct {
	Chunker             runnable.Runnable[Document, []Chunk]
	Embedder            runnable.Runnable[EmbeddingRequest, EmbeddingResponse]
	Store               runnable.Runnable[VectorRequest, VectorResponse]
	EmbeddingModel      string
	EmbeddingDimensions int
	EmbeddingRaw        map[string]any
	Namespace           string
	EmbeddingBatchSize  int
	UpsertBatchSize     int
	MaxConcurrency      int
}

type IndexStats struct {
	Documents        int
	Chunks           int
	Tokens           int
	EmbeddingBatches int
	UpsertBatches    int
}

// DocumentIndexer chunks documents, embeds batches concurrently and upserts
// vector records in bounded batches.
type DocumentIndexer struct {
	chunker             runnable.Runnable[Document, []Chunk]
	embedder            runnable.Runnable[EmbeddingRequest, EmbeddingResponse]
	store               runnable.Runnable[VectorRequest, VectorResponse]
	embeddingModel      string
	embeddingDimensions int
	embeddingRaw        map[string]any
	namespace           string
	embeddingBatchSize  int
	upsertBatchSize     int
	maxConcurrency      int
}

func NewDocumentIndexer(cfg IndexerConfig) (*DocumentIndexer, error) {
	if cfg.Chunker == nil {
		return nil, errors.New("document indexer requires a chunker")
	}
	if cfg.Embedder == nil {
		return nil, errors.New("document indexer requires an embedder")
	}
	if cfg.Store == nil {
		return nil, errors.New("document indexer requires a vector store")
	}
	if cfg.EmbeddingBatchSize == 0 {
		cfg.EmbeddingBatchSize = DefaultEmbeddingBatchSize
	}
	if cfg.UpsertBatchSize == 0 {
		cfg.UpsertBatchSize = DefaultUpsertBatchSize
	}
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = DefaultIndexConcurrency
	}
	if cfg.EmbeddingBatchSize < 1 || cfg.UpsertBatchSize < 1 || cfg.MaxConcurrency < 1 {
		return nil, errors.New("indexer batch sizes and concurrency must be greater than zero")
	}
	if cfg.EmbeddingBatchSize > maxEmbeddingBatchSize || cfg.UpsertBatchSize > maxUpsertBatchSize || cfg.MaxConcurrency > maxIndexConcurrency {
		return nil, errors.New("indexer batch sizes or concurrency exceed framework limits")
	}
	if cfg.EmbeddingDimensions < 0 {
		return nil, errors.New("embedding dimensions cannot be negative")
	}
	return &DocumentIndexer{
		chunker:             cfg.Chunker,
		embedder:            cfg.Embedder,
		store:               cfg.Store,
		embeddingModel:      cfg.EmbeddingModel,
		embeddingDimensions: cfg.EmbeddingDimensions,
		embeddingRaw:        cloneStringAnyMap(cfg.EmbeddingRaw),
		namespace:           cfg.Namespace,
		embeddingBatchSize:  cfg.EmbeddingBatchSize,
		upsertBatchSize:     cfg.UpsertBatchSize,
		maxConcurrency:      cfg.MaxConcurrency,
	}, nil
}

func (i *DocumentIndexer) Invoke(ctx context.Context, documents []Document, options ...runnable.Option) (IndexStats, error) {
	if ctx == nil {
		return IndexStats{}, errors.New("document index context cannot be nil")
	}
	stats := IndexStats{Documents: len(documents)}
	chunks := make([]Chunk, 0)
	seen := map[string]struct{}{}
	for _, document := range documents {
		values, err := i.chunker.Invoke(ctx, Document{
			ID: document.ID, Text: document.Text, Metadata: cloneMetadata(document.Metadata),
		}, options...)
		if err != nil {
			return stats, fmt.Errorf("chunk document %q: %w", document.ID, err)
		}
		for _, chunk := range values {
			if _, exists := seen[chunk.ID]; exists {
				continue
			}
			seen[chunk.ID] = struct{}{}
			chunks = append(chunks, chunk)
			stats.Tokens += chunk.Tokens
		}
	}
	stats.Chunks = len(chunks)
	if len(chunks) == 0 {
		return stats, nil
	}

	vectors, batches, err := i.embedChunks(ctx, chunks, options...)
	stats.EmbeddingBatches = batches
	if err != nil {
		return stats, err
	}
	records := make([]VectorRecord, len(chunks))
	for index, chunk := range chunks {
		metadata := cloneMetadata(chunk.Metadata)
		if metadata == nil {
			metadata = Metadata{}
		}
		metadata["_rag_document_id"] = chunk.DocumentID
		metadata["_rag_chunk_index"] = chunk.Index
		metadata["_rag_chunk_start"] = chunk.Start
		metadata["_rag_chunk_end"] = chunk.End
		metadata["_rag_chunk_tokens"] = chunk.Tokens
		records[index] = VectorRecord{
			ID:        chunk.ID,
			Vector:    append([]float32(nil), vectors[index]...),
			Text:      chunk.Text,
			Metadata:  metadata,
			Namespace: i.namespace,
		}
	}
	for start := 0; start < len(records); start += i.upsertBatchSize {
		end := min(len(records), start+i.upsertBatchSize)
		_, err := i.store.Invoke(ctx, VectorRequest{Operation: VectorUpsertOperation, Records: cloneVectorRecords(records[start:end])}, options...)
		if err != nil {
			return stats, fmt.Errorf("upsert chunk batch %d: %w", stats.UpsertBatches, err)
		}
		stats.UpsertBatches++
	}
	return stats, nil
}

var _ runnable.Runnable[[]Document, IndexStats] = (*DocumentIndexer)(nil)

func (i *DocumentIndexer) embedChunks(ctx context.Context, chunks []Chunk, options ...runnable.Option) ([][]float32, int, error) {
	vectors := make([][]float32, len(chunks))
	batchCount := (len(chunks) + i.embeddingBatchSize - 1) / i.embeddingBatchSize
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type embedJob struct {
		batch int
		start int
		end   int
	}
	jobs := make(chan embedJob)
	var wait sync.WaitGroup
	var protected struct {
		sync.Mutex
		err error
	}
	workerCount := min(i.maxConcurrency, batchCount)
	for range workerCount {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					inputs := make([]string, job.end-job.start)
					for offset := range inputs {
						inputs[offset] = chunks[job.start+offset].Text
					}
					response, err := i.embedder.Invoke(ctx, EmbeddingRequest{
						Model:      i.embeddingModel,
						Inputs:     inputs,
						Dimensions: i.embeddingDimensions,
						Raw:        cloneStringAnyMap(i.embeddingRaw),
					}, options...)
					if err == nil {
						err = validateEmbeddingBatch(job.batch, response.Vectors, len(inputs), i.embeddingDimensions)
					}
					if err != nil {
						protected.Lock()
						if protected.err == nil {
							protected.err = fmt.Errorf("embed chunk batch %d: %w", job.batch, err)
							cancel()
						}
						protected.Unlock()
						return
					}
					for offset, vector := range response.Vectors {
						vectors[job.start+offset] = append([]float32(nil), vector...)
					}
				}
			}
		}()
	}

sendJobs:
	for batch := 0; batch < batchCount; batch++ {
		start := batch * i.embeddingBatchSize
		job := embedJob{batch: batch, start: start, end: min(len(chunks), start+i.embeddingBatchSize)}
		select {
		case <-ctx.Done():
			break sendJobs
		case jobs <- job:
		}
	}
	close(jobs)
	wait.Wait()
	protected.Lock()
	err := protected.err
	protected.Unlock()
	if err != nil {
		return nil, batchCount, err
	}
	if err := ctx.Err(); err != nil {
		return nil, batchCount, err
	}
	return vectors, batchCount, nil
}

func validateEmbeddingBatch(batch int, vectors [][]float32, inputCount, dimensions int) error {
	if len(vectors) != inputCount {
		return fmt.Errorf("embedding batch %d returned %d vectors for %d inputs", batch, len(vectors), inputCount)
	}
	for offset, vector := range vectors {
		if len(vector) == 0 {
			return fmt.Errorf("embedding batch %d vector %d is empty", batch, offset)
		}
		if dimensions > 0 && len(vector) != dimensions {
			return fmt.Errorf(
				"embedding batch %d vector %d has %d dimensions, want %d",
				batch, offset, len(vector), dimensions,
			)
		}
	}
	return nil
}
