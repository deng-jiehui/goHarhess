// Package retrieval defines provider-neutral contracts for RAG data services.
// HTTP-backed implementations should use httpclient.NewClient so they share
// the framework connection pool.
package retrieval

// Metadata is structured data attached to records and search hits.
type Metadata map[string]any

// VectorRecord is the common representation stored by Qdrant, Chroma, Milvus,
// Pinecone and similar vector databases.
type VectorRecord struct {
	ID        string
	Vector    []float32
	Text      string
	Metadata  Metadata
	Namespace string
}

type VectorQuery struct {
	Vector    []float32
	TopK      int
	MinScore  float64
	Filter    Metadata
	Namespace string
	Include   IncludeOptions
	// Raw carries backend-specific search settings such as ef_search or nprobe.
	Raw map[string]any
}

type IncludeOptions struct {
	Vectors  bool
	Text     bool
	Metadata bool
}

type SearchHit struct {
	ID       string
	Score    float64
	Text     string
	Vector   []float32
	Metadata Metadata
	Source   string
	Raw      map[string]any
}

type VectorOperation string

const (
	VectorQueryOperation  VectorOperation = "query"
	VectorUpsertOperation VectorOperation = "upsert"
	VectorDeleteOperation VectorOperation = "delete"
	// VectorListOperation 按命名空间与元数据过滤列举记录（不需向量），
	// 供入库清理旧版本分块等维护路径使用。返回命中至少含 ID 与 Metadata。
	VectorListOperation VectorOperation = "list"
)

// VectorRequest is the single operation envelope implemented by vector
// database adapters and decorators.
type VectorRequest struct {
	Operation VectorOperation
	Query     *VectorQuery
	Records   []VectorRecord
	Namespace string
	IDs       []string
	// Filter 是 list 操作的元数据精确匹配过滤；_ns 等保留键由存储拒绝。
	Filter Metadata
}

type VectorResponse struct {
	Hits []SearchHit
}

type TextSearchRequest struct {
	Index   string
	Query   string
	TopK    int
	Filters Metadata
	Raw     map[string]any
}

type EmbeddingRequest struct {
	Model      string
	Inputs     []string
	Dimensions int
	Raw        map[string]any
}

type EmbeddingUsage struct {
	InputTokens int
	Raw         map[string]any
}

type EmbeddingResponse struct {
	Vectors [][]float32
	Usage   EmbeddingUsage
}

type RerankDocument struct {
	ID       string
	Text     string
	Metadata Metadata
}

type RerankRequest struct {
	Model     string
	Query     string
	Documents []RerankDocument
	TopK      int
	Raw       map[string]any
}

type RerankResult struct {
	Index    int
	ID       string
	Score    float64
	Document *RerankDocument
}
