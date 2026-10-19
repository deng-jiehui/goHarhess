package eval

import (
	"context"

	"github.com/goharness/goharness/extensions/retrieval"
)

// fixedCorpus 是固定的评测文档集：按主题分组的短文本，
// 哈希嵌入与词项重叠都能给出确定且合理的排序。
var fixedCorpus = []retrieval.VectorRecord{
	{ID: "go-install", Text: "how to install go golang toolchain download setup"},
	{ID: "go-module", Text: "go modules init dependency management go.mod"},
	{ID: "go-test", Text: "running go test unit tests coverage flags"},
	{ID: "git-rebase", Text: "git rebase interactive squash commits history"},
	{ID: "git-merge", Text: "git merge conflict resolution branch"},
	{ID: "docker-build", Text: "docker build image dockerfile layers cache"},
	{ID: "docker-run", Text: "docker run container ports volumes network"},
	{ID: "k8s-deploy", Text: "kubernetes deployment replicas rollout pods"},
	{ID: "http-timeout", Text: "http client timeout deadline context cancel"},
	{ID: "sql-index", Text: "database index btree query plan slow query"},
}

// fixedCases 是固定评测集：每条查询的相关文档由语料主题决定。
var fixedCases = []Case{
	{Query: "install golang toolchain", Relevant: []string{"go-install"}},
	{Query: "go dependency modules", Relevant: []string{"go-module"}},
	{Query: "run unit tests", Relevant: []string{"go-test"}},
	{Query: "squash commits rebase", Relevant: []string{"git-rebase"}},
	{Query: "merge conflict branch", Relevant: []string{"git-merge"}},
	{Query: "docker image layers", Relevant: []string{"docker-build"}},
	{Query: "container ports network", Relevant: []string{"docker-run"}},
	{Query: "kubernetes pods rollout", Relevant: []string{"k8s-deploy"}},
	{Query: "client timeout cancel", Relevant: []string{"http-timeout"}},
	{Query: "slow query index", Relevant: []string{"sql-index"}},
}

// FixedCases 返回固定评测集的副本，供对自有检索器跑同一套评测。
func FixedCases() []Case {
	return append([]Case(nil), fixedCases...)
}

// NewFixedSearcher 构建固定数据集上的混合检索器：哈希嵌入 +
// 内存向量 + 词项关键词，全离线、结果确定。
func NewFixedSearcher(ctx context.Context) (Searcher, error) {
	embedder := HashEmbedder{Dim: 64}
	store := &MemoryVectorStore{}
	keyword := MemoryKeywordSearch{Store: store}
	// 先嵌入语料再写入存储。
	resp, err := embedder.Invoke(ctx, retrieval.EmbeddingRequest{
		Inputs: corpusTexts(),
	})
	if err != nil {
		return nil, err
	}
	records := make([]retrieval.VectorRecord, len(fixedCorpus))
	for i, rec := range fixedCorpus {
		rec.Vector = resp.Vectors[i]
		records[i] = rec
	}
	if _, err := store.Invoke(ctx, retrieval.VectorRequest{
		Operation: retrieval.VectorUpsertOperation, Records: records,
	}); err != nil {
		return nil, err
	}
	return retrieval.NewHybridSearcher(retrieval.HybridSearchConfig{
		Embedder:       embedder,
		VectorStore:    store,
		KeywordSearch:  keyword,
		EmbeddingModel: "hash",
		VectorTopK:     5,
		KeywordTopK:    5,
		FinalTopK:      10,
	})
}

func corpusTexts() []string {
	texts := make([]string, len(fixedCorpus))
	for i, rec := range fixedCorpus {
		texts[i] = rec.Text
	}
	return texts
}
