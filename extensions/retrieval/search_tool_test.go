package retrieval_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// fakeSearcher 回送固定命中并记录收到的查询。
type fakeSearcher struct {
	hits []retrieval.SearchHit
	last retrieval.HybridQuery
}

func (f *fakeSearcher) Invoke(_ context.Context, q retrieval.HybridQuery, _ ...runnable.Option) ([]retrieval.SearchHit, error) {
	f.last = q
	return f.hits, nil
}
func (f *fakeSearcher) Stream(ctx context.Context, in retrieval.HybridQuery, o ...runnable.Option) (*runnable.StreamReader[[]retrieval.SearchHit], error) {
	return runnable.StreamFromInvoke(ctx, in, f.Invoke, o...)
}
func (f *fakeSearcher) Collect(ctx context.Context, in *runnable.StreamReader[retrieval.HybridQuery], o ...runnable.Option) ([]retrieval.SearchHit, error) {
	return runnable.CollectFromInvoke(ctx, in, f.Invoke, o...)
}
func (f *fakeSearcher) Transform(ctx context.Context, in *runnable.StreamReader[retrieval.HybridQuery], o ...runnable.Option) (*runnable.StreamReader[[]retrieval.SearchHit], error) {
	return runnable.TransformFromInvoke(ctx, in, f.Invoke, o...)
}

func TestSearchToolCitations(t *testing.T) {
	searcher := &fakeSearcher{hits: []retrieval.SearchHit{
		{ID: "c1", Score: 0.9, Text: "第一段内容", Source: "redis",
			Metadata: retrieval.Metadata{"_rag_document_id": "doc-a", "kind": "faq"}},
		{ID: "c2", Score: 0.5, Text: "第二段内容", Source: "redis"},
	}}
	st, err := retrieval.NewSearchTool(searcher, retrieval.SearchToolConfig{Index: "kb"})
	if err != nil {
		t.Fatal(err)
	}
	if !st.Spec().IsReadOnly {
		t.Fatal("search tool must be read-only")
	}
	result, err := st.Invoke(context.Background(), tool.CallInput{RawArgs: `{"query":"安装"}`})
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := result.ToContentBlocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("blocks = %d, want text+citations", len(blocks))
	}
	if blocks[0].Type != schema.ContentBlockTypeText || blocks[0].Text == nil || !containsAll(blocks[0].Text.Text, "[Reference 1", "第一段内容", "[Reference 2") {
		t.Fatalf("text block = %+v", blocks[0])
	}
	if blocks[1].Type != schema.ContentBlockTypeOpaque || blocks[1].Opaque == nil {
		t.Fatalf("citations block = %+v", blocks[1])
	}
	var citations []retrieval.Citation
	if err := json.Unmarshal(blocks[1].Opaque.Raw, &citations); err != nil {
		t.Fatal(err)
	}
	if len(citations) != 2 || citations[0].ID != "c1" || citations[0].DocumentID != "doc-a" {
		t.Fatalf("citations = %+v", citations)
	}
	// 命名空间不在工具参数里——无租户作用域时为空，模型无法指定。
	if searcher.last.Namespace != "" {
		t.Fatalf("namespace leaked into tool args: %q", searcher.last.Namespace)
	}
}

func TestSearchToolTenantFromContext(t *testing.T) {
	searcher := &fakeSearcher{hits: []retrieval.SearchHit{{ID: "x", Text: "t"}}}
	st, err := retrieval.NewSearchTool(searcher, retrieval.SearchToolConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := scopedCtx(t, "ta-1")
	if _, err := st.Invoke(ctx, tool.CallInput{RawArgs: `{"query":"q"}`}); err != nil {
		t.Fatal(err)
	}
	if searcher.last.Namespace != "ta-1" {
		t.Fatalf("namespace = %q, want tenant scope", searcher.last.Namespace)
	}
	// 参数里塞租户字段被 schema 校验拒绝，永远不可能覆盖作用域。
	if _, err := st.Invoke(ctx, tool.CallInput{RawArgs: `{"query":"q","namespace":"tb"}`}); err == nil {
		t.Fatal("args carrying namespace must be rejected")
	}
	if searcher.last.Namespace != "ta-1" {
		t.Fatal("args namespace must not override tenant scope")
	}
}

func TestSearchToolEmptyResult(t *testing.T) {
	searcher := &fakeSearcher{}
	st, err := retrieval.NewSearchTool(searcher, retrieval.SearchToolConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := st.Invoke(context.Background(), tool.CallInput{RawArgs: `{"query":"nothing"}`})
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := result.ToContentBlocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0].Text == nil || !containsAll(blocks[0].Text.Text, "没有找到") {
		t.Fatalf("empty result must give explicit message: %+v", blocks)
	}
}

func containsAll(text string, subs ...string) bool {
	for _, s := range subs {
		if !strings.Contains(text, s) {
			return false
		}
	}
	return true
}
