package redisstore

import (
	"strings"
	"testing"

	"github.com/goharness/goharness/extensions/retrieval"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	// 纯构造：client 为 nil，单测只走查询构造路径。
	return &Store{
		index: "idx", prefix: "doc:", dim: 4, distance: Cosine,
		metadata: map[string]MetadataField{
			"kind": {Name: "kind", Type: FieldTag},
			"year": {Name: "year", Type: FieldNumeric},
		},
	}
}

func TestEscapeTagSpecialChars(t *testing.T) {
	for _, in := range []string{"a,b", "a b", `a"b`, "a(b)", "a|b", "-a", "a@b", `a\b`} {
		got := escapeTag(in)
		for _, r := range `,."()|@- ` {
			if strings.ContainsRune(got, r) && !strings.Contains(got, `\`+string(r)) {
				t.Fatalf("escapeTag(%q) = %q still has unescaped %q", in, got, r)
			}
		}
	}
	if escapeTag("plain") != "plain" {
		t.Fatal("plain value should pass through")
	}
}

func TestVectorQueryForcesNamespaceFilter(t *testing.T) {
	s := newTestStore(t)
	q, k, err := vectorQuery(s, &retrieval.VectorQuery{
		Namespace: "t1", TopK: 5, Vector: []float32{1, 0, 0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if k != 5 || !strings.HasPrefix(q, "@ns:{t1}") || !strings.Contains(q, "KNN 5 @vec $vec AS score") {
		t.Fatalf("query = %q k = %d", q, k)
	}
}

func TestVectorQueryRejectsEmptyNamespace(t *testing.T) {
	s := newTestStore(t)
	if _, _, err := vectorQuery(s, &retrieval.VectorQuery{Namespace: "", TopK: 1}); err == nil {
		t.Fatal("empty namespace must fail")
	}
}

func TestVectorQueryEscapesNamespace(t *testing.T) {
	s := newTestStore(t)
	q, _, err := vectorQuery(s, &retrieval.VectorQuery{Namespace: `a,b"c`, TopK: 1, Vector: []float32{1}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q, `{a,b"c}`) {
		t.Fatalf("namespace not escaped: %q", q)
	}
	if !strings.Contains(q, `a\,b\"c`) {
		t.Fatalf("escaped namespace missing: %q", q)
	}
}

func TestVectorQueryUndeclaredFilterFails(t *testing.T) {
	s := newTestStore(t)
	_, _, err := vectorQuery(s, &retrieval.VectorQuery{
		Namespace: "t1", TopK: 1, Vector: []float32{1},
		Filter: retrieval.Metadata{"evil": "x"},
	})
	if err == nil || !strings.Contains(err.Error(), "not a declared field") {
		t.Fatalf("err = %v", err)
	}
}

func TestVectorQueryDeclaredFilters(t *testing.T) {
	s := newTestStore(t)
	q, _, err := vectorQuery(s, &retrieval.VectorQuery{
		Namespace: "t1", TopK: 2, Vector: []float32{1},
		Filter: retrieval.Metadata{"kind": "doc", "year": 2024},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "@kind:{doc}") || !strings.Contains(q, "@year:[2024 2024]") {
		t.Fatalf("filters missing: %q", q)
	}
}

func TestVectorQueryNumericRangeFilter(t *testing.T) {
	s := newTestStore(t)
	q, _, err := vectorQuery(s, &retrieval.VectorQuery{
		Namespace: "t1", TopK: 1, Vector: []float32{1},
		Filter: retrieval.Metadata{"year": [2]float64{2020, 2024}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "@year:[2020 2024]") {
		t.Fatalf("range filter missing: %q", q)
	}
}

func TestKeywordQueryEscapesUserText(t *testing.T) {
	s := newTestStore(t)
	q, err := keywordQuery(s, retrieval.TextSearchRequest{
		Query:   `foo (bar) | -baz "qux" @at`,
		Filters: retrieval.Metadata{nsFilterKey: "t1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(q, "@ns:{t1}") {
		t.Fatalf("missing ns filter: %q", q)
	}
	// 每个词必须被引号包裹：特殊字符只能出现在引号内。
	want := `@ns:{t1} "foo" "(bar)" "|" "-baz" "\"qux\"" "@at"`
	if q != want {
		t.Fatalf("query = %q, want %q", q, want)
	}
}

func TestKeywordQueryEmptyFails(t *testing.T) {
	s := newTestStore(t)
	if _, err := keywordQuery(s, retrieval.TextSearchRequest{Query: "   ", Filters: retrieval.Metadata{nsFilterKey: "t1"}}); err == nil {
		t.Fatal("empty query must fail")
	}
	if _, err := keywordQuery(s, retrieval.TextSearchRequest{Query: "x"}); err == nil {
		t.Fatal("missing ns must fail")
	}
}

func TestKeywordQueryIgnoresNsKeyInMetadata(t *testing.T) {
	s := newTestStore(t)
	q, err := keywordQuery(s, retrieval.TextSearchRequest{
		Query:   "hello",
		Filters: retrieval.Metadata{nsFilterKey: "t1", "kind": "note"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "@kind:{note}") || strings.Contains(q, nsFilterKey+":") {
		t.Fatalf("bad query: %q", q)
	}
}
