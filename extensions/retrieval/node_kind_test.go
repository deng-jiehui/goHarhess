package retrieval_test

import (
	"testing"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

func TestRetrievalRunnablesDeclareRetrieverNodeKind(t *testing.T) {
	values := []runnable.NodeDescriptor{
		(*retrieval.DocumentIndexer)(nil),
		(*retrieval.HybridSearcher)(nil),
		(*retrieval.HybridRetriever)(nil),
	}
	for _, value := range values {
		if value.NodeKind() != runnable.NodeKindRetriever {
			t.Fatalf("node kind = %q", value.NodeKind())
		}
	}
}
