package retrieval_test

import (
	"testing"

	"github.com/goharness/goharness/pkg/retrieval"
)

func TestRetrieverCallbackInputRequiresOneTypedValue(t *testing.T) {
	valid := retrieval.CallbackInput{Operation: retrieval.OperationHybridSearch, Hybrid: &retrieval.HybridQuery{Text: "query"}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Vector = &retrieval.VectorRequest{}
	if err := invalid.Validate(); err == nil {
		t.Fatal("multiple active callback values were accepted")
	}
}

func TestRetrieverCallbackCloneIsolatesMetadata(t *testing.T) {
	input := retrieval.CallbackInput{Operation: retrieval.OperationHybridSearch, Hybrid: &retrieval.HybridQuery{Text: "query", Filter: retrieval.Metadata{"nested": map[string]any{"owner": "original"}}}}
	cloned := input.Clone()
	cloned.Hybrid.Filter["nested"].(map[string]any)["owner"] = "changed"
	if input.Hybrid.Filter["nested"].(map[string]any)["owner"] != "original" {
		t.Fatal("metadata mutation leaked")
	}
}
