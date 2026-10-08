package contextengine

import (
	"testing"

	"github.com/goharness/goharness/pkg/schema"
)

func TestTruncateBlocksPreservesNilAndEmptySlices(t *testing.T) {
	tests := []struct {
		name   string
		blocks []schema.ContentBlock
		nil    bool
	}{
		{name: "nil", blocks: nil, nil: true},
		{name: "empty", blocks: []schema.ContentBlock{}, nil: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := truncateBlocks(test.blocks, 0, ApproxTokenCounter{}, true)
			if (got == nil) != test.nil || len(got) != 0 {
				t.Fatalf("truncateBlocks(%#v) = %#v", test.blocks, got)
			}
		})
	}
}
