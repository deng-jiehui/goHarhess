package compose_test

import (
	"context"
	"strings"
	"testing"

	"goharness/pkg/compose"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type nilAgent struct{}

func (*nilAgent) Name() string        { return "nil-agent" }
func (*nilAgent) Description() string { return "" }
func (*nilAgent) Invoke(context.Context, []schema.Message, ...runnable.Option) ([]schema.Message, error) {
	return nil, nil
}

func TestGraphBuilderRejectsNilAgent(t *testing.T) {
	var value *nilAgent
	_, err := compose.NewGraph("nil-agent").
		Agent("agent", value).
		Edge("__start__", "agent").
		Edge("agent", "__end__").
		Build()
	if err == nil || !strings.Contains(err.Error(), "agent cannot be nil") {
		t.Fatalf("Build error=%v", err)
	}
}
