package runnable_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"goharness/pkg/runnable"
)

type failingOption struct{}

func (failingOption) Apply(*runnable.OptionSet) error {
	return errors.New("invalid test option")
}

func TestRunnableInvokeBatchAndStream(t *testing.T) {
	component := runnable.Func[int, int](func(_ context.Context, value int, _ ...runnable.Option) (int, error) {
		return value * 2, nil
	})

	value, err := component.Invoke(context.Background(), 3)
	if err != nil || value != 6 {
		t.Fatalf("Invoke() = %d, %v", value, err)
	}
	values, err := runnable.Batch(context.Background(), component, []int{1, 2, 3})
	if err != nil || len(values) != 3 || values[2] != 6 {
		t.Fatalf("Batch() = %#v, %v", values, err)
	}
	last, err := runnable.Collect(context.Background(), runnable.Stream[int](runnable.Single(context.Background(), 7, nil)))
	if err != nil || last != 7 {
		t.Fatalf("Collect() = %d, %v", last, err)
	}
}

func TestResolveOptionsValidatesAndCollectsMetadata(t *testing.T) {
	resolved, err := runnable.ResolveOptions(runnable.WithMetadata("tenant", "tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Metadata["tenant"] != "tenant-a" {
		t.Fatalf("metadata = %#v", resolved.Metadata)
	}

	if _, err := runnable.ResolveOptions(failingOption{}); err == nil || !strings.Contains(err.Error(), "invalid test option") {
		t.Fatalf("failing option error = %v", err)
	}
	var nilOption runnable.Option
	if _, err := runnable.ResolveOptions(nilOption); err == nil || !strings.Contains(err.Error(), "option is nil") {
		t.Fatalf("nil option error = %v", err)
	}
}

func TestNodeOptionDeclaresDownstreamScopeWithoutChangingValidation(t *testing.T) {
	option := runnable.NodeOption(runnable.WithMetadata("model", "fast"))
	if option == nil || runnable.ScopeOf(option) != runnable.OptionScopeNode {
		t.Fatalf("node option scope = %q", runnable.ScopeOf(option))
	}
	resolved, err := runnable.ResolveOptions(option)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Metadata["model"] != "fast" {
		t.Fatalf("node option metadata = %#v", resolved.Metadata)
	}
}

func TestOptionPlanAppliesEachOptionOnceAcrossCascade(t *testing.T) {
	applications := 0
	option := runnable.OptionFunc(func(set *runnable.OptionSet) {
		applications++
		set.Metadata["value"] = applications
	})
	plan, err := runnable.NewOptionPlan(option)
	if err != nil {
		t.Fatal(err)
	}
	child, err := plan.Cascade(runnable.WithMetadata("local", true))
	if err != nil {
		t.Fatal(err)
	}
	if applications != 1 {
		t.Fatalf("parent option Apply count = %d, want 1", applications)
	}
	set := child.Set()
	if set.Metadata["value"] != 1 || set.Metadata["local"] != true {
		t.Fatalf("cascaded option set = %#v", set.Metadata)
	}
}

func TestOptionPlanForwardsOnlyTheOwningScopeDelta(t *testing.T) {
	plan, err := runnable.NewOptionPlan(
		runnable.GlobalOption(runnable.WithMetadata("tenant", "alpha")),
		runnable.NodeOption(runnable.WithMetadata("provider_mode", "fast")),
	)
	if err != nil {
		t.Fatal(err)
	}
	forwarded := plan.ForwardOptions()
	if len(forwarded) != 2 {
		t.Fatalf("forwarded options = %d, want 2", len(forwarded))
	}
	global, err := runnable.ResolveOptions(forwarded[0])
	if err != nil {
		t.Fatal(err)
	}
	node, err := runnable.ResolveOptions(forwarded[1])
	if err != nil {
		t.Fatal(err)
	}
	if global.Metadata["tenant"] != "alpha" || global.Metadata["provider_mode"] != nil {
		t.Fatalf("global scope leaked node metadata: %#v", global.Metadata)
	}
	if node.Metadata["provider_mode"] != "fast" || node.Metadata["tenant"] != nil {
		t.Fatalf("node scope leaked global metadata: %#v", node.Metadata)
	}
	globalOnly := plan.ForwardOptionsFor(runnable.OptionScopeGlobal)
	nodeOnly := plan.ForwardOptionsFor(runnable.OptionScopeNode)
	if len(globalOnly) != 1 || runnable.ScopeOf(globalOnly[0]) != runnable.OptionScopeGlobal {
		t.Fatalf("global filtered options = %#v", globalOnly)
	}
	if len(nodeOnly) != 1 || runnable.ScopeOf(nodeOnly[0]) != runnable.OptionScopeNode {
		t.Fatalf("node filtered options = %#v", nodeOnly)
	}
}

func TestOptionPlanForwardingReplaysDeletions(t *testing.T) {
	setValue := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) { set.Metadata["removed"] = "value" }))
	deleteValue := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) { delete(set.Metadata, "removed") }))
	plan, err := runnable.NewOptionPlan(setValue, deleteValue)
	if err != nil {
		t.Fatal(err)
	}
	forwarded, err := runnable.ResolveOptions(plan.ForwardOptionsFor(runnable.OptionScopeGlobal)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := forwarded.Metadata["removed"]; exists {
		t.Fatalf("deleted metadata was restored: %#v", forwarded.Metadata)
	}
}

func TestOptionPlanDoesNotLeakOpaqueGlobalValueToNodeScope(t *testing.T) {
	marker := func() {}
	global := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) { set.Values["opaque"] = marker }))
	node := runnable.NodeOption(runnable.WithMetadata("provider_mode", "fast"))
	plan, err := runnable.NewOptionPlan(global, node)
	if err != nil {
		t.Fatal(err)
	}
	forwarded, err := runnable.ResolveOptions(plan.ForwardOptionsFor(runnable.OptionScopeNode)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := forwarded.Values["opaque"]; exists {
		t.Fatalf("opaque global value leaked: %#v", forwarded.Values)
	}
	if forwarded.Metadata["provider_mode"] != "fast" {
		t.Fatalf("node metadata=%#v", forwarded.Metadata)
	}
}

func TestLambdaWrappersImplementTypedAndErasedRunnableContracts(t *testing.T) {
	invokable := runnable.InvokableLambda(func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	})
	var typed runnable.Runnable[int, int] = invokable
	var erased runnable.AnyRunnable = invokable
	value, err := typed.Invoke(context.Background(), 2)
	if err != nil || value != 3 {
		t.Fatalf("typed lambda = %d, %v", value, err)
	}
	anyValue, err := erased.InvokeAny(context.Background(), 4)
	if err != nil || anyValue != 5 {
		t.Fatalf("erased lambda = %#v, %v", anyValue, err)
	}

	streamable := runnable.StreamableLambda(func(ctx context.Context, value int) (runnable.Stream[int], error) {
		return runnable.Single(ctx, value*2, nil), nil
	})
	value, err = streamable.Invoke(context.Background(), 3)
	if err != nil || value != 6 {
		t.Fatalf("stream barrier lambda = %d, %v", value, err)
	}
}
