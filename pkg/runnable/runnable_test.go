package runnable_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/runnable"
)

func requireCompleteRunnable[I, O any](value runnable.Runnable[I, O]) {
	_ = value.Invoke
	_ = value.Stream
	_ = value.Collect
	_ = value.Transform
}

func TestRunnableExposesAllExecutionForms(t *testing.T) {
	value := runnable.InvokableLambda(func(_ context.Context, input int) (string, error) {
		return fmt.Sprint(input), nil
	})
	requireCompleteRunnable[int, string](value)
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
	reader, writer := runnable.Pipe[int](context.Background())
	go func() {
		_ = writer.Send(7)
		_ = writer.Close()
	}()
	last, err := reader.Collect(context.Background())
	if err != nil || last != 7 {
		t.Fatalf("Collect() = %d, %v", last, err)
	}
}

type testConfig struct {
	Value int
	Label string
}

type otherConfig struct{ Value int }

func withTestValue(v int) runnable.Option {
	return runnable.WrapOption(func(c *testConfig) { c.Value = v })
}

func withOtherValue(v int) runnable.Option {
	return runnable.WrapOption(func(c *otherConfig) { c.Value = v })
}

func TestResolveComponentOptionsAppliesOwnedTypeInOrder(t *testing.T) {
	resolved, remaining, err := runnable.ResolveComponentOptions(testConfig{Value: 1, Label: "base"},
		withTestValue(5), withOtherValue(9), withTestValue(7))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != 7 || resolved.Label != "base" {
		t.Fatalf("resolved = %#v", resolved)
	}
	if len(remaining) != 1 {
		t.Fatalf("remaining = %d, want 1", len(remaining))
	}
	other, _, err := runnable.ResolveComponentOptions(otherConfig{}, remaining...)
	if err != nil || other.Value != 9 {
		t.Fatalf("remaining resolve = %#v, %v", other, err)
	}
}

func TestResolveComponentOptionsErrors(t *testing.T) {
	var nilOption runnable.Option
	if _, _, err := runnable.ResolveComponentOptions(testConfig{}, nilOption); err == nil {
		t.Fatal("nil option should fail")
	}
	if _, _, err := runnable.ResolveComponentOptions(testConfig{}, runnable.WrapOption[testConfig](nil)); err == nil {
		t.Fatal("nil apply should fail")
	}
	panicOption := runnable.WrapOption(func(*testConfig) { panic("boom") })
	if _, _, err := runnable.ResolveComponentOptions(testConfig{}, panicOption); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("panic error = %v", err)
	}
	ptrOption := runnable.WrapOption[*testConfig](func(**testConfig) {})
	if _, _, err := runnable.ResolveComponentOptions(testConfig{}, ptrOption); err == nil {
		t.Fatal("pointer config type should fail")
	}
}

func TestCheckNoRemainingOptions(t *testing.T) {
	if err := runnable.CheckNoRemainingOptions(withOtherValue(1)); err == nil {
		t.Fatal("foreign option should fail")
	}
	targeted := runnable.ForNode("node", withTestValue(1))
	if err := runnable.CheckNoRemainingOptions(targeted); err == nil || !strings.Contains(err.Error(), "node") {
		t.Fatalf("targeted leftover error = %v", err)
	}
}

func TestFlattenOptionsExposesPath(t *testing.T) {
	specs, err := runnable.FlattenOptions([]runnable.Option{
		runnable.ForPath([]string{"sub", "search"}, withTestValue(3)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("specs = %d, want 1", len(specs))
	}
	if specs[0].ConfigType == nil || len(specs[0].Path) != 2 || specs[0].Path[0] != "sub" {
		t.Fatalf("targeted spec = %#v", specs[0])
	}
}

func TestFlattenOptionsRejectsInvalidConstruction(t *testing.T) {
	cases := [][]runnable.Option{
		{runnable.ForNode("")},
		{runnable.ForPath(nil, withTestValue(1))},
		{runnable.ForPath([]string{"a", ""}, withTestValue(1))},
		{runnable.ForPath([]string{"a"})},
		{runnable.ForNode("a", runnable.ForNode("b", withTestValue(1)))},
	}
	for index, options := range cases {
		if _, err := runnable.FlattenOptions(options); err == nil {
			t.Fatalf("case %d should fail", index)
		}
	}
}

func TestOptionsFromSpecsRoundTrip(t *testing.T) {
	specs, err := runnable.FlattenOptions([]runnable.Option{
		runnable.ForNode("search", withTestValue(4)),
	})
	if err != nil {
		t.Fatal(err)
	}
	// 消费一段路径后再包装，内部语义保持。
	for index := range specs {
		if len(specs[index].Path) > 0 {
			specs[index].Path = specs[index].Path[1:]
		}
	}
	options := runnable.OptionsFromSpecs(specs)
	resolved, remaining, err := runnable.ResolveComponentOptions(testConfig{}, options...)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != 4 {
		t.Fatalf("resolved = %#v", resolved)
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining = %d, want 0", len(remaining))
	}
}

// 冻结选项与原始设置函数语义一致：显式赋零值、写入与原值相同的值都被
// 保留；未被触碰的字段保留组件默认值；map 合并型 setter 语义不变。
func TestFreezeSpecsPreservesSetterSemantics(t *testing.T) {
	freeze := func(t *testing.T, opts ...runnable.Option) []runnable.Option {
		t.Helper()
		frozen, err := runnable.FreezeOptions(opts)
		if err != nil {
			t.Fatal(err)
		}
		return frozen
	}
	base := func() testConfig { return testConfig{Value: 7, Label: "keep"} }

	// 显式赋零值不能被丢弃：组件默认值必须被覆盖为 0。
	resolved, _, err := runnable.ResolveComponentOptions(base(), freeze(t, withTestValue(0))...)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != 0 {
		t.Fatalf("explicit zero write lost: %#v", resolved)
	}
	// 写入与基线相同的值同样保留——针对默认值不同的组件解析基底。
	resolved, _, err = runnable.ResolveComponentOptions(testConfig{Value: 99, Label: "keep"}, freeze(t, withTestValue(7))...)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Value != 7 {
		t.Fatalf("same-value write lost: %#v", resolved)
	}
	// 未触碰字段保留解析基底的原值。
	if resolved.Label != "keep" {
		t.Fatalf("untouched field overwritten: %#v", resolved)
	}

	// 合并型 setter 的累计语义保留：多个选项并入同一 map，冻结快照承载
	// 累计结果。冻结边界每个设置函数只执行一次（无可观测写入的 spec
	// 原样透传，由组件在真实基底上重放）。
	type mergeConfig struct{ Tags map[string]string }
	calls := 0
	tag := func(key, value string) runnable.Option {
		return runnable.WrapOption(func(c *mergeConfig) {
			calls++
			if c.Tags == nil {
				c.Tags = map[string]string{}
			}
			c.Tags[key] = value
		})
	}
	frozen := freeze(t, tag("a", "1"), tag("b", "2"))
	freezeCalls := calls
	if freezeCalls != 2 {
		t.Fatalf("setter executions during freeze = %d, want 2", freezeCalls)
	}
	resolvedMerge, _, err := runnable.ResolveComponentOptions(mergeConfig{}, frozen...)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedMerge.Tags["a"] != "1" || resolvedMerge.Tags["b"] != "2" {
		t.Fatalf("cumulative merge lost: %#v", resolvedMerge.Tags)
	}
	// 合并型写入在预置非空基底的组件上保留已有条目而不是整体覆盖。
	resolvedMerge, _, err = runnable.ResolveComponentOptions(
		mergeConfig{Tags: map[string]string{"base": "0"}}, frozen...)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedMerge.Tags["base"] != "0" || resolvedMerge.Tags["a"] != "1" || resolvedMerge.Tags["b"] != "2" {
		t.Fatalf("merge onto prepopulated base lost entries: %#v", resolvedMerge.Tags)
	}
	// 再次应用冻结结果不重复执行原始设置函数。
	if _, _, err := runnable.ResolveComponentOptions(mergeConfig{}, frozen...); err != nil {
		t.Fatal(err)
	}
	if calls != freezeCalls {
		t.Fatalf("setter re-executed on apply: %d", calls)
	}

	// 差分不可见的写入（显式赋零）以原始 spec 透传：Frozen 为 false，
	// 组件端按真实解析基底执行原始设置函数。
	zeroFrozen := freeze(t, withTestValue(0))
	zeroSpecs, err := runnable.FlattenOptions(zeroFrozen)
	if err != nil {
		t.Fatal(err)
	}
	if len(zeroSpecs) != 1 || zeroSpecs[0].Frozen {
		t.Fatalf("invisible write should pass through unfrozen: %#v", zeroSpecs)
	}
}

func TestLambdaWrappersImplementCompleteExecutionContract(t *testing.T) {
	invokable := runnable.InvokableLambda(func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	})
	var typed runnable.Runnable[int, int] = invokable
	value, err := typed.Invoke(context.Background(), 2)
	if err != nil || value != 3 {
		t.Fatalf("typed lambda = %d, %v", value, err)
	}

	streamable := runnable.StreamableLambda(func(ctx context.Context, value int) (*runnable.StreamReader[int], error) {
		reader, writer := runnable.Pipe[int](ctx)
		go func() {
			_ = writer.Send(value * 2)
			_ = writer.Close()
		}()
		return reader, nil
	})
	value, err = streamable.Invoke(context.Background(), 3)
	if err != nil || value != 6 {
		t.Fatalf("stream barrier lambda = %d, %v", value, err)
	}
}
