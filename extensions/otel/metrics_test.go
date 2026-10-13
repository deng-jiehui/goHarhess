package otel_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goharness/goharness/extensions/otel"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collect 收集全部指标为 name -> datapoints。
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string][]metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string][]metricdata.DataPoint[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				out[m.Name] = append(out[m.Name], data.DataPoints...)
			case metricdata.Gauge[int64]:
				out[m.Name] = append(out[m.Name], data.DataPoints...)
			}
		}
	}
	return out
}

func histogramCount(t *testing.T, reader *sdkmetric.ManualReader, name string) (int, []metricdata.HistogramDataPoint[float64]) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			data, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s type %T, want histogram", name, m.Data)
			}
			total := 0
			for _, dp := range data.DataPoints {
				total += int(dp.Count)
			}
			return total, data.DataPoints
		}
	}
	return 0, nil
}

// hasAttrKV/hasAttrHist 断言属性集合含给定键值对。
func hasAttrKV(set attribute.Set, key, value string) bool {
	v, ok := set.Value(attribute.Key(key))
	return ok && v.AsString() == value
}

func hasAttrHist(dp metricdata.HistogramDataPoint[float64], key, value string) bool {
	return hasAttrKV(dp.Attributes, key, value)
}

func attributeKey(key string) attribute.Key { return attribute.Key(key) }

// 指标观察者的端到端：真实 run 事件序列 → 各指标与标签。
func TestMetricsFromEventStream(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	observer, err := otel.Metrics(provider.Meter("test"), otel.MetricsConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	trace := observe.Trace{TraceID: "t1", SpanID: "s1"}
	attrs := map[string]any{"agent": "lead", observe.AttrTenantID: "t1", "checkpoint.store": "redis.Store"}

	observer.OnEvent(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseStart, Name: "react", Trace: trace, Attrs: attrs})
	if dp := collect(t, reader)["goharness.run.active"]; len(dp) != 1 || dp[0].Value != 1 {
		t.Fatalf("run.active after start = %v", dp)
	}
	observer.OnEvent(ctx, observe.Event{Kind: observe.KindNode, Phase: observe.PhaseEnd, Name: "model", Trace: trace, Duration: 20 * time.Millisecond})
	observer.OnEvent(ctx, observe.Event{Kind: observe.KindLLM, Phase: observe.PhaseEnd, Name: "gpt-4o", Trace: trace, Duration: 50 * time.Millisecond,
		Usage: &schema.TokenUsage{InputTokens: 100, OutputTokens: 40, ReasoningTokens: 10, CacheReadTokens: 5}})
	observer.OnEvent(ctx, observe.Event{Kind: observe.KindTool, Phase: observe.PhaseEnd, Name: "search", Trace: trace, Duration: 10 * time.Millisecond})
	observer.OnEvent(ctx, observe.Event{Kind: observe.KindCheckpoint, Phase: observe.PhaseEnd, Name: "save", Trace: trace, Duration: 3 * time.Millisecond, Attrs: attrs})
	observer.OnEvent(ctx, observe.Event{Kind: observe.KindInterrupt, Phase: observe.PhaseEvent, Name: "approval", Trace: trace, Attrs: attrs})
	observer.OnEvent(ctx, observe.Event{Kind: observe.KindRun, Phase: observe.PhaseEnd, Name: "react", Trace: trace, Duration: 90 * time.Millisecond, Attrs: attrs})

	points := collect(t, reader)
	if dp := points["goharness.run.active"]; len(dp) != 1 || dp[0].Value != 0 || !hasAttrKV(dp[0].Attributes, "agent", "lead") {
		t.Fatalf("run.active after end = %v", dp)
	}
	if n, dps := histogramCount(t, reader, "goharness.run.duration"); n != 1 || !hasAttrHist(dps[0], "status", "ok") {
		t.Fatalf("run.duration n=%d attrs=%v", n, dps)
	}
	if n, _ := histogramCount(t, reader, "goharness.node.duration"); n != 1 {
		t.Fatalf("node.duration n=%d", n)
	}
	if n, _ := histogramCount(t, reader, "goharness.llm.duration"); n != 1 {
		t.Fatalf("llm.duration n=%d", n)
	}
	if n, dps := histogramCount(t, reader, "goharness.tool.duration"); n != 1 || !hasAttrHist(dps[0], "tool", "search") {
		t.Fatalf("tool.duration n=%d", n)
	}
	if n, dps := histogramCount(t, reader, "goharness.checkpoint.save.duration"); n != 1 || !hasAttrHist(dps[0], "store", "redis.Store") {
		t.Fatalf("checkpoint.save.duration n=%d", n)
	}
	directions := map[string]int64{}
	for _, dp := range points["goharness.llm.tokens"] {
		v, _ := dp.Attributes.Value(attributeKey("direction"))
		directions[v.AsString()] = dp.Value
	}
	if directions["input"] != 100 || directions["output"] != 40 || directions["reasoning"] != 10 || directions["cache_read"] != 5 {
		t.Fatalf("llm.tokens = %v", directions)
	}
	if total(points["goharness.interrupts"]) != 1 {
		t.Fatalf("interrupts = %v", points["goharness.interrupts"])
	}
}

func TestMetricsConflictCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	observer, err := otel.Metrics(provider.Meter("test"), otel.MetricsConfig{})
	if err != nil {
		t.Fatal(err)
	}
	observer.OnEvent(context.Background(), observe.Event{
		Kind: observe.KindCheckpoint, Phase: observe.PhaseError, Name: "save",
		Err:   fmt.Errorf("wrap: %w", state.ErrConflict),
		Attrs: map[string]any{"checkpoint.store": "redis.Store"},
	})
	points := collect(t, reader)
	if total(points["goharness.checkpoint.conflicts"]) != 1 {
		t.Fatalf("conflicts = %v", points["goharness.checkpoint.conflicts"])
	}
}

// 租户标签基数保护：超过上限的租户合并为 other。
func TestMetricsTenantLabelCap(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	observer, err := otel.Metrics(provider.Meter("test"), otel.MetricsConfig{TenantLabelMax: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"t1", "t2", "t3"} {
		observer.OnEvent(context.Background(), observe.Event{
			Kind: observe.KindInterrupt, Phase: observe.PhaseEvent, Name: "i",
			Attrs: map[string]any{observe.AttrTenantID: tenant, "agent": "a"},
		})
	}
	points := collect(t, reader)
	tenants := map[string]int64{}
	for _, dp := range points["goharness.interrupts"] {
		v, _ := dp.Attributes.Value(attributeKey("tenant"))
		tenants[v.AsString()] = dp.Value
	}
	if tenants["t1"] != 1 || tenants["other"] != 2 || len(tenants) != 2 {
		t.Fatalf("tenant labels = %v", tenants)
	}
}

func TestMetricsObservables(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	dropped := int64(7)
	_, err := otel.Metrics(provider.Meter("test"), otel.MetricsConfig{Observables: []otel.ObservableSource{
		{Name: "observer.dropped", Labels: map[string]string{"observer": "events"}, Value: func() int64 { return dropped }},
		{Name: "redis.logcache.size", Unit: "{sessions}", Value: func() int64 { return 42 }},
	}})
	if err != nil {
		t.Fatal(err)
	}
	points := collect(t, reader)
	if total(points["goharness.observer.dropped"]) != 7 {
		t.Fatalf("observer.dropped = %v", points["goharness.observer.dropped"])
	}
	if total(points["goharness.redis.logcache.size"]) != 42 {
		t.Fatalf("logcache.size = %v", points["goharness.redis.logcache.size"])
	}
}

func total(points []metricdata.DataPoint[int64]) int64 {
	var sum int64
	for _, dp := range points {
		sum += dp.Value
	}
	return sum
}
