package otel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/state"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricsConfig 配置指标观察者。
//
// 标签基数规则（具体方案 6.2）：用户 ID、会话 ID、运行 ID 永远不进指标
// 标签；租户标签默认关闭，开启后由 TenantLabelMax 封顶——前 N 个出现的
// 租户保留自己的名字，其余合并为 "other"。
type MetricsConfig struct {
	// TenantLabelMax >0 时在 run.active / run.duration / interrupts 上附加
	// tenant 标签并按该上限合并；0 关闭租户标签。
	TenantLabelMax int
	// Observables 注册观察型指标（如 observer.dropped、redis.logcache.*），
	// 采集时回调读值。
	Observables []ObservableSource
}

// ObservableSource 描述一个观察型 int64 指标。
type ObservableSource struct {
	Name        string // 指标名，自动加 goharness. 前缀
	Description string
	Unit        string
	Labels      map[string]string // 固定标签集（如 {"observer": "events"}）
	Value       func() int64      // 采集回调，必须并发安全
}

// metricsObserver 把 observe 事件映射为 OTel 指标；指标全部由现有事件的
// Duration、Usage、Err 与 Attrs 推导，不需要新的发射点。
type metricsObserver struct {
	cfg          MetricsConfig
	runActive    metric.Int64UpDownCounter
	runDuration  metric.Float64Histogram
	nodeDuration metric.Float64Histogram
	llmDuration  metric.Float64Histogram
	llmTokens    metric.Int64Counter
	toolDuration metric.Float64Histogram
	saveDuration metric.Float64Histogram
	conflicts    metric.Int64Counter
	interrupts   metric.Int64Counter

	mu      sync.Mutex
	tenants map[string]struct{} // 已分配独立标签的租户，容量 TenantLabelMax
	order   []string            // 出现顺序，超出上限的租户合并为 other
}

// Metrics 创建指标观察者。meter 为 nil 时返回错误——与 Observer 不同，
// 指标需要显式注入 Meter，便于调用方控制 MeterProvider 生命周期。
func Metrics(meter metric.Meter, cfg MetricsConfig) (observe.Observer, error) {
	if meter == nil {
		return nil, fmt.Errorf("otel metrics requires a meter")
	}
	if cfg.TenantLabelMax < 0 {
		return nil, fmt.Errorf("otel metrics: TenantLabelMax must be >= 0")
	}
	m := &metricsObserver{cfg: cfg}
	var err error
	newCounter := func(dst *metric.Int64Counter, name, desc string) {
		if err != nil {
			return
		}
		*dst, err = meter.Int64Counter("goharness."+name,
			metric.WithDescription(desc), metric.WithUnit("{count}"))
	}
	newUpDown := func(dst *metric.Int64UpDownCounter, name, desc string) {
		if err != nil {
			return
		}
		*dst, err = meter.Int64UpDownCounter("goharness."+name,
			metric.WithDescription(desc), metric.WithUnit("{count}"))
	}
	newHist := func(dst *metric.Float64Histogram, name, desc string) {
		if err != nil {
			return
		}
		*dst, err = meter.Float64Histogram("goharness."+name,
			metric.WithDescription(desc), metric.WithUnit("s"))
	}
	newUpDown(&m.runActive, "run.active", "in-flight runs")
	newHist(&m.runDuration, "run.duration", "run duration")
	newHist(&m.nodeDuration, "node.duration", "node duration")
	newHist(&m.llmDuration, "llm.duration", "model call duration")
	newCounter(&m.llmTokens, "llm.tokens", "model token usage")
	newHist(&m.toolDuration, "tool.duration", "tool execution duration")
	newHist(&m.saveDuration, "checkpoint.save.duration", "checkpoint save duration")
	newCounter(&m.conflicts, "checkpoint.conflicts", "checkpoint version conflicts")
	newCounter(&m.interrupts, "interrupts", "run interrupts")
	if err != nil {
		return nil, err
	}
	for _, source := range cfg.Observables {
		if source.Value == nil || source.Name == "" {
			return nil, fmt.Errorf("otel metrics: observable %q requires a name and value func", source.Name)
		}
		attrs := observableAttrs(source.Labels)
		fn := source.Value
		_, err = meter.Int64ObservableGauge("goharness."+source.Name,
			metric.WithDescription(source.Description),
			metric.WithUnit(source.Unit),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(fn(), metric.WithAttributes(attrs...))
				return nil
			}))
		if err != nil {
			return nil, fmt.Errorf("otel metrics: register %q: %w", source.Name, err)
		}
	}
	return m, nil
}

func (m *metricsObserver) OnEvent(_ context.Context, event observe.Event) {
	switch event.Kind {
	case observe.KindRun:
		m.onRun(event)
	case observe.KindNode:
		m.onFinish(event, m.nodeDuration, labelPair("node", event.Name))
	case observe.KindLLM:
		m.onLLM(event)
	case observe.KindTool:
		m.onFinish(event, m.toolDuration, labelPair("tool", event.Name))
	case observe.KindCheckpoint:
		m.onCheckpoint(event)
	case observe.KindInterrupt:
		m.interrupts.Add(context.Background(), 1, metric.WithAttributes(m.runLabels(event)...))
	}
}

func (m *metricsObserver) onRun(event observe.Event) {
	labels := m.runLabels(event)
	switch event.Phase {
	case observe.PhaseStart:
		m.runActive.Add(context.Background(), 1, metric.WithAttributes(labels...))
	case observe.PhaseEnd, observe.PhaseError:
		m.runActive.Add(context.Background(), -1, metric.WithAttributes(labels...))
		m.runDuration.Record(context.Background(), event.Duration.Seconds(),
			metric.WithAttributes(appendStatus(labels, event.Phase)...))
	}
}

func (m *metricsObserver) onLLM(event observe.Event) {
	m.onFinish(event, m.llmDuration, labelPair("model", event.Name))
	if event.Phase != observe.PhaseEnd || event.Usage == nil {
		return
	}
	usage := event.Usage
	add := func(direction string, tokens int) {
		if tokens == 0 {
			return
		}
		m.llmTokens.Add(context.Background(), int64(tokens),
			metric.WithAttributes(attribute.String("model", event.Name), attribute.String("direction", direction)))
	}
	add("input", usage.InputTokens)
	add("output", usage.OutputTokens)
	add("reasoning", usage.ReasoningTokens)
	add("cache_read", usage.CacheReadTokens)
	add("cache_write", usage.CacheWriteTokens)
}

func (m *metricsObserver) onCheckpoint(event observe.Event) {
	if event.Phase == observe.PhaseError && event.Err != nil && errors.Is(event.Err, state.ErrConflict) {
		m.conflicts.Add(context.Background(), 1, metric.WithAttributes(m.storeLabel(event)))
	}
	// 指标只覆盖 save；load 事件的名称不是 save，直接跳过。
	if event.Name != "save" {
		return
	}
	m.onFinish(event, m.saveDuration, metric.WithAttributes(m.storeLabel(event)))
}

func (m *metricsObserver) onFinish(event observe.Event, hist metric.Float64Histogram, extra ...metric.RecordOption) {
	if event.Phase != observe.PhaseEnd && event.Phase != observe.PhaseError {
		return
	}
	options := append(extra, metric.WithAttributes(attribute.String("status", statusOf(event.Phase))))
	hist.Record(context.Background(), event.Duration.Seconds(), options...)
}

// runLabels 组装 run 级标签：agent 恒在（无名 Agent 用空值，指标端仍一致），
// tenant 仅在开启后按出现顺序保留前 TenantLabelMax 个，其余归并 other。
func (m *metricsObserver) runLabels(event observe.Event) []attribute.KeyValue {
	labels := []attribute.KeyValue{attribute.String("agent", attrString(event.Attrs, "agent"))}
	if m.cfg.TenantLabelMax > 0 {
		labels = append(labels, attribute.String("tenant", m.tenantLabel(attrString(event.Attrs, observe.AttrTenantID))))
	}
	return labels
}

func (m *metricsObserver) tenantLabel(tenantID string) string {
	if tenantID == "" {
		return "unknown"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tenants[tenantID]; ok {
		return tenantID
	}
	if len(m.order) >= m.cfg.TenantLabelMax {
		return "other"
	}
	if m.tenants == nil {
		m.tenants = map[string]struct{}{}
	}
	m.tenants[tenantID] = struct{}{}
	m.order = append(m.order, tenantID)
	return tenantID
}

func (m *metricsObserver) storeLabel(event observe.Event) attribute.KeyValue {
	store := attrString(event.Attrs, "checkpoint.store")
	if store == "" {
		store = "default"
	}
	return attribute.String("store", store)
}

func statusOf(phase observe.Phase) string {
	if phase == observe.PhaseError {
		return "error"
	}
	return "ok"
}

func attrString(attrs map[string]any, key string) string {
	if attrs == nil {
		return ""
	}
	if value, ok := attrs[key].(string); ok {
		return value
	}
	return ""
}

func labelPair(key, value string) metric.RecordOption {
	return metric.WithAttributes(attribute.String(key, value))
}

func appendStatus(labels []attribute.KeyValue, phase observe.Phase) []attribute.KeyValue {
	return append(labels, attribute.String("status", statusOf(phase)))
}

func observableAttrs(labels map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(labels))
	for key, value := range labels {
		attrs = append(attrs, attribute.String(key, value))
	}
	return attrs
}
