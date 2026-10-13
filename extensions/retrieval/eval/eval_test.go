package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

// baseline 是固定数据集上已保存的基线：指标低于它即失败（回归）。
// 变更检索算法导致指标改善时，应先确认改善合理，再更新此基线。
var baseline = Report{
	RecallAtK: map[int]float64{1: 1.0, 3: 1.0, 5: 1.0},
	NDCGAtK:   map[int]float64{1: 1.0, 3: 1.0, 5: 1.0},
	MRR:       1.0,
}

// TestFixedDatasetRegression 固定数据集回归门槛：指标不得低于基线。
func TestFixedDatasetRegression(t *testing.T) {
	searcher, err := NewFixedSearcher(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	report, err := Run(context.Background(), searcher, fixedCases, []int{1, 3, 5})
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range baseline.RecallAtK {
		if got := report.RecallAtK[k]; got < want {
			t.Errorf("Recall@%d %.3f < baseline %.3f", k, got, want)
		}
	}
	for k, want := range baseline.NDCGAtK {
		if got := report.NDCGAtK[k]; got < want {
			t.Errorf("NDCG@%d %.3f < baseline %.3f", k, got, want)
		}
	}
	if report.MRR < baseline.MRR {
		t.Errorf("MRR %.3f < baseline %.3f", report.MRR, baseline.MRR)
	}
	if report.LatencyP95 < report.LatencyP50 {
		t.Errorf("latency percentiles invalid: p50=%v p95=%v", report.LatencyP50, report.LatencyP95)
	}
}

// TestReportDeterminism 同一数据集重复评测指标一致（确定性）。
func TestReportDeterminism(t *testing.T) {
	searcher, _ := NewFixedSearcher(context.Background())
	a, err := Run(context.Background(), searcher, fixedCases, []int{1, 3, 5})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Run(context.Background(), searcher, fixedCases, []int{1, 3, 5})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(a.RecallAtK) != fmt.Sprint(b.RecallAtK) || a.MRR != b.MRR {
		t.Errorf("non-deterministic metrics: %+v vs %+v", a, b)
	}
}

// TestMetricsEdgeCases 空相关集、重复命中、缺失 ID 的指标正确性。
func TestMetricsEdgeCases(t *testing.T) {
	hits := []retrieval.SearchHit{{ID: "a"}, {ID: "a"}, {ID: "b"}}
	rel := map[string]bool{"a": true, "missing": true}
	if got := recallAtK(hits, rel, 1); got != 0.5 {
		t.Errorf("recall@1=%v want 0.5（去重命中仍算一个相关）", got)
	}
	if got := ndcgAtK(hits, map[string]bool{}, 3); got != 0 {
		t.Errorf("ndcg 空相关集=%v want 0", got)
	}
	// 无命中时 MRR 为 0。
	s := runnable.Func[retrieval.HybridQuery, []retrieval.SearchHit](func(context.Context, retrieval.HybridQuery, ...runnable.Option) ([]retrieval.SearchHit, error) {
		return []retrieval.SearchHit{{ID: "x"}}, nil
	})
	report, err := Run(context.Background(), s, []Case{{Query: "q", Relevant: []string{"y"}}}, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if report.MRR != 0 || report.RecallAtK[1] != 0 {
		t.Errorf("want zero metrics, got %+v", report)
	}
}

// TestRunRejectsEmpty 空 case / nil searcher 的校验。
func TestRunRejectsEmpty(t *testing.T) {
	if _, err := Run(context.Background(), nil, fixedCases, nil); err == nil {
		t.Error("nil searcher should fail")
	}
	s, _ := NewFixedSearcher(context.Background())
	if _, err := Run(context.Background(), s, nil, nil); err == nil {
		t.Error("empty cases should fail")
	}
}

// TestLatencyPercentiles 多 case 下的 p50/p95 计算。
func TestLatencyPercentiles(t *testing.T) {
	var delays []time.Duration
	for i := 0; i < 20; i++ {
		delays = append(delays, time.Duration(i)*time.Millisecond)
	}
	idx := 0
	s := runnable.Func[retrieval.HybridQuery, []retrieval.SearchHit](func(context.Context, retrieval.HybridQuery, ...runnable.Option) ([]retrieval.SearchHit, error) {
		time.Sleep(delays[idx%len(delays)])
		idx++
		return []retrieval.SearchHit{{ID: "a"}}, nil
	})
	cases := make([]Case, 20)
	for i := range cases {
		cases[i] = Case{Query: "q", Relevant: []string{"a"}}
	}
	report, err := Run(context.Background(), s, cases, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if report.LatencyP50 <= 0 || report.LatencyP95 < report.LatencyP50 {
		t.Errorf("invalid percentiles: p50=%v p95=%v", report.LatencyP50, report.LatencyP95)
	}
}

// TestSaveBaseline 辅助：打印当前报告供更新基线（非 CI 断言）。
func TestSaveBaseline(t *testing.T) {
	if os.Getenv("EVAL_DUMP_BASELINE") == "" {
		t.Skip("set EVAL_DUMP_BASELINE to dump")
	}
	searcher, _ := NewFixedSearcher(context.Background())
	report, _ := Run(context.Background(), searcher, fixedCases, []int{1, 3, 5})
	data, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(filepath.Join(t.TempDir(), "baseline.json"), data, 0o644)
	t.Log(string(data))
}
