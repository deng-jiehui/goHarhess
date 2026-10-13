// Package eval 提供检索质量评测：固定的 Case 集跑同一个 Searcher，
// 产出 Recall@K / MRR / NDCG@K 与延迟分位点；配合包内固定数据集与
// 确定性假嵌入器可离线运行，作为检索算法的回归门槛。
package eval

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

// Searcher 是被评测的检索面（HybridSearcher 或同签名实现）。
type Searcher = runnable.Runnable[retrieval.HybridQuery, []retrieval.SearchHit]

// Case 是一条评测样本：查询与相关文档 ID 集合。
type Case struct {
	Query    string
	Relevant []string
}

// Report 是评测结果汇总。
type Report struct {
	RecallAtK  map[int]float64 `json:"recall_at_k"`
	NDCGAtK    map[int]float64 `json:"ndcg_at_k"`
	MRR        float64         `json:"mrr"`
	LatencyP50 time.Duration   `json:"latency_p50"`
	LatencyP95 time.Duration   `json:"latency_p95"`
	Cases      int             `json:"cases"`
}

// Run 逐条跑评测：每个 Case 查询一次，TopK 取 ks 上限。
func Run(ctx context.Context, s Searcher, cases []Case, ks []int) (Report, error) {
	if s == nil {
		return Report{}, errors.New("eval requires a searcher")
	}
	if len(cases) == 0 {
		return Report{}, errors.New("eval requires at least one case")
	}
	if len(ks) == 0 {
		ks = []int{5}
	}
	topK := 0
	for _, k := range ks {
		if k > topK {
			topK = k
		}
	}
	report := Report{RecallAtK: map[int]float64{}, NDCGAtK: map[int]float64{}, Cases: len(cases)}
	var latencies []time.Duration
	var rrSum float64
	recallSum := map[int]float64{}
	ndcgSum := map[int]float64{}
	for _, c := range cases {
		start := time.Now()
		hits, err := s.Invoke(ctx, retrieval.HybridQuery{Text: c.Query, TopK: topK})
		if err != nil {
			return report, err
		}
		latencies = append(latencies, time.Since(start))
		relevant := map[string]bool{}
		for _, id := range c.Relevant {
			relevant[id] = true
		}
		// MRR：第一个相关命中的倒数排名。
		for rank, hit := range hits {
			if relevant[hit.ID] {
				rrSum += 1.0 / float64(rank+1)
				break
			}
		}
		for _, k := range ks {
			recallSum[k] += recallAtK(hits, relevant, k)
			ndcgSum[k] += ndcgAtK(hits, relevant, k)
		}
	}
	report.MRR = rrSum / float64(len(cases))
	for _, k := range ks {
		report.RecallAtK[k] = recallSum[k] / float64(len(cases))
		report.NDCGAtK[k] = ndcgSum[k] / float64(len(cases))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	report.LatencyP50 = latencies[len(latencies)*50/100]
	report.LatencyP95 = latencies[len(latencies)*95/100]
	return report, nil
}

func recallAtK(hits []retrieval.SearchHit, relevant map[string]bool, k int) float64 {
	if len(relevant) == 0 {
		return 0
	}
	found := 0
	for i, hit := range hits {
		if i >= k {
			break
		}
		if relevant[hit.ID] {
			found++
		}
	}
	return float64(found) / float64(len(relevant))
}

// ndcgAtK：相关=1 的二元增益 DCG/IDCG。
func ndcgAtK(hits []retrieval.SearchHit, relevant map[string]bool, k int) float64 {
	dcg := 0.0
	for i, hit := range hits {
		if i >= k {
			break
		}
		if relevant[hit.ID] {
			dcg += 1.0 / math.Log2(float64(i)+2)
		}
	}
	ideal := 0.0
	for i := 0; i < min(len(relevant), k); i++ {
		ideal += 1.0 / math.Log2(float64(i)+2)
	}
	if ideal == 0 {
		return 0
	}
	return dcg / ideal
}
