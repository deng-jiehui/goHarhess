package mcp

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/goharness/autoresearch/internal/domain"
)

// ParseCandidates 将学术来源返回的 JSON 归一化为项目候选；它只接受
// papers/results 数组，不把任意第三方 JSON 猜测成论文。
func ParseCandidates(provider string, data []byte) ([]domain.Candidate, error) {
	provider = strings.TrimSpace(strings.ToLower(provider))
	if provider == "" {
		return nil, fmt.Errorf("来源 provider 不能为空")
	}
	var envelope struct {
		Papers  []json.RawMessage `json:"papers"`
		Results []json.RawMessage `json:"results"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("解析 %s 论文结果: %w", provider, err)
	}
	items := envelope.Papers
	if len(items) == 0 {
		items = envelope.Results
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("来源 %s 未返回论文列表", provider)
	}
	out := make([]domain.Candidate, 0, len(items))
	for index, raw := range items {
		var item struct {
			ID        string          `json:"id"`
			Title     string          `json:"title"`
			Published string          `json:"published"`
			Year      int             `json:"year"`
			Summary   string          `json:"summary"`
			Abstract  string          `json:"abstract"`
			URL       string          `json:"url"`
			DOI       string          `json:"doi"`
			Authors   json.RawMessage `json:"authors"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, fmt.Errorf("来源 %s 第 %d 条论文格式错误: %w", provider, index+1, err)
		}
		if strings.TrimSpace(item.Title) == "" {
			return nil, fmt.Errorf("来源 %s 第 %d 条论文缺少标题", provider, index+1)
		}
		keyInput := item.ID
		if strings.TrimSpace(item.DOI) != "" {
			keyInput = item.DOI
		}
		key, err := domain.NormalizePaperKey(keyInput)
		if err != nil {
			return nil, fmt.Errorf("来源 %s 第 %d 条论文身份无效: %w", provider, index+1, err)
		}
		authors, err := parseAuthors(item.Authors)
		if err != nil {
			return nil, fmt.Errorf("来源 %s 第 %d 条作者格式错误: %w", provider, index+1, err)
		}
		year := item.Year
		if year == 0 && len(item.Published) >= 4 {
			year, err = strconv.Atoi(item.Published[:4])
			if err != nil {
				return nil, fmt.Errorf("来源 %s 第 %d 条年份无效", provider, index+1)
			}
		}
		candidate := domain.Candidate{SourceKey: key, Title: strings.TrimSpace(item.Title), URL: item.URL, DOI: item.DOI, Authors: authors, Year: year, Abstract: item.Abstract, Provider: provider}
		if candidate.Abstract == "" {
			candidate.Abstract = item.Summary
		}
		if strings.HasPrefix(key, "arxiv:") {
			candidate.ArxivID = strings.TrimPrefix(key, "arxiv:")
		}
		if strings.HasPrefix(key, "doi:") && candidate.DOI == "" {
			candidate.DOI = strings.TrimPrefix(key, "doi:")
		}
		out = append(out, candidate)
	}
	return out, nil
}

func parseAuthors(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []string{}, nil
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err == nil {
		return names, nil
	}
	var values []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	names = make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value.Name) != "" {
			names = append(names, strings.TrimSpace(value.Name))
		}
	}
	return names, nil
}
