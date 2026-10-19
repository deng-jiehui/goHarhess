package domain_test

import (
	"strings"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

func TestResearchSettingsRejectInvalidBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*domain.ResearchSettings)
	}{
		{"起始年份缺少结束", func(s *domain.ResearchSettings) { s.YearFrom = 2021 }},
		{"年份顺序相反", func(s *domain.ResearchSettings) { s.YearFrom = 2026; s.YearTo = 2021 }},
		{"无效年份", func(s *domain.ResearchSettings) { s.YearFrom = -1; s.YearTo = 2026 }},
		{"候选数越界", func(s *domain.ResearchSettings) { s.CandidateLimit = 101 }},
		{"分析数大于候选", func(s *domain.ResearchSettings) { s.CandidateLimit = 5 }},
		{"分析数越界", func(s *domain.ResearchSettings) { s.AnalyzeLimit = 31 }},
		{"空来源", func(s *domain.ResearchSettings) { s.Providers = nil }},
		{"不支持的来源", func(s *domain.ResearchSettings) { s.Providers = []string{"unknown"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := domain.DefaultSettings()
			tc.change(&s)
			if _, err := domain.NormalizeSettings(s); err == nil {
				t.Fatal("非法范围被接受")
			}
		})
	}
}

func TestTitleLimitCountsUnicodeCharacters(t *testing.T) {
	if _, err := domain.NormalizeTitle(strings.Repeat("研", 120)); err != nil {
		t.Fatal(err)
	}
	if _, err := domain.NormalizeTitle(strings.Repeat("研", 121)); err == nil {
		t.Fatal("超长标题被接受")
	}
	if _, err := domain.NormalizeTitle(" \t "); err == nil {
		t.Fatal("空白标题被接受")
	}
}
