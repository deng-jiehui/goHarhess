package domain

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type ResearchSettings struct {
	YearFrom       int      `json:"year_from"`
	YearTo         int      `json:"year_to"`
	CandidateLimit int      `json:"candidate_limit"`
	AnalyzeLimit   int      `json:"analyze_limit"`
	Providers      []string `json:"providers"`
}

type Project struct {
	ID       string           `json:"id"`
	OwnerID  string           `json:"owner_id"`
	Title    string           `json:"title"`
	Question string           `json:"question"`
	Field    string           `json:"field"`
	Settings ResearchSettings `json:"settings"`
	Archived bool             `json:"archived"`
	Revision int64            `json:"revision"`
}

func DefaultSettings() ResearchSettings {
	return ResearchSettings{Providers: []string{"arxiv"}, CandidateLimit: 30, AnalyzeLimit: 12}
}

func NormalizeTitle(title string) (string, error) {
	title = strings.TrimSpace(title)
	if n := utf8.RuneCountInString(title); n < 1 || n > 120 {
		return "", fmt.Errorf("%w：标题须为 1–120 个字符", ErrInvalid)
	}
	return title, nil
}

func NormalizeSettings(settings ResearchSettings) (ResearchSettings, error) {
	if settings.YearFrom < 0 || settings.YearTo < 0 || settings.YearFrom > 9999 || settings.YearTo > 9999 ||
		(settings.YearFrom == 0) != (settings.YearTo == 0) || settings.YearFrom > settings.YearTo {
		return settings, fmt.Errorf("%w：年份应同时留空，或设置为有效的起止年份", ErrInvalid)
	}
	if settings.CandidateLimit < 1 || settings.CandidateLimit > 100 || settings.AnalyzeLimit < 1 || settings.AnalyzeLimit > 30 || settings.AnalyzeLimit > settings.CandidateLimit {
		return settings, fmt.Errorf("%w：候选数须为 1–100，分析数须为 1–30 且不超过候选数", ErrInvalid)
	}
	if len(settings.Providers) == 0 {
		return settings, fmt.Errorf("%w：至少选择一个来源", ErrInvalid)
	}
	for _, provider := range settings.Providers {
		if provider != "arxiv" {
			return settings, fmt.Errorf("%w：当前只支持 arxiv 来源设置", ErrInvalid)
		}
	}
	settings.Providers = []string{"arxiv"}
	return settings, nil
}

func NormalizeProject(p Project) (Project, error) {
	var err error
	p.Title, err = NormalizeTitle(p.Title)
	if err != nil {
		return p, err
	}
	if utf8.RuneCountInString(p.Question) > 4000 || utf8.RuneCountInString(p.Field) > 120 {
		return p, fmt.Errorf("%w：研究问题或领域过长", ErrInvalid)
	}
	p.Settings, err = NormalizeSettings(p.Settings)
	return p, err
}
