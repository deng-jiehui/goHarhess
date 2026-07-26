package memory

import (
	"context"
	"path/filepath"
	"strings"

	"goharness/pkg/schema"
)

type RAGConfig struct {
	MaxRefToken     int
	ParserPageSize  int
	KeygenStrategy  string
	Searchers       []string
	SupportedSuffix map[string]bool
}

type Retriever interface {
	Retrieve(ctx context.Context, query string, files []string, cfg RAGConfig) (string, error)
}

type Memory struct {
	SystemFiles []string
	Config      RAGConfig
	Retriever   Retriever
}

func New(systemFiles []string, cfg RAGConfig, retriever Retriever) *Memory {
	if cfg.SupportedSuffix == nil {
		cfg.SupportedSuffix = defaultSupportedSuffix()
	}
	return &Memory{
		SystemFiles: append([]string(nil), systemFiles...),
		Config:      cfg,
		Retriever:   retriever,
	}
}

func (m *Memory) Knowledge(ctx context.Context, messages []schema.Message, lang string) (string, error) {
	files := m.ragFiles(messages)
	if len(files) == 0 || m.Retriever == nil {
		return "", nil
	}
	query := schema.LastUserText(messages)
	return m.Retriever.Retrieve(ctx, query, files, m.Config)
}

func (m *Memory) ragFiles(messages []schema.Message) []string {
	seen := map[string]struct{}{}
	files := make([]string, 0)
	add := func(path string) {
		if path == "" {
			return
		}
		if !m.isSupported(path) {
			return
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		files = append(files, path)
	}

	for _, file := range m.SystemFiles {
		add(file)
	}
	for _, file := range schema.ExtractFiles(messages, false) {
		add(file)
	}
	return files
}

func (m *Memory) isSupported(path string) bool {
	suffixes := m.Config.SupportedSuffix
	if suffixes == nil {
		suffixes = defaultSupportedSuffix()
	}
	clean := strings.Split(path, "?")[0]
	clean = strings.Split(clean, "#")[0]
	ext := strings.ToLower(filepath.Ext(clean))
	return suffixes[ext]
}

type StaticRetriever string

func (s StaticRetriever) Retrieve(ctx context.Context, query string, files []string, cfg RAGConfig) (string, error) {
	return string(s), nil
}

func defaultSupportedSuffix() map[string]bool {
	return map[string]bool{
		".pdf":  true,
		".docx": true,
		".pptx": true,
		".txt":  true,
		".csv":  true,
		".tsv":  true,
		".xlsx": true,
		".xls":  true,
		".html": true,
		".htm":  true,
	}
}
