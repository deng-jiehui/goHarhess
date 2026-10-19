package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	mcpext "github.com/goharness/goharness/extensions/mcp"
	"github.com/goharness/goharness/pkg/tool"

	"github.com/goharness/autoresearch/internal/domain"
)

type MCPSourceConfig struct {
	Provider    string
	SearchTool  string
	ResolveTool string
	ReadTool    string
	// 参数构造函数由应用按来源 schema 提供，避免把第三方工具参数写死在 MCP 扩展中。
	SearchArgs  func(domain.PaperQuery) map[string]any
	ResolveArgs func(string) map[string]any
	ReadArgs    func(string, string) map[string]any
}

type MCPSource struct {
	client *mcpext.Client
	config MCPSourceConfig
}

func NewMCPSource(client *mcpext.Client, config MCPSourceConfig) (*MCPSource, error) {
	if client == nil || strings.TrimSpace(config.Provider) == "" || strings.TrimSpace(config.SearchTool) == "" {
		return nil, fmt.Errorf("MCP 来源缺少客户端、provider 或搜索工具")
	}
	return &MCPSource{client: client, config: config}, nil
}

func (s *MCPSource) Search(ctx context.Context, query domain.PaperQuery) ([]domain.Candidate, error) {
	args := map[string]any{"query": query.Text, "limit": query.Limit}
	if s.config.SearchArgs != nil {
		args = s.config.SearchArgs(query)
	}
	data, err := s.call(ctx, s.config.SearchTool, args)
	if err != nil {
		return nil, err
	}
	return ParseCandidates(s.config.Provider, data)
}

func (s *MCPSource) Resolve(ctx context.Context, reference string) (domain.Candidate, error) {
	if strings.TrimSpace(s.config.ResolveTool) == "" {
		return domain.Candidate{}, fmt.Errorf("MCP 来源未配置元数据工具")
	}
	args := map[string]any{"reference": reference}
	if s.config.ResolveArgs != nil {
		args = s.config.ResolveArgs(reference)
	}
	data, err := s.call(ctx, s.config.ResolveTool, args)
	if err != nil {
		return domain.Candidate{}, err
	}
	var item domain.Candidate
	if err = json.Unmarshal(data, &item); err != nil {
		return domain.Candidate{}, fmt.Errorf("解析 %s 元数据: %w", s.config.Provider, err)
	}
	return item, nil
}

func (s *MCPSource) Read(ctx context.Context, reference, section string) (domain.SourceDocument, error) {
	if strings.TrimSpace(s.config.ReadTool) == "" {
		return domain.SourceDocument{}, fmt.Errorf("MCP 来源未配置正文工具")
	}
	args := map[string]any{"reference": reference, "section": section}
	if s.config.ReadArgs != nil {
		args = s.config.ReadArgs(reference, section)
	}
	data, err := s.call(ctx, s.config.ReadTool, args)
	if err != nil {
		return domain.SourceDocument{}, err
	}
	var document domain.SourceDocument
	if err = json.Unmarshal(data, &document); err != nil {
		return domain.SourceDocument{}, fmt.Errorf("解析 %s 文档: %w", s.config.Provider, err)
	}
	return document, nil
}

func (s *MCPSource) call(ctx context.Context, name string, args map[string]any) ([]byte, error) {
	tools, err := s.client.Tools(ctx)
	if err != nil {
		return nil, fmt.Errorf("发现 %s MCP 工具失败: %w", s.config.Provider, err)
	}
	for _, value := range tools {
		if value.Spec().Name != name && !strings.HasSuffix(value.Spec().Name, name) {
			continue
		}
		result, invokeErr := value.Invoke(ctx, tool.CallInput{Args: args})
		if invokeErr != nil {
			return nil, fmt.Errorf("调用 MCP 工具 %q 失败: %w", name, invokeErr)
		}
		return resultJSON(result)
	}
	return nil, fmt.Errorf("MCP 来源缺少工具 %q", name)
}

func resultJSON(result tool.Result) ([]byte, error) {
	blocks, err := result.ToContentBlocks()
	if err != nil {
		return nil, fmt.Errorf("读取 MCP 工具结果失败: %w", err)
	}
	for _, block := range blocks {
		if block.Text != nil && strings.TrimSpace(block.Text.Text) != "" {
			return []byte(block.Text.Text), nil
		}
		if block.Opaque != nil && len(block.Opaque.Raw) > 0 {
			return append([]byte(nil), block.Opaque.Raw...), nil
		}
	}
	return nil, fmt.Errorf("MCP 工具返回空结果")
}

var _ domain.PaperSource = (*MCPSource)(nil)
