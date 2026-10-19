package app

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/llm"
)

// ModelConfigInput 是本地工作区运行时切换模型所需的配置。
// APIKey 只在进程内用于创建模型，不会写入项目或会话数据。
type ModelConfigInput struct {
	Mode     string `json:"mode"`
	Endpoint string `json:"endpoint"`
	Model    string `json:"model"`
	APIKey   string `json:"api_key"`
}

// ModelSettings 是可安全返回给前端的模型状态，不包含密钥。
type ModelSettings struct {
	Mode       string `json:"mode"`
	Endpoint   string `json:"endpoint,omitempty"`
	Model      string `json:"model,omitempty"`
	Configured bool   `json:"configured"`
}

func normalizeModelMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return "fixture"
	}
	return mode
}

func buildChatModel(input ModelConfigInput) (llm.ChatModel, ModelSettings, error) {
	mode := normalizeModelMode(input.Mode)
	switch mode {
	case "fixture":
		return nil, ModelSettings{Mode: "fixture", Configured: true}, nil
	case "remote":
		endpoint := strings.TrimSpace(input.Endpoint)
		modelName := strings.TrimSpace(input.Model)
		apiKey := strings.TrimSpace(input.APIKey)
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return nil, ModelSettings{}, fmt.Errorf("%w：远程模型端点无效", domain.ErrInvalid)
		}
		if modelName == "" {
			return nil, ModelSettings{}, fmt.Errorf("%w：远程模型名称不能为空", domain.ErrInvalid)
		}
		if apiKey == "" {
			return nil, ModelSettings{}, fmt.Errorf("%w：远程模型 API Key 不能为空", domain.ErrInvalid)
		}
		model, err := llm.New(llm.Config{ModelType: "openai-compatible", Model: modelName, ModelServer: endpoint, APIKey: apiKey})
		if err != nil {
			return nil, ModelSettings{}, fmt.Errorf("%w：初始化远程模型失败: %v", domain.ErrInvalid, err)
		}
		return model, ModelSettings{Mode: mode, Endpoint: endpoint, Model: modelName, Configured: true}, nil
	default:
		return nil, ModelSettings{}, fmt.Errorf("%w：模型模式必须为 fixture 或 remote", domain.ErrInvalid)
	}
}
