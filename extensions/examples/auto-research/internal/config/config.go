package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config 只保存密钥引用，密钥值由启动入口读取。
type Config struct {
	Listen         string `json:"listen"`
	DataDir        string `json:"data_dir"`
	ModelMode      string `json:"model_mode"`
	ModelEndpoint  string `json:"model_endpoint"`
	ModelName      string `json:"model_name"`
	APIKeyEnv      string `json:"api_key_env"`
	AuthTokenEnv   string `json:"auth_token_env"`
	WorkspaceRoot  string `json:"workspace_root"`
	WorkspaceShell string `json:"workspace_shell"`
}

func Load(path string) (Config, error) {
	cfg := Config{Listen: "127.0.0.1:8080", DataDir: "./data", ModelMode: "fixture", WorkspaceRoot: "."}
	f, err := os.Open(path)
	if err != nil {
		return cfg, fmt.Errorf("读取配置: %w", err)
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("配置格式错误: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return cfg, errors.New("配置只能包含一个 JSON 对象")
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DataDir) == "" {
		return errors.New("数据目录不能为空")
	}
	host, port, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return errors.New("监听地址必须包含主机与端口")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return errors.New("监听端口无效")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		if c.AuthTokenEnv == "" || strings.TrimSpace(os.Getenv(c.AuthTokenEnv)) == "" {
			return errors.New("非回环监听必须配置非空认证环境变量")
		}
	}
	if c.AuthTokenEnv != "" && strings.TrimSpace(os.Getenv(c.AuthTokenEnv)) == "" {
		return errors.New("认证环境变量为空")
	}
	switch c.ModelMode {
	case "fixture":
	case "remote":
		endpoint, err := url.Parse(c.ModelEndpoint)
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || strings.TrimSpace(c.ModelName) == "" {
			return errors.New("远程模型必须配置有效 HTTP 端点与模型名称")
		}
	default:
		return errors.New("模型模式必须为 fixture 或 remote")
	}
	return nil
}
