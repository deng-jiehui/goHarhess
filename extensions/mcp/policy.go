package mcp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode"

	sdkmcp "github.com/mark3labs/mcp-go/mcp"
)

// Policy 收紧对远端 MCP 服务的信任（具体方案 9.1）：远端只读注解默认不被
// 信任，工具描述经清理与截断，端点、命令与环境变量可设白名单。
// 零值即安全默认：只读注解被忽略、描述净化、其余不限制——现有接入不受影响。
// 端点或命令来自用户输入时必须 Strict 并配齐对应白名单。
type Policy struct {
	// ReadOnly 决定远端工具是否允许按只读处理（自动重试）。默认忽略远端
	// 注解、一律非只读；应用想让某些工具可重试时按名给出判断。
	ReadOnly func(name string, remote *sdkmcp.ToolAnnotation) bool
	// AllowedTools 非空时只暴露这些远端工具名（不带前缀的原始名）。
	AllowedTools []string
	// DeniedTools 优先于 AllowedTools。
	DeniedTools []string
	// MaxDescription 是描述的最大字节数，0 用默认 1024；截断在字符边界。
	MaxDescription int
	// NamePrefix 给暴露的工具名加前缀，避免与本地工具重名；远端调用仍用原名。
	NamePrefix string
	// AllowedHosts 是 HTTP/SSE 端点的主机白名单：精确匹配或 *.后缀；非空才校验。
	AllowedHosts []string
	// BlockPrivateNetworks 拨号时对解析后的 IP 检查，拒绝回环、私网、
	// 链路本地与未指定地址；该连接使用独立 transport，不经共享连接池。
	BlockPrivateNetworks bool
	// AllowedCommands 是 stdio 可执行文件白名单（按原样字符串比较，建议绝对路径）。
	AllowedCommands []string
	// AllowedEnvKeys 非空时 stdio 只透传这些环境变量名。
	AllowedEnvKeys []string
	// Strict 为真时，所选传输需要的白名单缺失则 NewClient 直接报错：
	// stdio 需要 AllowedCommands；HTTP/SSE 需要 AllowedHosts。
	Strict bool
}

const defaultMaxDescription = 1024

// validate 在 NewClient 时按传输类型校验白名单并返回归一化副本。
func (p Policy) validate(config Config) (Policy, error) {
	out := p
	if out.MaxDescription == 0 {
		out.MaxDescription = defaultMaxDescription
	}
	if out.MaxDescription < 0 {
		return out, fmt.Errorf("mcp policy: MaxDescription must be >= 0")
	}
	switch effectiveTransport(config) {
	case TransportStdio:
		if out.Strict && len(out.AllowedCommands) == 0 {
			return out, fmt.Errorf("mcp policy: strict stdio requires AllowedCommands")
		}
		if len(out.AllowedCommands) > 0 && !contains(out.AllowedCommands, config.Command) {
			return out, fmt.Errorf("mcp policy: stdio command %q is not in AllowedCommands", config.Command)
		}
	case TransportStreamableHTTP, TransportSSE:
		if out.Strict && len(out.AllowedHosts) == 0 {
			return out, fmt.Errorf("mcp policy: strict %s requires AllowedHosts", effectiveTransport(config))
		}
		if len(out.AllowedHosts) > 0 {
			host, err := endpointHost(config.Endpoint)
			if err != nil {
				return out, err
			}
			if !hostAllowed(host, out.AllowedHosts) {
				return out, fmt.Errorf("mcp policy: endpoint host %q is not in AllowedHosts", host)
			}
		}
		if out.BlockPrivateNetworks && config.HTTPClient != nil {
			return out, fmt.Errorf("mcp policy: BlockPrivateNetworks cannot enforce a caller-supplied HTTPClient")
		}
	}
	return out, nil
}

func effectiveTransport(config Config) Transport {
	if config.Transport != "" {
		return config.Transport
	}
	if config.Command != "" {
		return TransportStdio
	}
	if config.Endpoint != "" {
		return TransportStreamableHTTP
	}
	return ""
}

func endpointHost(endpoint string) (string, error) {
	rest := endpoint
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	host, _, err := net.SplitHostPort(rest)
	if err == nil {
		return strings.ToLower(host), nil
	}
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	// 无端口形式
	host = strings.ToLower(rest)
	if host == "" {
		return "", fmt.Errorf("mcp policy: cannot determine endpoint host %q", endpoint)
	}
	return host, nil
}

func hostAllowed(host string, allowed []string) bool {
	for _, entry := range allowed {
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == "" {
			continue
		}
		if strings.HasPrefix(entry, "*.") {
			if host == entry[2:] || strings.HasSuffix(host, entry[1:]) {
				return true
			}
			continue
		}
		if host == entry {
			return true
		}
	}
	return false
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// filterEnv 按 AllowedEnvKeys 过滤 KEY=VALUE 环境变量；列表为空时全部透传。
func (p Policy) filterEnv(env []string) []string {
	if len(p.AllowedEnvKeys) == 0 {
		return env
	}
	kept := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if contains(p.AllowedEnvKeys, key) {
			kept = append(kept, entry)
		}
	}
	return kept
}

// expose 判定远端工具是否对外可见（AllowedTools/DeniedTools）。
func (p Policy) expose(name string) bool {
	if contains(p.DeniedTools, name) {
		return false
	}
	if len(p.AllowedTools) > 0 && !contains(p.AllowedTools, name) {
		return false
	}
	return true
}

// readOnly 应用只读策略：未配回调一律假（远端注解被忽略）。
func (p Policy) readOnly(name string, remote sdkmcp.ToolAnnotation) bool {
	return p.ReadOnly != nil && p.ReadOnly(name, &remote)
}

// sanitizeDescription 去控制字符并按字节截断到 MaxDescription（字符边界）。
func (p Policy) sanitizeDescription(description string) string {
	var b strings.Builder
	b.Grow(len(description))
	for _, r := range description {
		if unicode.IsControl(r) {
			continue
		}
		b.WriteRune(r)
	}
	clean := b.String()
	max := p.MaxDescription
	if max == 0 {
		max = defaultMaxDescription
	}
	if len(clean) <= max {
		return clean
	}
	return clean[:max]
}

// secureDialContext 返回拨号前检查解析地址的 DialContext：全部解析结果
// 中任一是回环/私网/链路本地/未指定地址即拒绝；通过后拨已检查的 IP，
// 避免 DNS 重绑定在检查与连接之间换地址。
func secureDialContext() func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("mcp policy: resolve %s: %w", host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("mcp policy: no address for %s", host)
		}
		for _, ip := range ips {
			if blockedIP(ip) {
				return nil, fmt.Errorf("mcp policy: endpoint %s resolves to blocked address %s", host, ip)
			}
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}
}

func blockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

// secureTransport 为 BlockPrivateNetworks 构造独立 transport：基于默认
// transport 克隆后替换 DialContext，不与共享 httpclient.Pool 混用。
func secureTransport() *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		base = &http.Transport{}
	}
	transport := base.Clone()
	transport.DialContext = secureDialContext()
	return transport
}
