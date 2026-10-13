// Package media 在发送前解析消息里的媒体块：本地或远端 URI 按策略取回
// 内容换成内联 data（JSON base64），结果只用于本次请求、不写回消息与
// 检查点。默认不放行任何抓取：http(s) 必须命中主机白名单，file:// 必须
// 落在声明的根目录内，私网地址在拨号层拦截（与 extensions/mcp 共用
// netguard 检查）。
package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goharness/goharness/extensions/internal/netguard"
	"github.com/goharness/goharness/pkg/httpclient"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// ErrMediaTooLarge 表示媒体内容超过上限：远端/本地媒体超过
// ResolverConfig.MaxBytes，或检查点内联媒体超过 NewStoreGuard 的阈值。
var ErrMediaTooLarge = errors.New("media content exceeds the size limit")

// ResolverConfig 声明媒体来源的允许范围（具体方案 9.5）。
type ResolverConfig struct {
	// AllowedSchemes 是可解析的 URI 协议（如 "https"、"file"）；空表示
	// 只允许已内联的媒体与 data: URI，不解析任何外部来源。
	AllowedSchemes []string
	// AllowedHosts 是 http(s) 主机白名单：精确匹配或 *.后缀。
	// 允许 http/https 协议时必须配置——抓取永不放行未列出的主机。
	AllowedHosts []string
	// FileRoots 是 file:// URI 允许读取的根目录；空拒绝所有文件读取。
	// 解析后的路径必须位于某一根目录内，防止 .. 路径穿越。
	FileRoots []string
	// MaxBytes 是单个媒体的字节上限；0 拒绝一切外部内容。
	MaxBytes int64
	// AllowedMIME 是允许的 MIME 类型；空不限制（仍校验声明与嗅探一致）。
	AllowedMIME []string
	// BlockPrivateNetworks 拨号时对解析后的 IP 检查，拒绝回环、私网、
	// 链路本地与未指定地址；此时使用独立 transport，不经共享连接池。
	BlockPrivateNetworks bool
	// Pool 是共享连接池；BlockPrivateNetworks 时忽略（安全优先）。
	Pool *httpclient.Pool
	// HTTPTimeout 是单次远端获取的整请求超时；0 用 30 秒。
	HTTPTimeout time.Duration
}

type Resolver struct {
	cfg    ResolverConfig
	client *http.Client
	roots  []string
}

// NewResolver 校验配置并创建解析器。
func NewResolver(cfg ResolverConfig) (*Resolver, error) {
	if cfg.MaxBytes < 0 {
		return nil, errors.New("media resolver: MaxBytes must be >= 0")
	}
	if cfg.HTTPTimeout < 0 {
		return nil, errors.New("media resolver: HTTPTimeout must be >= 0")
	}
	schemes := map[string]bool{}
	for _, s := range cfg.AllowedSchemes {
		schemes[strings.ToLower(strings.TrimSpace(s))] = true
	}
	if (schemes["http"] || schemes["https"]) && len(cfg.AllowedHosts) == 0 {
		return nil, errors.New("media resolver: http(s) scheme requires AllowedHosts")
	}
	r := &Resolver{cfg: cfg}
	for _, root := range cfg.FileRoots {
		abs, err := filepath.Abs(root)
		if err != nil {
			return nil, fmt.Errorf("media resolver: file root %q: %w", root, err)
		}
		r.roots = append(r.roots, abs+string(filepath.Separator))
	}
	timeout := cfg.HTTPTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	switch {
	case cfg.BlockPrivateNetworks:
		r.client = &http.Client{Transport: netguard.SecureTransport("media resolver"), Timeout: timeout}
	case cfg.Pool != nil:
		r.client = cfg.Pool.Client(timeout)
	default:
		r.client = &http.Client{Timeout: timeout}
	}
	return r, nil
}

// Resolve 解析单条消息里的媒体块：URI 指向允许来源时取回内容换成内联
// data 并清掉 URI；已内联（Data 非空）与 data: URI 的块原样保留。
// 返回新切片，不改入参。
func (r *Resolver) Resolve(ctx context.Context, messages []schema.Message) ([]schema.Message, error) {
	out := schema.CloneMessages(messages)
	for mi := range out {
		blocks := out[mi].ContentBlocks
		for bi := range blocks {
			media := mediaOf(blocks[bi])
			if media == nil || len(media.Data) > 0 {
				continue
			}
			uri := strings.TrimSpace(media.URI)
			if uri == "" || strings.HasPrefix(uri, "data:") {
				continue
			}
			if err := r.resolveInto(ctx, media, uri); err != nil {
				return nil, fmt.Errorf("messages[%d].content_blocks[%d]: %w", mi, bi, err)
			}
		}
	}
	return out, nil
}

func mediaOf(block schema.ContentBlock) *schema.MediaContent {
	switch {
	case block.Image != nil:
		return block.Image
	case block.File != nil:
		return block.File
	case block.Audio != nil:
		return block.Audio
	case block.Video != nil:
		return block.Video
	}
	return nil
}

// resolveInto 抓取并校验后把媒体改写为内联 data；观测只记大小与 MIME。
func (r *Resolver) resolveInto(ctx context.Context, media *schema.MediaContent, uri string) error {
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme == "" {
		return fmt.Errorf("media URI %q is not an absolute URI", uri)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if !schemeAllowed(scheme, r.cfg.AllowedSchemes) {
		return fmt.Errorf("media URI scheme %q is not allowed", scheme)
	}
	var body []byte
	switch scheme {
	case "http", "https":
		body, err = r.fetchHTTP(ctx, parsed)
	case "file":
		body, err = r.readFile(parsed)
	default:
		return fmt.Errorf("media URI scheme %q has no resolver", scheme)
	}
	if err != nil {
		return err
	}
	mime, err := checkMIME(media.MIMEType, body, r.cfg.AllowedMIME)
	if err != nil {
		return err
	}
	data, err := json.Marshal(base64.StdEncoding.EncodeToString(body))
	if err != nil {
		return err
	}
	media.Data = data
	media.MIMEType = mime
	media.URI = ""
	return nil
}

func schemeAllowed(scheme string, allowed []string) bool {
	for _, s := range allowed {
		if strings.EqualFold(strings.TrimSpace(s), scheme) {
			return true
		}
	}
	return false
}

func (r *Resolver) fetchHTTP(ctx context.Context, parsed *url.URL) ([]byte, error) {
	host := strings.ToLower(parsed.Hostname())
	if !netguard.HostAllowed(host, r.cfg.AllowedHosts) {
		return nil, fmt.Errorf("media host %q is not in AllowedHosts", host)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch media: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch media: status %d", resp.StatusCode)
	}
	return readLimited(resp.Body, r.cfg.MaxBytes)
}

func (r *Resolver) readFile(parsed *url.URL) ([]byte, error) {
	if len(r.roots) == 0 {
		return nil, errors.New("media resolver: file scheme requires FileRoots")
	}
	// file:///abs/path → /abs/path；Host 非空时是 UNC 语义，拒绝。
	if parsed.Host != "" {
		return nil, fmt.Errorf("media file URI %q has a host component", parsed.String())
	}
	path, err := url.PathUnescape(parsed.Path)
	if err != nil {
		return nil, err
	}
	// Windows 的 file:///C:/... 解码后带前导斜杠，剥掉再交给 filepath。
	if len(path) >= 3 && path[0] == '/' && path[1] != '/' && path[2] == ':' {
		path = path[1:]
	}
	abs, err := filepath.Abs(filepath.FromSlash(path))
	if err != nil {
		return nil, err
	}
	inside := false
	for _, root := range r.roots {
		if strings.HasPrefix(abs+string(filepath.Separator), root) {
			inside = true
			break
		}
	}
	if !inside {
		return nil, fmt.Errorf("media file %q is outside FileRoots", abs)
	}
	f, err := os.Open(abs)
	if err != nil {
		return nil, fmt.Errorf("read media file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("media file %q is not a regular file", abs)
	}
	return readLimited(f, r.cfg.MaxBytes)
}

// readLimited 读至多 limit+1 字节；超限返回 ErrMediaTooLarge。
func readLimited(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%w: %d bytes > %d", ErrMediaTooLarge, len(body), limit)
	}
	return body, nil
}

// checkMIME 校验声明 MIME 与内容嗅探一致；声明为空时取嗅探结果。
// AllowedMIME 非空时声明值必须在列表内。
func checkMIME(declared string, body []byte, allowed []string) (string, error) {
	declared = strings.TrimSpace(declared)
	if len(allowed) > 0 && declared != "" {
		ok := false
		for _, item := range allowed {
			if strings.EqualFold(strings.TrimSpace(item), declared) {
				ok = true
				break
			}
		}
		if !ok {
			return "", fmt.Errorf("media MIME %q is not in AllowedMIME", declared)
		}
	}
	sniffed := http.DetectContentType(body)
	if i := strings.IndexByte(sniffed, ';'); i >= 0 {
		sniffed = strings.TrimSpace(sniffed[:i])
	}
	if declared == "" {
		return sniffed, nil
	}
	base, _, _ := strings.Cut(declared, ";")
	if !strings.EqualFold(strings.TrimSpace(base), sniffed) {
		return "", fmt.Errorf("media declared MIME %q does not match sniffed %q", declared, sniffed)
	}
	return declared, nil
}

// WrapModel 返回在每次请求前解析媒体块的模型包装：解析结果只进本次
// 请求的消息，不写回调用方的消息切片。
func (r *Resolver) WrapModel(m llm.ChatModel) llm.ChatModel {
	return resolvedModel{resolver: r, inner: m}
}

type resolvedModel struct {
	resolver *Resolver
	inner    llm.ChatModel
}

func (m resolvedModel) Capabilities() llm.Capabilities { return m.inner.Capabilities() }
func (m resolvedModel) ModelInfo() llm.ModelInfo       { return m.inner.ModelInfo() }

func (m resolvedModel) resolved(ctx context.Context, req llm.Request) (llm.Request, error) {
	if !hasURIMedia(req.Messages) {
		return req.Clone(), nil
	}
	messages, err := m.resolver.Resolve(ctx, req.Messages)
	if err != nil {
		return llm.Request{}, err
	}
	resolved := req.Clone()
	resolved.Messages = messages
	return resolved, nil
}

// hasURIMedia 判断是否存在待解析的外部 URI 媒体块；没有时不进解析路径。
func hasURIMedia(messages []schema.Message) bool {
	for _, m := range messages {
		for _, block := range m.ContentBlocks {
			media := mediaOf(block)
			if media == nil || len(media.Data) > 0 {
				continue
			}
			uri := strings.TrimSpace(media.URI)
			if uri != "" && !strings.HasPrefix(uri, "data:") {
				return true
			}
		}
	}
	return false
}

func (m resolvedModel) Invoke(ctx context.Context, req llm.Request, opts ...runnable.Option) (llm.Response, error) {
	resolved, err := m.resolved(ctx, req)
	if err != nil {
		return llm.Response{}, err
	}
	return m.inner.Invoke(ctx, resolved, opts...)
}

func (m resolvedModel) Stream(ctx context.Context, req llm.Request, opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	resolved, err := m.resolved(ctx, req)
	if err != nil {
		return nil, err
	}
	return m.inner.Stream(ctx, resolved, opts...)
}

func (m resolvedModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], opts ...runnable.Option) (llm.Response, error) {
	return m.inner.Collect(ctx, input, opts...)
}

func (m resolvedModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], opts ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return m.inner.Transform(ctx, input, opts...)
}

var _ llm.ChatModel = resolvedModel{}
