package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// 1x1 PNG，DetectContentType 嗅探为 image/png。
var pngBytes = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
	0x0a, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
	0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
}

func imageMessage(uri, mime string) schema.Message {
	return schema.NewMessage(schema.RoleUser, schema.NewImageBlock(uri, mime))
}

func resolver(t *testing.T, cfg ResolverConfig) *Resolver {
	t.Helper()
	r, err := NewResolver(cfg)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	return r
}

// testHost 返回 httptest 服务 URL 的主机名（白名单按主机名匹配，不带端口）。
func testHost(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Hostname()
}

func TestResolver_HTTPResolve(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(pngBytes)
	}))
	defer server.Close()
	host := testHost(t, server.URL)
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"http"},
		AllowedHosts:   []string{host},
		MaxBytes:       1 << 20,
	})
	in := []schema.Message{imageMessage(server.URL+"/img.png", "image/png")}
	out, err := r.Resolve(context.Background(), in)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	media := out[0].ContentBlocks[0].Image
	if media.URI != "" {
		t.Fatalf("expected URI cleared, got %q", media.URI)
	}
	var b64 string
	if err := json.Unmarshal(media.Data, &b64); err != nil {
		t.Fatalf("Data is not a JSON string: %v", err)
	}
	if got, _ := base64.StdEncoding.DecodeString(b64); string(got) != string(pngBytes) {
		t.Fatal("decoded media bytes mismatch")
	}
	// 入参消息不得被改写。
	orig := in[0].ContentBlocks[0].Image
	if orig.URI != server.URL+"/img.png" || len(orig.Data) > 0 {
		t.Fatal("input message was mutated")
	}
}

func TestResolver_SchemeRejected(t *testing.T) {
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"https"},
		AllowedHosts:   []string{"cdn.example.com"},
		MaxBytes:       1 << 20,
	})
	_, err := r.Resolve(context.Background(), []schema.Message{
		imageMessage("http://cdn.example.com/a.png", "image/png"),
	})
	if err == nil || !strings.Contains(err.Error(), `scheme "http"`) {
		t.Fatalf("expected scheme rejection, got %v", err)
	}
}

func TestResolver_HostRejected(t *testing.T) {
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"https"},
		AllowedHosts:   []string{"cdn.example.com"},
		MaxBytes:       1 << 20,
	})
	_, err := r.Resolve(context.Background(), []schema.Message{
		imageMessage("https://evil.example.com/a.png", "image/png"),
	})
	if err == nil || !strings.Contains(err.Error(), "not in AllowedHosts") {
		t.Fatalf("expected host rejection, got %v", err)
	}
}

func TestResolver_PrivateNetworkRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(pngBytes)
	}))
	defer server.Close()
	host := testHost(t, server.URL)
	r := resolver(t, ResolverConfig{
		AllowedSchemes:       []string{"http"},
		AllowedHosts:         []string{host},
		MaxBytes:             1 << 20,
		BlockPrivateNetworks: true,
	})
	_, err := r.Resolve(context.Background(), []schema.Message{
		imageMessage(server.URL+"/img.png", "image/png"),
	})
	if err == nil || !strings.Contains(err.Error(), "fetch media") {
		t.Fatalf("expected private-network dial rejection, got %v", err)
	}
}

func TestResolver_ByteLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(make([]byte, 2048))
	}))
	defer server.Close()
	host := testHost(t, server.URL)
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"http"},
		AllowedHosts:   []string{host},
		MaxBytes:       1024,
	})
	_, err := r.Resolve(context.Background(), []schema.Message{
		imageMessage(server.URL+"/big.bin", ""),
	})
	if !errors.Is(err, ErrMediaTooLarge) {
		t.Fatalf("expected ErrMediaTooLarge, got %v", err)
	}
}

func TestResolver_FileRootsAndTraversal(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "ok.png"), pngBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(root, "..", "secret.png")
	if err := os.WriteFile(secret, pngBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	fileURI := func(path string) string {
		return (&url.URL{Scheme: "file", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
	}
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"file"},
		FileRoots:      []string{root},
		MaxBytes:       1 << 20,
	})
	out, err := r.Resolve(context.Background(), []schema.Message{
		imageMessage(fileURI(filepath.Join(root, "ok.png")), "image/png"),
	})
	if err != nil {
		t.Fatalf("Resolve file: %v", err)
	}
	if len(out[0].ContentBlocks[0].Image.Data) == 0 {
		t.Fatal("expected inline data after file resolution")
	}
	_, err = r.Resolve(context.Background(), []schema.Message{
		imageMessage(fileURI(secret), "image/png"),
	})
	if err == nil || !strings.Contains(err.Error(), "outside FileRoots") {
		t.Fatalf("expected traversal rejection, got %v", err)
	}
}

func TestResolver_MIMEMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(pngBytes)
	}))
	defer server.Close()
	host := testHost(t, server.URL)
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"http"},
		AllowedHosts:   []string{host},
		MaxBytes:       1 << 20,
		AllowedMIME:    []string{"image/jpeg", "image/png"},
	})
	_, err := r.Resolve(context.Background(), []schema.Message{
		imageMessage(server.URL+"/img.png", "image/jpeg"),
	})
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected MIME mismatch rejection, got %v", err)
	}
}

func TestResolver_AllowedMIMEList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(pngBytes)
	}))
	defer server.Close()
	host := testHost(t, server.URL)
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"http"},
		AllowedHosts:   []string{host},
		MaxBytes:       1 << 20,
		AllowedMIME:    []string{"image/jpeg"},
	})
	_, err := r.Resolve(context.Background(), []schema.Message{
		imageMessage(server.URL+"/img.png", "image/png"),
	})
	if err == nil || !strings.Contains(err.Error(), "not in AllowedMIME") {
		t.Fatalf("expected AllowedMIME rejection, got %v", err)
	}
}

func TestResolver_DataAndInlineUntouched(t *testing.T) {
	r := resolver(t, ResolverConfig{MaxBytes: 1 << 20})
	data, _ := json.Marshal(base64.StdEncoding.EncodeToString(pngBytes))
	messages := []schema.Message{
		imageMessage("data:image/png;base64,AAAA", "image/png"), // data: URI 不抓
		{Role: schema.RoleAssistant, ContentBlocks: []schema.ContentBlock{
			{Image: &schema.MediaContent{Data: data, MIMEType: "image/png"}}, // 已内联不抓
		}},
	}
	out, err := r.Resolve(context.Background(), messages)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if out[0].ContentBlocks[0].Image.URI == "" {
		t.Fatal("data: URI should be preserved")
	}
}

func TestNewResolver_ConfigValidation(t *testing.T) {
	if _, err := NewResolver(ResolverConfig{MaxBytes: -1}); err == nil {
		t.Fatal("negative MaxBytes should be rejected")
	}
	// 允许 http/https 时必须声明主机白名单。
	if _, err := NewResolver(ResolverConfig{AllowedSchemes: []string{"https"}, MaxBytes: 1 << 20}); err == nil {
		t.Fatal("http(s) without AllowedHosts should be rejected")
	}
	// 默认配置可用：不解析任何外部来源。
	if _, err := NewResolver(ResolverConfig{}); err != nil {
		t.Fatalf("empty config should be valid: %v", err)
	}
}

// spyModel 记录收到的请求消息。
type spyModel struct {
	seen []schema.Message
}

func (m *spyModel) Capabilities() llm.Capabilities { return llm.Capabilities{Vision: true} }
func (m *spyModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{} }
func (m *spyModel) Invoke(_ context.Context, req llm.Request, _ ...runnable.Option) (llm.Response, error) {
	m.seen = req.Messages
	return llm.Response{Messages: []schema.Message{schema.AssistantMessage("ok")}}, nil
}
func (m *spyModel) Stream(context.Context, llm.Request, ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return nil, errors.New("not implemented")
}
func (m *spyModel) Collect(context.Context, *runnable.StreamReader[llm.Request], ...runnable.Option) (llm.Response, error) {
	return llm.Response{}, nil
}
func (m *spyModel) Transform(context.Context, *runnable.StreamReader[llm.Request], ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return nil, errors.New("not implemented")
}

func TestWrapModel_ResolvedOnlyForRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(pngBytes)
	}))
	defer server.Close()
	host := testHost(t, server.URL)
	r := resolver(t, ResolverConfig{
		AllowedSchemes: []string{"http"},
		AllowedHosts:   []string{host},
		MaxBytes:       1 << 20,
	})
	spy := &spyModel{}
	model := r.WrapModel(spy)
	req := llm.Request{Messages: []schema.Message{imageMessage(server.URL+"/img.png", "image/png")}}
	if _, err := model.Invoke(context.Background(), req); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	// 模型看到的是内联数据。
	seen := spy.seen[0].ContentBlocks[0].Image
	if len(seen.Data) == 0 || seen.URI != "" {
		t.Fatal("model did not receive resolved inline media")
	}
	// 调用方请求保持 URI、无内联字节——检查点序列化的正是这份。
	orig := req.Messages[0].ContentBlocks[0].Image
	if orig.URI != server.URL+"/img.png" || len(orig.Data) > 0 {
		t.Fatal("caller request messages were mutated")
	}
}

// stubStore 记录保存的检查点。
type stubStore struct {
	state.Store
	saved    []state.Checkpoint
	rollback *state.Checkpoint
}

func (s *stubStore) Save(_ context.Context, cp state.Checkpoint) (state.Checkpoint, error) {
	s.saved = append(s.saved, cp)
	return cp, nil
}

func (s *stubStore) RollbackBasis(context.Context, string) (state.Checkpoint, bool, error) {
	if s.rollback == nil {
		return state.Checkpoint{}, false, nil
	}
	return *s.rollback, true, nil
}

func TestStoreGuard_RejectsOversizedInline(t *testing.T) {
	inner := &stubStore{}
	guard := NewStoreGuard(inner, 100)
	big := schema.NewImageBlock("", "image/png")
	big.Image.Data = json.RawMessage(`"` + strings.Repeat("A", 200) + `"`)
	_, err := guard.Save(context.Background(), state.Checkpoint{
		ConversationID: "c1",
		Messages: []schema.Message{
			{Role: schema.RoleAssistant, ContentBlocks: []schema.ContentBlock{big}},
		},
	})
	if !errors.Is(err, ErrMediaTooLarge) {
		t.Fatalf("expected ErrMediaTooLarge, got %v", err)
	}
	if len(inner.saved) != 0 {
		t.Fatal("oversized checkpoint was saved")
	}
	small := schema.NewImageBlock("", "image/png")
	small.Image.Data = json.RawMessage(`"QUJD"`)
	if _, err := guard.Save(context.Background(), state.Checkpoint{
		ConversationID: "c1",
		Messages: []schema.Message{
			{Role: schema.RoleAssistant, ContentBlocks: []schema.ContentBlock{small}},
		},
	}); err != nil {
		t.Fatalf("small inline media should pass: %v", err)
	}
	// URI 引用的媒体不计入。
	if _, err := guard.Save(context.Background(), state.Checkpoint{
		ConversationID: "c1",
		Messages:       []schema.Message{imageMessage("https://cdn.example.com/a.png", "image/png")},
	}); err != nil {
		t.Fatalf("URI-only media should pass: %v", err)
	}
}

func TestStoreGuard_ForwardsRollbackBasisReader(t *testing.T) {
	inner := &stubStore{rollback: &state.Checkpoint{ConversationID: "c1"}}
	guard := NewStoreGuard(inner, 100)
	reader, ok := guard.(state.RollbackBasisReader)
	if !ok {
		t.Fatal("guard does not forward RollbackBasisReader")
	}
	cp, ok, err := reader.RollbackBasis(context.Background(), "c1")
	if err != nil || !ok || cp.ConversationID != "c1" {
		t.Fatalf("RollbackBasis = (%v, %v, %v)", cp, ok, err)
	}
}
