package mcp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	sdkclient "github.com/mark3labs/mcp-go/client"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	mcpext "github.com/goharness/goharness/extensions/mcp"
)

func inProcess(t *testing.T, tools ...sdkmcp.Tool) *mcpext.Client {
	t.Helper()
	serverValue := server.NewMCPServer("policy-server", "1.0.0", server.WithToolCapabilities(true))
	for _, definition := range tools {
		def := definition
		serverValue.AddTool(def, func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
			return sdkmcp.NewToolResultText("ok"), nil
		})
	}
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// 远端声称只读默认不被信任；ReadOnly 回调放行才标记 IsReadOnly。
func TestPolicyIgnoresRemoteReadOnlyHint(t *testing.T) {
	readonly := true
	def := sdkmcp.NewTool("hinted")
	def.Annotations.ReadOnlyHint = &readonly
	client := inProcess(t, def)
	tools, err := client.Tools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tools[0].Spec().IsReadOnly {
		t.Fatal("remote read-only hint was trusted")
	}
}

func TestPolicyReadOnlyCallback(t *testing.T) {
	readonly := true
	def := sdkmcp.NewTool("hinted")
	def.Annotations.ReadOnlyHint = &readonly
	serverValue := server.NewMCPServer("policy-server", "1.0.0", server.WithToolCapabilities(true))
	serverValue.AddTool(def, func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
		return sdkmcp.NewToolResultText("ok"), nil
	})
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk, Policy: mcpext.Policy{
		ReadOnly: func(name string, _ *sdkmcp.ToolAnnotation) bool { return name == "hinted" },
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tools, err := client.Tools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !tools[0].Spec().IsReadOnly {
		t.Fatal("ReadOnly callback was ignored")
	}
}

// 描述去控制字符并截断；AllowedTools/DeniedTools/NamePrefix 生效。
func TestPolicySanitizesAndFilters(t *testing.T) {
	long := strings.Repeat("x", 2000)
	dirty := sdkmcp.NewTool("dirty", sdkmcp.WithDescription("inject\n\x00\x1b"+long))
	def2 := sdkmcp.NewTool("hidden")
	def3 := sdkmcp.NewTool("exposed")
	serverValue := server.NewMCPServer("policy-server", "1.0.0", server.WithToolCapabilities(true))
	for _, d := range []sdkmcp.Tool{dirty, def2, def3} {
		def := d
		serverValue.AddTool(def, func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
			return sdkmcp.NewToolResultText("ok"), nil
		})
	}
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk, Policy: mcpext.Policy{
		DeniedTools:    []string{"hidden"},
		AllowedTools:   []string{"dirty", "exposed", "hidden"},
		NamePrefix:     "remote_",
		MaxDescription: 32,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tools, err := client.Tools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	specs := map[string]string{}
	for _, tl := range tools {
		specs[tl.Spec().Name] = tl.Spec().Description
	}
	if _, ok := specs["remote_hidden"]; ok {
		t.Fatal("denied tool was exposed")
	}
	if _, ok := specs["remote_exposed"]; !ok {
		t.Fatalf("exposed tool missing: %v", specs)
	}
	desc := specs["remote_dirty"]
	if len(desc) > 32 {
		t.Fatalf("description not truncated: %d bytes", len(desc))
	}
	if strings.Contains(desc, "\x00") || strings.Contains(desc, "\x1b") || strings.Contains(desc, "\n") {
		t.Fatalf("description not sanitized: %q", desc)
	}
	// 远端调用仍用原始名（前缀只影响暴露面）。
}

func TestPolicyAllowedHostsRejects(t *testing.T) {
	_, err := mcpext.NewClient(mcpext.Config{
		Transport: mcpext.TransportStreamableHTTP,
		Endpoint:  "http://example.internal/mcp",
		Policy:    mcpext.Policy{AllowedHosts: []string{"api.example.com", "*.corp.internal"}},
	})
	if err == nil || !strings.Contains(err.Error(), "AllowedHosts") {
		t.Fatalf("err = %v", err)
	}
}

func TestPolicyStrictRequiresWhitelist(t *testing.T) {
	_, err := mcpext.NewClient(mcpext.Config{
		Transport: mcpext.TransportStreamableHTTP,
		Endpoint:  "https://api.example.com/mcp",
		Policy:    mcpext.Policy{Strict: true},
	})
	if err == nil || !strings.Contains(err.Error(), "AllowedHosts") {
		t.Fatalf("strict http err = %v", err)
	}
	_, err = mcpext.NewClient(mcpext.Config{
		Transport: mcpext.TransportStdio,
		Command:   "/usr/bin/server",
		Policy:    mcpext.Policy{Strict: true},
	})
	if err == nil || !strings.Contains(err.Error(), "AllowedCommands") {
		t.Fatalf("strict stdio err = %v", err)
	}
	_, err = mcpext.NewClient(mcpext.Config{
		Transport: mcpext.TransportStdio,
		Command:   "/usr/bin/not-allowed",
		Policy:    mcpext.Policy{AllowedCommands: []string{"/usr/bin/server"}},
	})
	if err == nil || !strings.Contains(err.Error(), "AllowedCommands") {
		t.Fatalf("command err = %v", err)
	}
}

// BlockPrivateNetworks 在拨号层拦截回环地址（httptest 也在回环，本例即被拦截）。
func TestPolicyBlocksPrivateDial(t *testing.T) {
	client, err := mcpext.NewClient(mcpext.Config{
		Transport: mcpext.TransportSSE,
		Endpoint:  "http://127.0.0.1:1/sse",
		Policy:    mcpext.Policy{BlockPrivateNetworks: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = client.Initialize(ctx)
	if err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("err = %v, want private-network block", err)
	}
}
