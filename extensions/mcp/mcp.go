// Package mcp integrates the mark3labs MCP client with goHarness tools.
// Protocol details stay inside this extension; callers receive BaseTool and
// ToolsNode abstractions from the core framework.
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdkclient "github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"

	"goharness/pkg/callback"
	"goharness/pkg/httpclient"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

type Transport string

const (
	TransportStreamableHTTP Transport = "streamable_http"
	TransportSSE            Transport = "sse"
	TransportStdio          Transport = "stdio"
)

// Config configures one MCP connection. SDKClient can be supplied for custom
// transports or in-process tests; otherwise Endpoint/Command selects a
// mature SDK transport implementation.
type Config struct {
	SDKClient     *sdkclient.Client
	Transport     Transport
	Endpoint      string
	Command       string
	Args          []string
	Env           []string
	HTTPClient    *http.Client
	Pool          *httpclient.Pool
	HTTPTimeout   time.Duration
	Headers       map[string]string
	ClientName    string
	ClientVersion string
}

// Client owns the SDK connection and the current dynamic tool snapshot.
type Client struct {
	sdk           *sdkclient.Client
	closeOnClose  bool
	clientName    string
	clientVersion string

	mu          sync.Mutex
	refreshMu   sync.Mutex
	initialized bool
	tools       []tool.BaseTool
	dirty       atomic.Bool
}

func NewClient(config Config) (*Client, error) {
	if config.ClientName == "" {
		config.ClientName = "goharness-mcp"
	}
	if config.ClientVersion == "" {
		config.ClientVersion = "1.0.0"
	}
	if config.SDKClient != nil {
		value := &Client{sdk: config.SDKClient, clientName: config.ClientName, clientVersion: config.ClientVersion}
		value.watchToolChanges()
		return value, nil
	}
	if config.Transport == "" {
		switch {
		case config.Command != "":
			config.Transport = TransportStdio
		case config.Endpoint != "":
			config.Transport = TransportStreamableHTTP
		default:
			return nil, errors.New("MCP client requires SDKClient, Endpoint, or Command")
		}
	}
	var (
		value *sdkclient.Client
		err   error
	)
	switch config.Transport {
	case TransportStreamableHTTP:
		if config.Endpoint == "" {
			return nil, errors.New("MCP streamable HTTP endpoint is empty")
		}
		opts := []transport.StreamableHTTPCOption{transport.WithHTTPBasicClient(resolveHTTPClient(config))}
		if len(config.Headers) > 0 {
			opts = append(opts, transport.WithHTTPHeaders(cloneHeaders(config.Headers)))
		}
		value, err = sdkclient.NewStreamableHttpClient(config.Endpoint, opts...)
	case TransportSSE:
		if config.Endpoint == "" {
			return nil, errors.New("MCP SSE endpoint is empty")
		}
		opts := []transport.ClientOption{transport.WithHTTPClient(resolveHTTPClient(config))}
		if len(config.Headers) > 0 {
			opts = append(opts, transport.WithHeaders(cloneHeaders(config.Headers)))
		}
		value, err = sdkclient.NewSSEMCPClient(config.Endpoint, opts...)
	case TransportStdio:
		if config.Command == "" {
			return nil, errors.New("MCP stdio command is empty")
		}
		value, err = sdkclient.NewStdioMCPClient(config.Command, append([]string(nil), config.Env...), append([]string(nil), config.Args...)...)
	default:
		return nil, fmt.Errorf("unsupported MCP transport %q", config.Transport)
	}
	if err != nil {
		return nil, fmt.Errorf("create MCP client: %w", err)
	}
	result := &Client{
		sdk:           value,
		closeOnClose:  true,
		clientName:    config.ClientName,
		clientVersion: config.ClientVersion,
	}
	result.watchToolChanges()
	return result, nil
}

func resolveHTTPClient(config Config) *http.Client {
	return httpclient.NewClient(httpclient.ClientConfig{
		Client:  config.HTTPClient,
		Pool:    config.Pool,
		Timeout: config.HTTPTimeout,
	})
}

func (c *Client) Initialize(ctx context.Context) error {
	if c == nil || c.sdk == nil {
		return errors.New("MCP client is not configured")
	}
	if ctx == nil {
		return errors.New("MCP initialization context cannot be nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized || c.sdk.IsInitialized() {
		c.initialized = true
		return nil
	}
	callbackRequest := CallbackRequest{Operation: OperationInitialize, Initialize: &InitializeRequest{ClientName: c.clientName, ClientVersion: c.clientVersion}}
	operationCtx, invocation := beginCallback(ctx, callbackRequest)
	if err := c.sdk.Start(operationCtx); err != nil {
		errorCallback(operationCtx, invocation, callbackRequest, err)
		return fmt.Errorf("start MCP client: %w", err)
	}
	request := sdkmcp.InitializeRequest{}
	request.Params.ProtocolVersion = sdkmcp.LATEST_PROTOCOL_VERSION
	request.Params.ClientInfo = sdkmcp.Implementation{Name: c.clientName, Version: c.clientVersion}
	request.Params.Capabilities = sdkmcp.ClientCapabilities{}
	result, err := c.sdk.Initialize(operationCtx, request)
	if err != nil {
		errorCallback(operationCtx, invocation, callbackRequest, err)
		return fmt.Errorf("initialize MCP client: %w", err)
	}
	c.initialized = true
	endCallback(operationCtx, invocation, CallbackResponse{Operation: OperationInitialize, Initialize: &InitializeResponse{ProtocolVersion: result.ProtocolVersion}})
	return nil
}

func (c *Client) Close() error {
	if c == nil || c.sdk == nil || !c.closeOnClose {
		return nil
	}
	return c.sdk.Close()
}

// RefreshTools fetches the complete MCP tools/list result and atomically
// replaces the adapter snapshot. Existing adapters remain valid for in-flight
// calls; new dynamic nodes use the new schema set.
func (c *Client) RefreshTools(ctx context.Context) ([]tool.BaseTool, error) {
	if c == nil {
		return nil, errors.New("MCP client is not configured")
	}
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	if err := c.Initialize(ctx); err != nil {
		return nil, err
	}
	// Clear before the request so a list_changed notification received during
	// the request remains dirty and triggers one more refresh.
	c.dirty.Store(false)
	callbackRequest := CallbackRequest{Operation: OperationListTools, ListTools: &ListToolsRequest{Refresh: true}}
	operationCtx, invocation := beginCallback(ctx, callbackRequest)
	result, err := c.sdk.ListTools(operationCtx, sdkmcp.ListToolsRequest{})
	if err != nil {
		errorCallback(operationCtx, invocation, callbackRequest, err)
		c.dirty.Store(true)
		return nil, fmt.Errorf("list MCP tools: %w", err)
	}
	values := make([]tool.BaseTool, 0, len(result.Tools))
	for index, definition := range result.Tools {
		value, err := newMCPTool(c, definition)
		if err != nil {
			c.dirty.Store(true)
			return nil, fmt.Errorf("adapt MCP tool %d: %w", index, err)
		}
		values = append(values, value)
	}
	c.mu.Lock()
	c.tools = append([]tool.BaseTool(nil), values...)
	c.mu.Unlock()
	specs := make([]tool.Spec, len(values))
	for index, value := range values {
		specs[index] = value.Spec()
	}
	endCallback(operationCtx, invocation, CallbackResponse{Operation: OperationListTools, ListTools: &ListToolsResponse{Tools: specs}})
	return append([]tool.BaseTool(nil), values...), nil
}

func (c *Client) Tools(ctx context.Context) ([]tool.BaseTool, error) {
	c.mu.Lock()
	values := append([]tool.BaseTool(nil), c.tools...)
	dirty := c.dirty.Load()
	c.mu.Unlock()
	if len(values) == 0 || dirty {
		return c.RefreshTools(ctx)
	}
	return values, nil
}

// NewToolsNode creates a dynamic node backed by the current MCP tool schema.
// A tools/list_changed notification causes the next invocation to refresh it.
func (c *Client) NewToolsNode(ctx context.Context, config tool.ToolNodeConfig) (*DynamicToolsNode, error) {
	values, err := c.RefreshTools(ctx)
	if err != nil {
		return nil, err
	}
	config.Tools = values
	node, err := tool.NewToolsNode(config)
	if err != nil {
		return nil, err
	}
	return &DynamicToolsNode{client: c, config: config, node: node}, nil
}

type DynamicToolsNode struct {
	client *Client
	config tool.ToolNodeConfig

	mu   sync.RWMutex
	node *tool.ToolsNode
}

func (n *DynamicToolsNode) Name() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.node == nil {
		return n.config.Name
	}
	return n.node.Name()
}

func (n *DynamicToolsNode) Description() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.node == nil {
		return n.config.Description
	}
	return n.node.Description()
}

func (n *DynamicToolsNode) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	if n == nil || n.client == nil {
		return nil, errors.New("dynamic MCP tools node is not configured")
	}
	if n.client.dirty.Load() {
		values, err := n.client.RefreshTools(ctx)
		if err != nil {
			return nil, err
		}
		config := n.config
		config.Tools = values
		value, err := tool.NewToolsNode(config)
		if err != nil {
			return nil, err
		}
		n.mu.Lock()
		n.node = value
		n.mu.Unlock()
	}
	n.mu.RLock()
	value := n.node
	n.mu.RUnlock()
	if value == nil {
		return nil, errors.New("dynamic MCP tools node has no tool snapshot")
	}
	return value.Invoke(ctx, messages, options...)
}

func (c *Client) watchToolChanges() {
	if c == nil || c.sdk == nil {
		return
	}
	c.sdk.OnNotification(func(notification sdkmcp.JSONRPCNotification) {
		if notification.Method == sdkmcp.MethodNotificationToolsListChanged {
			c.dirty.Store(true)
		}
	})
}

type MCPTool struct {
	client *Client
	name   string
	spec   tool.Spec
}

func newMCPTool(client *Client, definition sdkmcp.Tool) (*MCPTool, error) {
	if client == nil || client.sdk == nil {
		return nil, errors.New("MCP tool client is not configured")
	}
	if definition.Name == "" {
		return nil, errors.New("MCP tool name is empty")
	}
	parameters, err := inputSchema(definition)
	if err != nil {
		return nil, fmt.Errorf("MCP tool %q schema: %w", definition.Name, err)
	}
	isReadOnly := definition.Annotations.ReadOnlyHint != nil && *definition.Annotations.ReadOnlyHint
	return &MCPTool{
		client: client,
		name:   definition.Name,
		spec: tool.Spec{
			Name:        definition.Name,
			Description: definition.Description,
			Parameters:  parameters,
			IsReadOnly:  isReadOnly,
			Concurrency: tool.ConcurrencyPolicy{
				Group:       "mcp:" + client.clientName,
				MaxParallel: tool.DefaultToolsNodeConcurrency,
			},
		},
	}, nil
}

func (t *MCPTool) Spec() tool.Spec {
	if t == nil {
		return tool.Spec{}
	}
	return t.spec
}

func (t *MCPTool) Invoke(ctx context.Context, input tool.CallInput, _ ...runnable.Option) (tool.Result, error) {
	if t == nil || t.client == nil || t.client.sdk == nil {
		return tool.Result{}, errors.New("MCP tool is not configured")
	}
	if ctx == nil {
		return tool.Result{}, errors.New("MCP tool context cannot be nil")
	}
	callbackRequest := CallbackRequest{Operation: OperationCallTool, CallTool: &CallToolRequest{Name: t.name, Args: callback.CloneMetadata(input.Args)}}
	operationCtx, invocation := beginCallback(ctx, callbackRequest)
	result, err := t.client.sdk.CallTool(operationCtx, sdkmcp.CallToolRequest{Params: sdkmcp.CallToolParams{
		Name:      t.name,
		Arguments: input.Args,
	}})
	if err != nil {
		errorCallback(operationCtx, invocation, callbackRequest, err)
		return tool.Result{}, fmt.Errorf("call MCP tool %q: %w", t.name, err)
	}
	content, err := resultContent(result)
	if err != nil {
		errorCallback(operationCtx, invocation, callbackRequest, err)
		return tool.Result{}, fmt.Errorf("decode MCP tool %q result: %w", t.name, err)
	}
	if result.IsError {
		value := tool.ErrorResult(content)
		endCallback(operationCtx, invocation, CallbackResponse{Operation: OperationCallTool, CallTool: &CallToolResponse{Result: value}})
		return value, nil
	}
	value := tool.Result{Content: content}
	endCallback(operationCtx, invocation, CallbackResponse{Operation: OperationCallTool, CallTool: &CallToolResponse{Result: value}})
	return value, nil
}

func inputSchema(definition sdkmcp.Tool) (map[string]any, error) {
	var data []byte
	if len(definition.RawInputSchema) > 0 {
		data = append([]byte(nil), definition.RawInputSchema...)
	} else {
		var err error
		data, err = json.Marshal(definition.InputSchema)
		if err != nil {
			return nil, err
		}
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	if schema == nil {
		schema = tool.EmptyParameters()
	}
	if _, ok := schema["type"]; !ok {
		schema["type"] = "object"
	}
	if _, ok := schema["properties"]; !ok {
		schema["properties"] = map[string]any{}
	}
	return schema, nil
}

func resultContent(result *sdkmcp.CallToolResult) (schema.Content, error) {
	if result == nil {
		return schema.Content{}, errors.New("MCP returned a nil tool result")
	}
	items := make([]schema.ContentItem, 0, len(result.Content)+1)
	for _, value := range result.Content {
		switch item := value.(type) {
		case sdkmcp.TextContent:
			items = append(items, schema.TextItem(item.Text))
		case *sdkmcp.TextContent:
			if item != nil {
				items = append(items, schema.TextItem(item.Text))
			}
		case sdkmcp.ImageContent:
			items = append(items, schema.ContentItem{Image: dataURI(item.MIMEType, item.Data)})
		case *sdkmcp.ImageContent:
			if item != nil {
				items = append(items, schema.ContentItem{Image: dataURI(item.MIMEType, item.Data)})
			}
		case sdkmcp.AudioContent:
			items = append(items, schema.ContentItem{Audio: map[string]any{"data": item.Data, "mime_type": item.MIMEType}})
		case *sdkmcp.AudioContent:
			if item != nil {
				items = append(items, schema.ContentItem{Audio: map[string]any{"data": item.Data, "mime_type": item.MIMEType}})
			}
		default:
			data, err := json.Marshal(item)
			if err != nil {
				return schema.Content{}, err
			}
			items = append(items, schema.TextItem(string(data)))
		}
	}
	if len(items) == 0 && result.StructuredContent != nil {
		data, err := json.MarshalIndent(result.StructuredContent, "", "  ")
		if err != nil {
			return schema.Content{}, err
		}
		items = append(items, schema.TextItem(string(data)))
	}
	if len(items) == 0 {
		return schema.TextContent(""), nil
	}
	if len(items) == 1 && items[0].Text != nil {
		return schema.TextContent(*items[0].Text), nil
	}
	return schema.MultiContent(items...), nil
}

func dataURI(mimeType, data string) string {
	if strings.HasPrefix(data, "data:") {
		return data
	}
	if strings.TrimSpace(mimeType) == "" {
		mimeType = "application/octet-stream"
	}
	return "data:" + mimeType + ";base64," + data
}

func cloneHeaders(headers map[string]string) map[string]string {
	values := make(map[string]string, len(headers))
	for key, value := range headers {
		values[key] = value
	}
	return values
}

var _ tool.BaseTool = (*MCPTool)(nil)
var _ runnable.NamedRunnable[[]schema.Message, []schema.Message] = (*DynamicToolsNode)(nil)
