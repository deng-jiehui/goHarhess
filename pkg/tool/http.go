package tool

import (
	"context"
	"net/http"
	"time"

	"goharness/pkg/httpclient"
)

const DefaultHTTPTimeout = 30 * time.Second

// HTTPOptions selects the shared client made available to one tool execution.
// Pool is preferred when a tool needs clients with different timeouts; Client
// is useful for tests, mTLS or a fully application-managed transport.
type HTTPOptions struct {
	Client  *http.Client
	Pool    *httpclient.Pool
	Timeout time.Duration
}

type httpOptionsContextKey struct{}

// WithHTTPOptions overrides the HTTP client source for tools executed with ctx.
// Runtime supplies defaults automatically when no override exists.
func WithHTTPOptions(ctx context.Context, options HTTPOptions) context.Context {
	return context.WithValue(ctx, httpOptionsContextKey{}, options)
}

// HTTPOptionsFromContext returns the options protected by the tool runtime.
func HTTPOptionsFromContext(ctx context.Context) (HTTPOptions, bool) {
	if ctx == nil {
		return HTTPOptions{}, false
	}
	options, ok := ctx.Value(httpOptionsContextKey{}).(HTTPOptions)
	return options, ok
}

// HTTPClient returns the pooled client for a tool call. It also works outside
// the runtime by falling back to the process-wide pool.
func HTTPClient(ctx context.Context) *http.Client {
	options, _ := HTTPOptionsFromContext(ctx)
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = DefaultHTTPTimeout
	}
	return httpclient.NewClient(httpclient.ClientConfig{
		Client:  options.Client,
		Pool:    options.Pool,
		Timeout: timeout,
	})
}

// NewHTTPService creates a JSON/REST service using the tool execution's Pool
// and timeout unless the service configuration explicitly supplies them.
func NewHTTPService(ctx context.Context, cfg httpclient.ServiceConfig) (*httpclient.Service, error) {
	options, _ := HTTPOptionsFromContext(ctx)
	if cfg.Client == nil && cfg.Pool == nil {
		if options.Client != nil {
			cfg.Client = options.Client
		} else {
			cfg.Pool = options.Pool
		}
	}
	if cfg.Timeout == 0 && cfg.Client == nil {
		cfg.Timeout = options.Timeout
		if cfg.Timeout <= 0 {
			cfg.Timeout = DefaultHTTPTimeout
		}
	}
	return httpclient.NewService(cfg)
}
