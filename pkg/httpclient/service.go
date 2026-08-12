package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultServiceTimeout = 30 * time.Second
	maxErrorBodySize      = 1 << 20
)

// ServiceConfig configures a JSON/REST API client. Authentication headers are
// applied per request, so unrelated services can safely share one Transport.
type ServiceConfig struct {
	BaseURL      string
	APIKey       string
	APIKeyHeader string
	APIKeyPrefix string
	Headers      map[string]string
	Client       *http.Client
	Pool         *Pool
	Timeout      time.Duration
}

// Service is a reusable JSON/REST client for vector, search, embedding and
// rerank adapters.
type Service struct {
	baseURL *url.URL
	client  *http.Client
	headers map[string]string
}

// NewService validates endpoint and authentication configuration. A zero
// timeout receives DefaultServiceTimeout; use an injected client for custom
// timeout behavior.
func NewService(cfg ServiceConfig) (*Service, error) {
	baseURL, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("invalid HTTP service base URL %q", cfg.BaseURL)
	}
	timeout := cfg.Timeout
	if timeout < 0 {
		return nil, errors.New("HTTP service timeout cannot be negative")
	}
	if timeout == 0 && cfg.Client == nil {
		timeout = DefaultServiceTimeout
	}
	headers := cloneHeaders(cfg.Headers)
	if cfg.APIKey != "" {
		header := strings.TrimSpace(cfg.APIKeyHeader)
		if header == "" {
			header = "Authorization"
		}
		prefix := cfg.APIKeyPrefix
		if prefix == "" && strings.EqualFold(header, "Authorization") {
			prefix = "Bearer "
		}
		headers[header] = prefix + cfg.APIKey
	}
	return &Service{
		baseURL: baseURL,
		client: NewClient(ClientConfig{
			Client:  cfg.Client,
			Pool:    cfg.Pool,
			Timeout: timeout,
		}),
		headers: headers,
	}, nil
}

// HTTPClient exposes the resolved client for diagnostics.
func (s *Service) HTTPClient() *http.Client {
	return s.client
}

// NewRequest creates a request relative to the service base URL.
func (s *Service) NewRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	if ctx == nil {
		return nil, errors.New("HTTP service request context cannot be nil")
	}
	endpoint, err := s.resolve(path)
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode HTTP service request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("build HTTP service request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range s.headers {
		request.Header.Set(key, value)
	}
	return request, nil
}

// DoJSON executes a JSON request and decodes a successful JSON response into
// output. output may be nil for endpoints without a response body.
func (s *Service) DoJSON(ctx context.Context, method, path string, input, output any) error {
	request, err := s.NewRequest(ctx, method, path, input)
	if err != nil {
		return err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("call HTTP service: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxErrorBodySize))
		if readErr != nil {
			return fmt.Errorf("HTTP service returned %s (read error: %w)", response.Status, readErr)
		}
		if message := strings.TrimSpace(string(data)); message != "" {
			return fmt.Errorf("HTTP service returned %s: %s", response.Status, message)
		}
		return fmt.Errorf("HTTP service returned %s", response.Status)
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(output); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("decode HTTP service response: %w", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	return nil
}

func (s *Service) resolve(path string) (*url.URL, error) {
	reference, err := url.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP service path %q: %w", path, err)
	}
	if reference.IsAbs() || reference.Host != "" {
		return nil, errors.New("HTTP service path must be relative")
	}
	base := *s.baseURL
	base.Path = strings.TrimRight(base.Path, "/") + "/"
	reference.Path = strings.TrimLeft(reference.Path, "/")
	return base.ResolveReference(reference), nil
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers)+1)
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}
