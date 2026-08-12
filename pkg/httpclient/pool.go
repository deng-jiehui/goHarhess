// Package httpclient provides shared HTTP transports and service clients for
// model providers, vector stores, search engines, embedding and rerank APIs.
package httpclient

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// TransportConfig controls the connection pools maintained by net/http.
// One Transport automatically keeps separate idle pools for each target host.
type TransportConfig struct {
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	KeepAlive             time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
	DialContext           func(context.Context, string, string) (net.Conn, error)
}

// ClientConfig resolves an HTTP client from an explicit client, transport or
// shared Pool, in that order.
type ClientConfig struct {
	Client    *http.Client
	Transport *http.Transport
	Pool      *Pool
	Timeout   time.Duration
}

// Pool owns one reusable Transport and clients with different whole-request
// timeouts. All of those clients share the same per-host connection pools.
type Pool struct {
	transport *http.Transport

	mu      sync.Mutex
	clients map[time.Duration]*http.Client
}

// NewTransport clones Go's standard transport and applies production-friendly
// pool defaults. Proxy, HTTP/2 and standard dial settings remain enabled.
func NewTransport(cfg TransportConfig) *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		base = &http.Transport{}
	}
	transport := base.Clone()
	if cfg.MaxIdleConns > 0 {
		transport.MaxIdleConns = cfg.MaxIdleConns
	} else if transport.MaxIdleConns == 0 {
		transport.MaxIdleConns = 100
	}
	if cfg.MaxIdleConnsPerHost > 0 {
		transport.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	} else if transport.MaxIdleConnsPerHost == 0 {
		transport.MaxIdleConnsPerHost = 20
	}
	if cfg.MaxConnsPerHost > 0 {
		transport.MaxConnsPerHost = cfg.MaxConnsPerHost
	}
	if cfg.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = cfg.IdleConnTimeout
	}
	if cfg.DialContext != nil {
		transport.DialContext = cfg.DialContext
	} else if cfg.KeepAlive > 0 {
		transport.DialContext = (&net.Dialer{KeepAlive: cfg.KeepAlive}).DialContext
	}
	if cfg.TLSHandshakeTimeout > 0 {
		transport.TLSHandshakeTimeout = cfg.TLSHandshakeTimeout
	}
	if cfg.ResponseHeaderTimeout > 0 {
		transport.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
	}
	if cfg.ExpectContinueTimeout > 0 {
		transport.ExpectContinueTimeout = cfg.ExpectContinueTimeout
	}
	return transport
}

// NewPool creates a pool backed by a new Transport.
func NewPool(cfg TransportConfig) *Pool {
	return NewPoolWithTransport(NewTransport(cfg))
}

// NewPoolWithTransport creates a pool backed by transport. A nil transport
// receives the package defaults.
func NewPoolWithTransport(transport *http.Transport) *Pool {
	if transport == nil {
		transport = NewTransport(TransportConfig{})
	}
	return &Pool{transport: transport, clients: map[time.Duration]*http.Client{}}
}

// Client returns a concurrency-safe client for timeout. Clients with different
// timeouts are distinct, but their Transport and connections are shared.
func (p *Pool) Client(timeout time.Duration) *http.Client {
	if p == nil {
		return DefaultPool.Client(timeout)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if client := p.clients[timeout]; client != nil {
		return client
	}
	client := &http.Client{Transport: p.transport, Timeout: timeout}
	p.clients[timeout] = client
	return client
}

// Transport returns the shared transport for diagnostics and tuning.
func (p *Pool) Transport() *http.Transport {
	if p == nil {
		return DefaultPool.Transport()
	}
	return p.transport
}

// CloseIdleConnections releases idle connections without interrupting active
// requests. The Pool remains usable and creates connections on demand.
func (p *Pool) CloseIdleConnections() {
	if p == nil {
		DefaultPool.CloseIdleConnections()
		return
	}
	p.transport.CloseIdleConnections()
}

// NewClient resolves a client while preserving shared pooling by default.
func NewClient(cfg ClientConfig) *http.Client {
	if cfg.Client != nil {
		return cfg.Client
	}
	if cfg.Transport != nil {
		return &http.Client{Transport: cfg.Transport, Timeout: cfg.Timeout}
	}
	pool := cfg.Pool
	if pool == nil {
		pool = DefaultPool
	}
	return pool.Client(cfg.Timeout)
}

// DefaultPool is shared by all built-in HTTP integrations unless a client or
// isolated Pool is explicitly injected.
var DefaultPool = NewPool(TransportConfig{})
