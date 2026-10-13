// Package netguard 提供远端获取与拨号的共享安全原语：主机白名单、
// 拨号时私网地址拦截。extensions/mcp 的信任策略与 extensions/media
// 的媒体解析器共用同一套检查，避免两套规则漂移。
package netguard

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// BlockedIP 判定拨号必须拒绝的地址：回环、私网、链路本地与未指定。
func BlockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

// HostAllowed 按白名单校验主机：条目是精确主机或 *.后缀（同时命中
// 后缀本身，与常见通配语义一致）。比较忽略大小写。
func HostAllowed(host string, allowed []string) bool {
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

// SecureDialContext 返回拨号前检查解析地址的 DialContext：全部解析结果
// 中任一是被拦截地址即拒绝；通过后拨已检查的 IP，避免 DNS 重绑定在
// 检查与连接之间换地址。prefix 用于错误消息标识调用方（如 "mcp policy"）。
func SecureDialContext(prefix string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("%s: resolve %s: %w", prefix, host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("%s: no address for %s", prefix, host)
		}
		for _, ip := range ips {
			if BlockedIP(ip) {
				return nil, fmt.Errorf("%s: %s resolves to blocked address %s", prefix, host, ip)
			}
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}
}

// SecureTransport 为私网拦截构造独立 transport：基于默认 transport
// 克隆后替换 DialContext，不与共享连接池混用。
func SecureTransport(prefix string) *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		base = &http.Transport{}
	}
	transport := base.Clone()
	transport.DialContext = SecureDialContext(prefix)
	return transport
}
