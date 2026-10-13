package retrieval

import "io"

// Source 是入库的原始文档：框架只接收字节流，不抓取 URL（防 SSRF）。
// 抓取、PDF/Office 解析由应用在自己的网关或旁路服务完成。
type Source struct {
	Name     string
	MIME     string
	Body     io.Reader
	Metadata Metadata
}
