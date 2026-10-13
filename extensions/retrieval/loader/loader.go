// Package loader 提供入库流水线的文档加载器：只接收字节流，不抓取 URL，
// 避免 SSRF。需要抓取的应用在自己的网关完成，把字节以 Source 交进来。
// PDF/Office 等复杂格式不内置，使用者实现自己的
// runnable.Runnable[retrieval.Source, []retrieval.Document]。
package loader

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

// Loader 把一个 Source 归一化成一个或多个 Document。
type Loader = runnable.Runnable[retrieval.Source, []retrieval.Document]

func readAll(source retrieval.Source) (string, error) {
	if source.Body == nil {
		return "", errors.New("loader: source body is required")
	}
	data, err := io.ReadAll(source.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// documentID 取文档标识：Source 元数据的 id > Name > 空（由后续环节兜底）。
func documentID(source retrieval.Source) string {
	if source.Metadata != nil {
		if id, ok := source.Metadata["id"].(string); ok && strings.TrimSpace(id) != "" {
			return strings.TrimSpace(id)
		}
	}
	return strings.TrimSpace(source.Name)
}

func withSourceMeta(source retrieval.Source, extra retrieval.Metadata) retrieval.Metadata {
	meta := make(retrieval.Metadata, len(source.Metadata)+3)
	for key, value := range source.Metadata {
		meta[key] = value
	}
	if source.Name != "" {
		meta["source_name"] = source.Name
	}
	if source.MIME != "" {
		meta["source_mime"] = source.MIME
	}
	for key, value := range extra {
		meta[key] = value
	}
	return meta
}

// Text 返回纯文本加载器：整份输入成为一个 Document。
func Text() Loader {
	return runnable.Func[retrieval.Source, []retrieval.Document](
		func(ctx context.Context, source retrieval.Source, _ ...runnable.Option) ([]retrieval.Document, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			text, err := readAll(source)
			if err != nil {
				return nil, err
			}
			return []retrieval.Document{{
				ID:       documentID(source),
				Text:     text,
				Metadata: withSourceMeta(source, nil),
			}}, nil
		},
	)
}
