package loader

import (
	"context"
	"errors"
	"strings"

	"golang.org/x/net/html"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

// HTML 去除 script/style/noscript/template 后收集正文文本，
// 标题层级写入元数据 heading_path；单文档输出。
func HTML() Loader {
	return runnable.Func[retrieval.Source, []retrieval.Document](
		func(ctx context.Context, source retrieval.Source, _ ...runnable.Option) ([]retrieval.Document, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if source.Body == nil {
				return nil, errors.New("loader: source body is required")
			}
			root, err := html.Parse(source.Body)
			if err != nil {
				return nil, err
			}
			var body strings.Builder
			var headings []string
			collectHTMLText(root, &body, &headings)
			meta := withSourceMeta(source, nil)
			if len(headings) > 0 {
				meta["heading_path"] = headings
			}
			return []retrieval.Document{{
				ID:       documentID(source),
				Text:     normalizeWhitespace(body.String()),
				Metadata: meta,
			}}, nil
		},
	)
}

// collectHTMLText 深度优先收集可见文本节点，跳过脚本与样式；
// 块级元素之间补换行以保留段落边界，h1-h6 文本记入 headings。
func collectHTMLText(node *html.Node, out *strings.Builder, headings *[]string) {
	if node.Type == html.ElementNode {
		switch node.Data {
		case "script", "style", "noscript", "template", "head":
			return
		}
		if isHeading(node.Data) {
			var title strings.Builder
			collectText(node, &title)
			if text := strings.TrimSpace(title.String()); text != "" {
				*headings = append(*headings, text)
				out.WriteString("\n")
				out.WriteString(text)
				out.WriteString("\n")
			}
			return
		}
	}
	if node.Type == html.TextNode {
		out.WriteString(node.Data)
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collectHTMLText(child, out, headings)
	}
	if node.Type == html.ElementNode && isBlock(node.Data) {
		out.WriteString("\n")
	}
}

func collectText(node *html.Node, out *strings.Builder) {
	if node.Type == html.TextNode {
		out.WriteString(node.Data)
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collectText(child, out)
	}
}

func isHeading(tag string) bool {
	return len(tag) == 2 && tag[0] == 'h' && tag[1] >= '1' && tag[1] <= '6'
}

func isBlock(tag string) bool {
	switch tag {
	case "p", "div", "br", "li", "ul", "ol", "section", "article",
		"table", "tr", "blockquote", "pre", "header", "footer", "main":
		return true
	}
	return false
}

func normalizeWhitespace(text string) string {
	var out strings.Builder
	blank := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.Join(strings.Fields(line), " "))
		if line == "" {
			blank = true
			continue
		}
		if blank && out.Len() > 0 {
			out.WriteString("\n\n")
		} else if out.Len() > 0 {
			out.WriteByte('\n')
		}
		out.WriteString(line)
		blank = false
	}
	return out.String()
}
