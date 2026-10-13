package loader

import (
	"context"
	"strings"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

// Markdown 按 ATX 标题层级切分文档：每个小节成为一个 Document，
// 标题路径写入元数据 `heading_path`（[]string），级层写入 `heading_level`。
// 没有标题的文档退化为单个 Document。
func Markdown() Loader {
	return runnable.Func[retrieval.Source, []retrieval.Document](
		func(ctx context.Context, source retrieval.Source, _ ...runnable.Option) ([]retrieval.Document, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			text, err := readAll(source)
			if err != nil {
				return nil, err
			}
			sections := splitMarkdownSections(text)
			docs := make([]retrieval.Document, 0, len(sections))
			base := documentID(source)
			for index, section := range sections {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				id := base
				if id != "" && len(sections) > 1 {
					id = base + "#" + section.anchor(index)
				}
				docs = append(docs, retrieval.Document{
					ID:   id,
					Text: section.body,
					Metadata: withSourceMeta(source, retrieval.Metadata{
						"heading_path":  append([]string(nil), section.path...),
						"heading_level": section.level,
						"section_index": index,
					}),
				})
			}
			return docs, nil
		},
	)
}

type mdSection struct {
	path  []string
	level int
	body  string
}

func (s mdSection) anchor(index int) string {
	if len(s.path) > 0 {
		return s.path[len(s.path)-1]
	}
	return string(rune('a' + index%26))
}

// splitMarkdownSections 按 ATX 标题切分：标题行进入 path 栈，
// 同级标题替换栈顶，更深层级追加，更浅层级回弹出栈。
func splitMarkdownSections(text string) []mdSection {
	var sections []mdSection
	var stack []string
	var body strings.Builder
	flush := func() {
		content := strings.TrimSpace(body.String())
		if content == "" && len(sections) > 0 {
			body.Reset()
			return
		}
		body.Reset()
		if content == "" {
			return
		}
		sections = append(sections, mdSection{
			path:  append([]string(nil), stack...),
			level: len(stack),
			body:  content,
		})
	}
	inFence := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inFence = !inFence
		}
		if !inFence {
			if level, title := atxHeading(trimmed); level > 0 {
				flush()
				switch {
				case level <= len(stack):
					stack = stack[:level-1]
					stack = append(stack, title)
				default:
					for len(stack) < level-1 {
						stack = append(stack, "")
					}
					stack = append(stack, title)
				}
				continue
			}
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	flush()
	return sections
}

// atxHeading 识别 `#`/`##`…前缀标题；返回层级与标题文本，非标题返回 0。
func atxHeading(line string) (int, string) {
	level := 0
	for level < len(line) && line[level] == '#' && level < 6 {
		level++
	}
	if level == 0 || level >= len(line) || line[level] != ' ' {
		return 0, ""
	}
	return level, strings.TrimSpace(line[level:])
}
