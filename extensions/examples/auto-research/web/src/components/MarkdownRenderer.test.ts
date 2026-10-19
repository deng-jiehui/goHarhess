import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { MarkdownRenderer } from "./MarkdownRenderer";

describe("MarkdownRenderer", () => {
  it("渲染标题、列表、强调和代码块", () => {
    const html = renderToStaticMarkup(
      createElement(MarkdownRenderer, { source: "# 研究方向\n\n- **基线**\n- `评测集`\n\n```go\nfmt.Println(\"ok\")\n```" }),
    );

    expect(html).toContain("<h1>研究方向</h1>");
    expect(html).toContain("<strong>基线</strong>");
    expect(html).toContain("<code>评测集</code>");
    expect(html).toContain("<pre><code class=\"language-go\">");
  });

  it("过滤危险 HTML 和链接协议", () => {
    const html = renderToStaticMarkup(
      createElement(MarkdownRenderer, { source: '<script>alert("x")</script>\n\n[危险](javascript:alert(1)) [文档](https://example.com)' }),
    );

    expect(html).not.toContain("<script");
    expect(html).not.toContain("javascript:");
    expect(html).toContain('href="https://example.com"');
  });
});
