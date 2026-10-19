import ReactMarkdown, { type Components } from "react-markdown";
import rehypeSanitize from "rehype-sanitize";
import remarkGfm from "remark-gfm";

const components: Components = {
  a: ({ href, children, ...props }) => {
    const external = typeof href === "string" && /^https?:\/\//i.test(href);
    return (
      <a
        {...props}
        href={href}
        target={external ? "_blank" : undefined}
        rel={external ? "noreferrer noopener" : undefined}
      >
        {children}
      </a>
    );
  },
};

/**
 * 统一渲染模型和项目笔记中的 Markdown。
 * rehype-sanitize 会过滤 HTML 标签和危险链接协议，避免把模型输出当作页面代码执行。
 */
export function MarkdownRenderer({ source, className = "markdown-renderer" }: { source: string; className?: string }) {
  return (
    <div className={className}>
      <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeSanitize]} components={components}>
        {source}
      </ReactMarkdown>
    </div>
  );
}
