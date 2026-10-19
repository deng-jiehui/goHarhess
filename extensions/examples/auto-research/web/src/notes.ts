/** 项目笔记默认只显示标题或首段，完整内容通过展开和弹窗查看。 */
export function noteSummary(text: string, limit = 120): string {
  const normalized = text.replace(/\r\n?/g, "\n").trim();
  if (!normalized) return "暂无笔记内容";
  const heading = normalized.split("\n").map((line) => line.trim()).find((line) => /^#{1,6}\s+/.test(line));
  if (heading) return heading.replace(/^#{1,6}\s+/, "").trim();
  const firstBlock = normalized.split(/\n\s*\n/)[0].replace(/\s+/g, " ").trim();
  if (firstBlock.length <= limit) return firstBlock;
  return `${firstBlock.slice(0, Math.max(1, limit - 1)).trim()}…`;
}
