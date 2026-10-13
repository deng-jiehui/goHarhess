package graph

import (
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/runnable"
)

// Mermaid 将快照渲染为 Mermaid flowchart 文本：节点按 NodeKind 区分
// 形状，条件边虚线并带标签，Start/End 用圆形端点。输出可直接喂给
// Mermaid 渲染器。
func (s Snapshot) Mermaid() string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")
	ids := mermaidIDs(s.Nodes)
	for _, node := range s.Nodes {
		fmt.Fprintf(&b, "    %s%s\n", ids[node.ID], mermaidShape(node.Kind, mermaidEscape(nodeLabel(node))))
	}
	// 入口与出口用圆形端点，仅在边实际引用时声明。
	if edgeHasEndpoint(s.Edges, Start) {
		fmt.Fprintf(&b, "    %s((start))\n", ids[Start])
	}
	if edgeHasEndpoint(s.Edges, End) {
		fmt.Fprintf(&b, "    %s((end))\n", ids[End])
	}
	for _, edge := range s.Edges {
		from := mermaidEdgeID(ids, edge.From)
		to := mermaidEdgeID(ids, edge.To)
		label := mermaidEscape(edge.Label)
		switch {
		case edge.Conditional && label != "":
			fmt.Fprintf(&b, "    %s -.->|%s| %s\n", from, label, to)
		case edge.Conditional:
			fmt.Fprintf(&b, "    %s -.-> %s\n", from, to)
		case label != "":
			fmt.Fprintf(&b, "    %s -->|%s| %s\n", from, label, to)
		default:
			fmt.Fprintf(&b, "    %s --> %s\n", from, to)
		}
	}
	return b.String()
}

// DOT 将快照渲染为 Graphviz DOT 文本，形状语义与 Mermaid 输出一致。
func (s Snapshot) DOT() string {
	var b strings.Builder
	fmt.Fprintf(&b, "digraph %s {\n", dotQuote(s.Name))
	b.WriteString("    rankdir=TB;\n")
	for _, node := range s.Nodes {
		fmt.Fprintf(&b, "    %s [label=%s, shape=%s];\n", dotQuote(node.ID), dotQuote(nodeLabel(node)), dotShape(node.Kind))
	}
	if edgeHasEndpoint(s.Edges, Start) {
		fmt.Fprintf(&b, "    %s [label=\"start\", shape=circle];\n", dotQuote(Start))
	}
	if edgeHasEndpoint(s.Edges, End) {
		fmt.Fprintf(&b, "    %s [label=\"end\", shape=doublecircle];\n", dotQuote(End))
	}
	for _, edge := range s.Edges {
		fmt.Fprintf(&b, "    %s -> %s [", dotQuote(edge.From), dotQuote(edge.To))
		attrs := make([]string, 0, 2)
		if edge.Label != "" {
			attrs = append(attrs, "label="+dotQuote(edge.Label))
		}
		if edge.Conditional {
			attrs = append(attrs, "style=dashed")
		}
		b.WriteString(strings.Join(attrs, ", "))
		b.WriteString("];\n")
	}
	b.WriteString("}\n")
	return b.String()
}

// nodeLabel 优先取展示名，缺省回退节点 ID。
func nodeLabel(node NodeInfo) string {
	if node.Label != "" {
		return node.Label
	}
	return node.ID
}

// mermaidIDs 为每个节点生成合法且唯一的 Mermaid ID：保留字母数字与
// 下划线，其余折叠为下划线；冲突时追加序号。Start/End 也纳入映射，
// 保证边引用与声明一致。
func mermaidIDs(nodes []NodeInfo) map[string]string {
	ids := make(map[string]string, len(nodes)+2)
	used := make(map[string]bool, len(nodes)+2)
	assign := func(id string) {
		if _, ok := ids[id]; ok {
			return
		}
		base := strings.Map(func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
				return r
			default:
				return '_'
			}
		}, id)
		if base == "" {
			base = "node"
		}
		candidate := base
		for index := 1; used[candidate]; index++ {
			candidate = fmt.Sprintf("%s_%d", base, index)
		}
		used[candidate] = true
		ids[id] = candidate
	}
	assign(Start)
	assign(End)
	for _, node := range nodes {
		assign(node.ID)
	}
	return ids
}

// mermaidEdgeID 解析边端点 ID；未声明的端点按同一规则现场生成。
func mermaidEdgeID(ids map[string]string, id string) string {
	if value, ok := ids[id]; ok {
		return value
	}
	return id
}

// mermaidShape 按节点类型选择形状：模型/Agent 用圆角框，工具/MCP
// 用双边框，子图/并行用六边形，检索器用平行四边形，其余矩形，
// Start/End 用圆形端点。
func mermaidShape(kind runnable.NodeKind, label string) string {
	quoted := `"` + label + `"`
	switch kind {
	case runnable.NodeKindChatModel, runnable.NodeKindAgent:
		return "([" + quoted + "])"
	case runnable.NodeKindTools, runnable.NodeKindMCP:
		return "[[" + quoted + "]]"
	case runnable.NodeKindGraph, runnable.NodeKindParallel:
		return "{{" + quoted + "}}"
	case runnable.NodeKindRetriever:
		return "[/" + quoted + "/]"
	default:
		return "[" + quoted + "]"
	}
}

// dotShape 为 DOT 输出选择等价形状。
func dotShape(kind runnable.NodeKind) string {
	switch kind {
	case runnable.NodeKindChatModel, runnable.NodeKindAgent:
		return "oval"
	case runnable.NodeKindTools, runnable.NodeKindMCP:
		return "component"
	case runnable.NodeKindGraph, runnable.NodeKindParallel:
		return "hexagon"
	case runnable.NodeKindRetriever:
		return "parallelogram"
	default:
		return "box"
	}
}

// mermaidEscape 转义 Mermaid 标签文本：双引号会破坏引号串，替换为
// 实体；反斜杠与竖线同样中立化，避免注入额外边语法。
func mermaidEscape(text string) string {
	replacer := strings.NewReplacer(
		`"`, "#quot;",
		`\`, "#92;",
		"|", "#124;",
		"[", "#91;",
		"]", "#93;",
	)
	return replacer.Replace(text)
}

// dotQuote 生成 DOT 引号字符串，转义反斜杠、双引号与换行。
func dotQuote(text string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
	)
	return `"` + replacer.Replace(text) + `"`
}

// edgeHasEndpoint 判断边集合是否引用了给定端点。
func edgeHasEndpoint(edges []EdgeInfo, id string) bool {
	for _, edge := range edges {
		if edge.From == id || edge.To == id {
			return true
		}
	}
	return false
}
