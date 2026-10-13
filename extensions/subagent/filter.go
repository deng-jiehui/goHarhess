package subagent

import "github.com/goharness/goharness/pkg/schema"

// Filter 过滤交给子 Agent 的父历史：输入是父会话的消息视图，输出是专家
// 实际看到的消息。过滤器是纯函数，既可用于 AsTool 的 WithHistory，也可
// 直接用在图节点的 Pre 处理器里。所有过滤器都保持工具调用与结果成对：
// 结果消息被丢弃时，对应的调用块也一并移除，不留下孤立工具结果。
type Filter func([]schema.Message) []schema.Message

// LastN 只保留最近 n 条消息；n<=0 丢弃全部。截断后若开头是配不上的工具
// 结果（其调用在更早的消息里被裁掉），连同去掉，保证不引入孤立结果。
func LastN(n int) Filter {
	return func(messages []schema.Message) []schema.Message {
		if n <= 0 || len(messages) == 0 {
			return nil
		}
		if len(messages) > n {
			messages = messages[len(messages)-n:]
		}
		return dropOrphanResults(messages)
	}
}

// DropSystem 移除系统消息：专家看不到父 Agent 的系统提示。
func DropSystem() Filter {
	return func(messages []schema.Message) []schema.Message {
		out := make([]schema.Message, 0, len(messages))
		for _, m := range messages {
			if m.Role != schema.RoleSystem {
				out = append(out, m)
			}
		}
		return out
	}
}

// DropToolResults 移除工具调用及其结果：其他工具的输出不泄露给专家，
// 对应的调用块也一并去掉，消息序列只剩纯对话。
func DropToolResults() Filter {
	return func(messages []schema.Message) []schema.Message {
		out := make([]schema.Message, 0, len(messages))
		for _, m := range messages {
			m = stripToolBlocks(m)
			if len(m.ContentBlocks) == 0 {
				continue
			}
			out = append(out, m)
		}
		return out
	}
}

// UserAndFinal 只保留用户消息与最终回答（最后一条含文本的助手消息）：
// 最小的交接上下文，不含工具往返与中间推理。
func UserAndFinal() Filter {
	return func(messages []schema.Message) []schema.Message {
		out := make([]schema.Message, 0, len(messages))
		last := -1
		for i := len(messages) - 1; i >= 0; i-- {
			m := messages[i]
			if m.Role == schema.RoleAssistant && len(m.FunctionToolCalls()) == 0 &&
				len(stripToolBlocks(m).ContentBlocks) > 0 {
				last = i
				break
			}
		}
		for i, m := range messages {
			switch {
			case m.Role == schema.RoleUser:
				out = append(out, m)
			case i == last:
				out = append(out, m)
			}
		}
		return out
	}
}

// stripToolBlocks 去掉消息中的工具调用与工具结果块。
func stripToolBlocks(m schema.Message) schema.Message {
	var kept []schema.ContentBlock
	for _, block := range m.ContentBlocks {
		if block.Type == schema.ContentBlockTypeFunctionToolCall ||
			block.Type == schema.ContentBlockTypeFunctionToolResult {
			continue
		}
		kept = append(kept, block)
	}
	if len(kept) == len(m.ContentBlocks) {
		return m
	}
	m.ContentBlocks = kept
	return m
}

// dropOrphanResults 去掉开头配不上的工具结果：截断后第一条消息若携带
// 工具结果而其调用块已不在序列里，则该消息（连同连续同类消息）丢弃。
func dropOrphanResults(messages []schema.Message) []schema.Message {
	callIDs := map[string]bool{}
	for _, m := range messages {
		for _, call := range m.FunctionToolCalls() {
			callIDs[call.ID] = true
		}
	}
	start := 0
	for start < len(messages) {
		orphan := false
		for _, result := range messages[start].FunctionToolResults() {
			if !callIDs[result.CallID] {
				orphan = true
				break
			}
		}
		if !orphan {
			break
		}
		start++
	}
	return messages[start:]
}
