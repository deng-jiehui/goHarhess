// Package benchfixture 提供性能基准的统一输入：固定规则的消息历史、
// 确定性模型替身、暂停现场导入与 Store 观测包装。所有生成规则标记为
// fixture-v2，修改生成规则时递增版本号并更新基准基线。
package benchfixture

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/goharness/goharness/pkg/schema"
)

// Version 标识统一输入的生成规则版本；规则变化需新基线。
const Version = "fixture-v2"

func repeatedText(size int) string {
	if size <= 0 {
		return ""
	}
	return strings.Repeat("x", size)
}

// TextHistory 生成普通对话历史：每轮一条 User 和一条 Assistant 消息，
// 每条文本 textBytes 字节（ASCII）。
func TextHistory(turns, textBytes int) []schema.Message {
	messages := make([]schema.Message, 0, turns*2)
	text := repeatedText(textBytes)
	for i := 0; i < turns; i++ {
		messages = append(messages, schema.UserMessage(text), schema.AssistantMessage(text))
	}
	return messages
}

// ToolHistory 生成工具循环历史：每轮 User、Assistant（含工具调用块）、
// Tool 结果、Assistant 共四条消息，调用 ID 与结果 ID 一一配对。
func ToolHistory(turns, textBytes int) []schema.Message {
	messages := make([]schema.Message, 0, turns*4)
	text := repeatedText(textBytes)
	for i := 0; i < turns; i++ {
		callID := fmt.Sprintf("fixture-call-%d", i)
		messages = append(messages,
			schema.UserMessage(text),
			schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(callID, "fixture_tool", `{"q":"fixture"}`)),
			schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock(callID, "fixture_tool", false, schema.NewTextBlock(text))),
			schema.AssistantMessage(text),
		)
	}
	return messages
}

// MetadataHistory 生成带嵌套元数据的消息历史：每三条为一组 Assistant
// 工具调用、Tool 结果、User 消息，调用与结果使用同一 ID 配对；末尾
// 不足一个完整组合时用普通消息补齐，不产生悬空的未配对调用。Extra
// 携带三层嵌套结构，用于测量深拷贝在富载荷下的成本。
func MetadataHistory(messages, textBytes int) []schema.Message {
	out := make([]schema.Message, 0, messages)
	text := repeatedText(textBytes)
	for i := 0; i < messages; i++ {
		pairID := fmt.Sprintf("fixture-meta-%d", i/3)
		var m schema.Message
		switch i % 3 {
		case 0:
			// 结果位存在才发调用，末尾落单用普通消息补齐。
			if i+1 < messages {
				m = schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(pairID, "fixture_tool", `{"q":"fixture"}`))
			} else {
				m = schema.UserMessage(text)
			}
		case 1:
			m = schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock(pairID, "fixture_tool", false, schema.NewTextBlock(text)))
		default:
			m = schema.UserMessage(text)
		}
		m.Extra = map[string]any{
			"nested": map[string]any{
				"level2": map[string]any{
					"values": []any{float64(i), "text", true},
				},
			},
		}
		out = append(out, m)
	}
	return out
}

// MediaHistory 生成含内嵌媒体载荷的历史：一条消息带 dataBytes 的
// base64 图片数据，用于测量拷贝与估算在媒体块上的行为。
func MediaHistory(dataBytes int) []schema.Message {
	raw := make([]byte, dataBytes)
	for i := range raw {
		raw[i] = byte(i % 251)
	}
	message := schema.NewMessage(schema.RoleUser, schema.ContentBlock{
		Type: schema.ContentBlockTypeImage,
		Image: &schema.MediaContent{
			Data:     json.RawMessage(strconv.Quote(base64.StdEncoding.EncodeToString(raw))),
			MIMEType: "image/png",
		},
	})
	return []schema.Message{message}
}

// ToolCallTail 生成一批工具调用消息：一条 Assistant 消息携带 calls 个
// 调用块，供工具批次基准作为输入末尾。
func ToolCallTail(calls int, toolNames []string) schema.Message {
	blocks := make([]schema.ContentBlock, 0, calls)
	for i := 0; i < calls; i++ {
		name := toolNames[i%len(toolNames)]
		blocks = append(blocks, schema.NewFunctionToolCallBlock(fmt.Sprintf("fixture-batch-%d", i), name, `{"i":0}`))
	}
	return schema.NewMessage(schema.RoleAssistant, blocks...)
}
