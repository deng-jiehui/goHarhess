package agent

import "github.com/goharness/goharness/pkg/schema"

const agentNameMetadataKey = "agent_name"

func setMessageAgentName(message schema.Message, name string) schema.Message {
	if name == "" {
		return message
	}
	// 已命名消息原样返回：先检查后克隆，命中时不再为整个消息做深拷贝。
	if current, _ := message.Extra[agentNameMetadataKey].(string); current != "" {
		return message
	}
	m := schema.CloneMessages([]schema.Message{message})[0]
	if m.Extra == nil {
		m.Extra = map[string]any{}
	}
	m.Extra[agentNameMetadataKey] = name
	return m
}

func messageAgentName(message schema.Message) string {
	if message.Extra == nil {
		return ""
	}
	value, _ := message.Extra[agentNameMetadataKey].(string)
	return value
}
