package agent

import "goharness/pkg/schema"

const agentNameMetadataKey = "agent_name"

func setMessageAgentName(message schema.Message, name string) schema.Message {
	if name == "" {
		return message
	}
	m := schema.CloneMessages([]schema.Message{message})[0]
	if current, _ := m.Extra[agentNameMetadataKey].(string); current != "" {
		return m
	}
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
