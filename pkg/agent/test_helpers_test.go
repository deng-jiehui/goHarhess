package agent_test

import (
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/schema"
)

func lastBusinessMessage(messages []schema.Message) schema.Message {
	for index := len(messages) - 1; index >= 0; index-- {
		if !contextengine.IsSystemReminder(messages[index]) {
			return messages[index]
		}
	}
	return schema.Message{}
}

func businessMessages(messages []schema.Message) []schema.Message {
	result := make([]schema.Message, 0, len(messages))
	for _, message := range messages {
		if !contextengine.IsSystemReminder(message) {
			result = append(result, message)
		}
	}
	return result
}
