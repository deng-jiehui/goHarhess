package app

import (
	"strings"

	"github.com/goharness/goharness/pkg/schema"
)

// WorkspaceToolEvent 是可公开给前端的工作区工具摘要，不包含完整调用参数。
type WorkspaceToolEvent struct {
	Tool    string `json:"tool"`
	CallID  string `json:"call_id"`
	Status  string `json:"status"`
	IsError bool   `json:"is_error"`
	Preview string `json:"preview,omitempty"`
}

func workspaceToolEvents(message schema.Message) []WorkspaceToolEvent {
	results := message.FunctionToolResults()
	if len(results) == 0 {
		return nil
	}
	events := make([]WorkspaceToolEvent, 0, len(results))
	for _, result := range results {
		preview := schema.NewMessage(schema.RoleTool, result.ContentBlocks...).JoinedText("")
		preview = truncateRunes(strings.TrimSpace(preview), 500)
		status := "completed"
		if result.IsError {
			status = "failed"
		}
		events = append(events, WorkspaceToolEvent{
			Tool: result.Name, CallID: result.CallID, Status: status,
			IsError: result.IsError, Preview: preview,
		})
	}
	return events
}
