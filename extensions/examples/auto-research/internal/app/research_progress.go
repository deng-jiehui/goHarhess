package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

const progressExtraKey = "autoresearch.progress"

// ResearchProgress 是允许展示给用户的阶段产物摘要，不包含模型原始响应。
type ResearchProgress struct {
	ID          string   `json:"id"`
	Stage       string   `json:"stage"`
	Status      string   `json:"status"`
	Title       string   `json:"title"`
	Summary     string   `json:"summary"`
	Facts       []string `json:"facts,omitempty"`
	NextActions []string `json:"next_actions,omitempty"`
}

func progressMessage(progress ResearchProgress) schema.Message {
	message := schema.AssistantMessage(progress.Summary)
	message.ID = progress.ID
	data, _ := json.Marshal(progress)
	var payload map[string]any
	_ = json.Unmarshal(data, &payload)
	message.Extra = map[string]any{progressExtraKey: payload}
	return message
}

func parseProgressMessage(message schema.Message) (ResearchProgress, bool) {
	value, ok := message.Extra[progressExtraKey]
	if !ok {
		return ResearchProgress{}, false
	}
	data, err := json.Marshal(value)
	if err != nil {
		return ResearchProgress{}, false
	}
	var progress ResearchProgress
	if err = json.Unmarshal(data, &progress); err != nil || progress.ID == "" || progress.Stage == "" || progress.Title == "" {
		return ResearchProgress{}, false
	}
	return progress, true
}

func publishProgress(ctx context.Context, progress ResearchProgress) error {
	progress.Summary = strings.TrimSpace(progress.Summary)
	if progress.ID == "" || progress.Stage == "" || progress.Status == "" || progress.Title == "" || progress.Summary == "" {
		return nil
	}
	return stream.PublishSnapshots(ctx, []schema.Message{progressMessage(progress)})
}

func truncateRunes(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return strings.TrimSpace(string(runes[:limit])) + "…"
}

// documentProgressSummary 只展示文档规模和结构，避免把同一份正文重复投影到多个阶段卡片。
func documentProgressSummary(content, prefix string) string {
	content = strings.TrimSpace(content)
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "已形成阶段产物"
	}
	runeCount := len([]rune(content))
	sectionCount := 0
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			level := 0
			for level < len(line) && line[level] == '#' {
				level++
			}
			if level > 0 && level <= 6 && len(line) > level && line[level] == ' ' {
				sectionCount++
			}
		}
	}
	return fmt.Sprintf("%s，约 %d 字，包含 %d 个章节。", prefix, runeCount, sectionCount)
}
