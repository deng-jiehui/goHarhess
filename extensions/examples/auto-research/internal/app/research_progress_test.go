package app

import (
	"context"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

func TestProgressMessageRoundTripsStablePayload(t *testing.T) {
	want := ResearchProgress{
		ID:          "turn-1:focus",
		Stage:       "focus",
		Status:      "completed",
		Title:       "形成研究关注点",
		Summary:     "先建立可重复的代码修复基线。",
		NextActions: []string{"比较 Defects4J 与 SWE-bench"},
	}
	message := progressMessage(want)
	if message.ID != want.ID || message.JoinedText("") != want.Summary {
		t.Fatalf("message=%+v", message)
	}
	got, ok := parseProgressMessage(message)
	if !ok || got.ID != want.ID || got.Stage != want.Stage || got.Summary != want.Summary {
		t.Fatalf("parsed=%+v ok=%v", got, ok)
	}
}

func TestProgressMessageIgnoresUnrelatedAssistantMessage(t *testing.T) {
	if _, ok := parseProgressMessage(schema.AssistantMessage("普通答复")); ok {
		t.Fatal("普通答复不应被识别为研究进度")
	}
}

func TestPublishProgressUsesPublicStreamSnapshot(t *testing.T) {
	reader, err := stream.Run(context.Background(), func(ctx context.Context) (int, error) {
		if err := publishProgress(ctx, ResearchProgress{ID: "turn-1:context", Stage: "context", Status: "completed", Title: "读取项目资料", Summary: "已读取项目资料。"}); err != nil {
			return 0, err
		}
		return 1, nil
	}, stream.Config{Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for {
		event, recvErr := reader.Recv()
		if recvErr != nil {
			if strings.Contains(recvErr.Error(), "EOF") {
				break
			}
			t.Fatal(recvErr)
		}
		if event.Message == nil || event.Message.Snapshot == nil {
			continue
		}
		progress, ok := parseProgressMessage(*event.Message.Snapshot)
		if ok && progress.ID == "turn-1:context" {
			return
		}
	}
}

func TestDocumentProgressSummaryDoesNotRepeatDocumentBody(t *testing.T) {
	content := "# 研究目标\n\n这是一段很长的研究笔记正文，应该只在答复和可展开的项目笔记中展示。"
	summary := documentProgressSummary(content, "已形成项目笔记草稿")
	if !strings.Contains(summary, "已形成项目笔记草稿") || !strings.Contains(summary, "字") {
		t.Fatalf("摘要缺少状态或长度: %q", summary)
	}
	if strings.Contains(summary, "这是一段很长的研究笔记正文") {
		t.Fatalf("摘要不应重复正文: %q", summary)
	}
}
