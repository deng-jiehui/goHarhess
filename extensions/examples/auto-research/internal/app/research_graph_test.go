package app

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/stream"
)

func TestDecodeGoalDecisionAcceptsMarkdownWrappedJSON(t *testing.T) {
	decision, err := decodeGoalDecision("```json\n{\"route\":\"focus\",\"scope\":\"\",\"save\":true}\n```")
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != "focus" || !decision.Save {
		t.Fatalf("解析结果=%+v", decision)
	}
}

func TestDecodeGoalDecisionRejectsMarkdownExplanation(t *testing.T) {
	if _, err := decodeGoalDecision("# 研究目标分析\n这里是说明文字"); err == nil {
		t.Fatal("说明文字不能作为结构化业务路线")
	}
}

func TestConversationContextIncludesPreviousClarification(t *testing.T) {
	state := &ResearchState{History: []schema.Message{
		schema.UserMessage("第一步我们应该怎么做"),
		schema.AssistantMessage("请先明确目标对象、核心问题和期望产出。"),
	}, InputText: "可以给我一些选项吗"}
	context := conversationContext(state)
	if !strings.Contains(context, "目标对象") {
		t.Fatalf("上下文缺少上一轮澄清问题: %q", context)
	}
	if !strings.Contains(context, "可以给我一些选项吗") {
		t.Fatalf("上下文缺少当前问题: %q", context)
	}
}

func TestFixtureFollowUpOffersOptionsInsteadOfWritingTemplate(t *testing.T) {
	answer := draftFor("__draft__answer 可以给我一些选项吗")
	if !strings.Contains(answer, "目标对象") || !strings.Contains(answer, "核心问题") {
		t.Fatalf("选项答复没有围绕上一轮目标澄清: %q", answer)
	}
}

func TestFixtureComposeKeepsFollowUpOptions(t *testing.T) {
	draft := draftFor("__draft__answer 可以给我一些选项吗")
	reply := composeFor(draft)
	if !strings.Contains(reply, "目标对象") || !strings.Contains(reply, "核心问题") {
		t.Fatalf("最终答复丢失了选项内容: %q", reply)
	}
}

func TestDecideGoalUsesCurrentUserInsteadOfHistoryKeywords(t *testing.T) {
	decision := decideGoal("可以给我一些选项吗\n对话历史：\n助手：请整理研究方向和下一步。\n当前用户：可以给我一些选项吗")
	if decision.Route != "answer" {
		t.Fatalf("历史关键词不应改变当前追问路由: %+v", decision)
	}
}

func TestDecideGoalRoutesWorkspaceRequests(t *testing.T) {
	for _, text := range []string{"请读取 README.md", "帮我运行项目测试", "查看当前代码 diff"} {
		if decision := decideGoal(text); decision.Route != "workspace" {
			t.Fatalf("目标 %q 路由=%+v，期望 workspace", text, decision)
		}
	}
}

func TestDecodeGoalDecisionRejectsRouteToolCall(t *testing.T) {
	message := schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(
		"call-route", "select_research_route", `{"route":"answer","scope":"","save":false}`,
	))
	if _, err := decodeGoalDecisionResponse(llm.Response{Messages: []schema.Message{message}}); err == nil {
		t.Fatal("结构化路由不能接受工具调用")
	}
}

func TestDecodeGoalDecisionRejectsInvalidSchema(t *testing.T) {
	for _, raw := range []string{
		`{"route":"answer","scope":""}`,
		`{"route":"answer","scope":"","save":"false"}`,
		`{"route":"answer","scope":"","save":false,"node":"persist_note"}`,
		`{"route":"answer","scope":"","save":null}`,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := decodeGoalDecision(raw); err == nil {
				t.Fatal("缺失字段、错误类型和额外字段不能作为合法路由")
			}
		})
	}
}

func TestDecodeGoalDecisionRejectsMixedToolAndJSONMessages(t *testing.T) {
	_, err := decodeGoalDecisionResponse(llm.Response{Messages: []schema.Message{
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("route-mixed", "ask_user", `{"question":"继续吗？"}`)),
		schema.AssistantMessage(`{"route":"answer","scope":"","save":false}`),
	}})
	if err == nil {
		t.Fatal("路由 JSON 前夹带的工具调用不能被忽略")
	}
}

func TestResearchGraphMalformedRouteDoesNotAskOrSave(t *testing.T) {
	var notes []domain.Note
	deps := testGraphDeps(&notes)
	var modelCalls int
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		modelCalls++
		return []schema.Message{schema.AssistantMessage("# 研究目标分析\n这里是 Markdown 说明")}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := state.NewMemoryStore()
	_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("请帮我整理研究方向并保存为项目笔记")}, graph.WithCheckpoint(checkpoint, "markdown-resume"), graph.WithDurability(graph.DurabilitySync))
	if err == nil || !strings.Contains(err.Error(), "业务路由格式错误") || errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("无效路由应在修复失败后报错，实际=%v", err)
	}
	if len(notes) != 0 || modelCalls != 2 {
		t.Fatalf("格式错误不能执行业务，notes=%v modelCalls=%d", notes, modelCalls)
	}
}

func testGraphDeps(saved *[]domain.Note) ResearchGraphDeps {
	project := domain.Project{ID: "project-1", OwnerID: "local-user", Title: "代码修复研究", Settings: domain.DefaultSettings(), Revision: 1}
	return ResearchGraphDeps{
		Scope:     domain.Scope{UserID: "local-user", ProjectID: project.ID},
		SessionID: "session-1", TurnID: "turn-1", Project: project,
		SaveNote: func(_ context.Context, note domain.Note) (domain.Note, error) {
			note.ID = "note-1"
			*saved = append(*saved, note)
			return note, nil
		},
	}
}

func TestResearchGraphRejectsOversizedProjectNoteBeforePersistence(t *testing.T) {
	var saved bool
	deps := testGraphDeps(nil)
	deps.SaveNote = func(_ context.Context, _ domain.Note) (domain.Note, error) {
		saved = true
		return domain.Note{}, nil
	}
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		marker := request.Messages[len(request.Messages)-1].JoinedText("")
		switch {
		case strings.Contains(marker, "严格 JSON"):
			return []schema.Message{schema.AssistantMessage(`{"route":"notes","scope":"","save":true}`)}, nil
		case strings.HasPrefix(marker, "__draft__"):
			return []schema.Message{schema.AssistantMessage(strings.Repeat("长", 4001))}, nil
		case strings.Contains(marker, "压缩"):
			return []schema.Message{schema.AssistantMessage(strings.Repeat("长", 4001))}, nil
		default:
			return []schema.Message{schema.AssistantMessage("已完成")}, nil
		}
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("请整理研究方向并保存项目笔记")})
	if err == nil || !strings.Contains(err.Error(), "项目笔记超过") {
		t.Fatalf("超长项目笔记未得到明确错误: %v", err)
	}
	if saved {
		t.Fatal("超长项目笔记不应进入持久化层")
	}
}

func TestResearchGraphCompactsOversizedProjectNoteBeforePersistence(t *testing.T) {
	var saved domain.Note
	deps := testGraphDeps(nil)
	deps.SaveNote = func(_ context.Context, note domain.Note) (domain.Note, error) {
		saved = note
		saved.ID = "note-compact"
		return saved, nil
	}
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		marker := request.Messages[len(request.Messages)-1].JoinedText("")
		switch {
		case strings.Contains(marker, "严格 JSON"):
			return []schema.Message{schema.AssistantMessage(`{"route":"notes","scope":"","save":true}`)}, nil
		case strings.HasPrefix(marker, "__draft__"):
			return []schema.Message{schema.AssistantMessage(strings.Repeat("长", 4001))}, nil
		case strings.Contains(marker, "压缩"):
			return []schema.Message{schema.AssistantMessage("压缩后的项目笔记")}, nil
		default:
			return []schema.Message{schema.AssistantMessage("已完成")}, nil
		}
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("请整理研究方向并保存项目笔记")})
	if err != nil || len(output) != 1 {
		t.Fatalf("超长项目笔记压缩后未完成: output=%v err=%v", output, err)
	}
	if saved.Text != "压缩后的项目笔记" {
		t.Fatalf("保存的笔记不是压缩结果: %+v", saved)
	}
}

func TestResearchGraphSnapshotHasBusinessGates(t *testing.T) {
	var notes []domain.Note
	compiled, err := BuildResearchGraph(testGraphDeps(&notes))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := compiled.Snapshot()
	for _, name := range []string{"understand_goal", "clarify", "check_artifact", "write_gate", "persist_note", "completion_gate", "finish"} {
		found := false
		for _, node := range snapshot.Nodes {
			if node.ID == name {
				found = true
			}
		}
		if !found {
			t.Fatalf("图缺少业务节点 %q", name)
		}
	}
}

func TestResearchGraphSnapshotHasWorkspaceSubgraphWhenEnabled(t *testing.T) {
	deps := testGraphDeps(nil)
	deps.WorkspacePolicy = WorkspacePolicy{Root: t.TempDir(), Shell: "pwsh"}
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := compiled.Snapshot()
	for _, name := range []string{"workspace_model", "workspace_tools", "workspace_gate"} {
		found := false
		for _, node := range snapshot.Nodes {
			if node.ID == name {
				found = true
			}
		}
		if !found {
			t.Fatalf("图缺少工作区节点 %q", name)
		}
	}
}

func TestResearchGraphWorkspaceModelSelectsReadTool(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "README.md", "workspace evidence\n")
	deps := testGraphDeps(nil)
	deps.WorkspacePolicy = WorkspacePolicy{Root: root, Shell: "pwsh"}
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		marker := request.Messages[len(request.Messages)-1].JoinedText("")
		if strings.Contains(marker, "严格 JSON") {
			return []schema.Message{schema.AssistantMessage(`{"route":"workspace","scope":"","save":false}`)}, nil
		}
		for _, message := range request.Messages {
			if message.Role == schema.RoleTool {
				return []schema.Message{schema.AssistantMessage("已读取 README.md：workspace evidence")}, nil
			}
		}
		if len(request.Functions) > 0 && strings.Contains(request.Functions[0].Name, "list_directory") {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(
				"read-workspace", "read_file", `{"path":"README.md"}`,
			))}, nil
		}
		return []schema.Message{schema.AssistantMessage("工作区检查完成")}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("请检查项目工作区中的 README.md")})
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 || !strings.Contains(output[0].JoinedText(""), "workspace evidence") {
		t.Fatalf("工作区答复 = %v", output)
	}
}

func TestResearchGraphFixtureWorkspaceRouteCompletesWithToolLoop(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "README.md", "fixture workspace\n")
	deps := testGraphDeps(nil)
	deps.WorkspacePolicy = WorkspacePolicy{Root: root, Shell: "pwsh"}
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("请读取 README.md")})
	if err != nil || len(output) != 1 {
		t.Fatalf("fixture 工作区结果=%v err=%v", output, err)
	}
}

func TestResearchGraphWorkspaceWritePausesForConfirmationAndResumes(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "main.go", "package main\n")
	patchText := "diff --git a/main.go b/main.go\n--- a/main.go\n+++ b/main.go\n@@ -1 +1,3 @@\n package main\n+\n+// confirmed\n"
	deps := testGraphDeps(nil)
	deps.WorkspacePolicy = WorkspacePolicy{Root: root, Shell: "pwsh"}
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if strings.Contains(request.Messages[len(request.Messages)-1].JoinedText(""), "严格 JSON") {
			return []schema.Message{schema.AssistantMessage(`{"route":"workspace","scope":"","save":false}`)}, nil
		}
		for _, message := range request.Messages {
			if message.Role == schema.RoleTool {
				return []schema.Message{schema.AssistantMessage("补丁已经应用。")}, nil
			}
		}
		return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("patch-write", "apply_patch", mustJSON(t, ApplyPatchArgs{Patch: patchText})))}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := state.NewMemoryStore()
	_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("请修改 main.go")}, graph.WithCheckpoint(checkpoint, "workspace-write"), graph.WithDurability(graph.DurabilitySync))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("写入前错误=%v，期望等待确认", err)
	}
	pending, err := compiled.PendingInterrupts(context.Background(), graph.WithCheckpoint(checkpoint, "workspace-write"))
	if err != nil || len(pending) != 1 {
		t.Fatalf("待确认中断=%v err=%v", pending, err)
	}
	output, err := compiled.Resume(context.Background(), graph.WithCheckpoint(checkpoint, "workspace-write"), graph.WithResume(pending[0].ID, true))
	if err != nil || len(output) != 1 {
		t.Fatalf("确认后输出=%v err=%v", output, err)
	}
	content, err := os.ReadFile(filepath.Join(root, "main.go"))
	if err != nil || !strings.Contains(string(content), "confirmed") {
		t.Fatalf("确认后文件=%q err=%v", content, err)
	}
}

func TestResearchGraphFocusPausesBeforeSavingAndResumes(t *testing.T) {
	var notes []domain.Note
	compiled, err := BuildResearchGraph(testGraphDeps(&notes))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := state.NewMemoryStore()
	input := []schema.Message{schema.UserMessage("帮我整理下一步关注点并保存为笔记")}
	_, err = compiled.Invoke(context.Background(), input, graph.WithCheckpoint(checkpoint, "turn-1"), graph.WithDurability(graph.DurabilitySync))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("首次执行错误=%v，期望等待补充信息", err)
	}
	if len(notes) != 0 {
		t.Fatalf("暂停前写入笔记 %d 条", len(notes))
	}
	pending, err := compiled.PendingInterrupts(context.Background(), graph.WithCheckpoint(checkpoint, "turn-1"))
	if err != nil || len(pending) != 1 {
		t.Fatalf("待回答问题=%v, err=%v", pending, err)
	}
	output, err := compiled.Resume(context.Background(), graph.WithCheckpoint(checkpoint, "turn-1"), graph.WithResume(pending[0].ID, "仓库级缺陷"))
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || len(output) != 1 || output[0].Role != schema.RoleAssistant {
		t.Fatalf("恢复结果 output=%v notes=%v", output, notes)
	}
}

func TestResearchGraphAnswerDoesNotWriteNote(t *testing.T) {
	var notes []domain.Note
	compiled, err := BuildResearchGraph(testGraphDeps(&notes))
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("请回答项目里已有笔记说明了什么")})
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 || len(notes) != 0 {
		t.Fatalf("只读问答结果=%v notes=%v", output, notes)
	}
}

func TestResearchGraphFixtureSavesCurrentTopicWithoutCodeRepairTemplate(t *testing.T) {
	var notes []domain.Note
	deps := testGraphDeps(&notes)
	deps.Project.Title = "时间序列"
	deps.Project.Question = "多模态时间序列与大模型的结合"
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	options := graph.WithCheckpoint(state.NewMemoryStore(), "time-series-note")
	_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("整理时间序列与大模型的研究方向并保存笔记")}, options, graph.WithDurability(graph.DurabilitySync))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("离线演示未进入提问: %v", err)
	}
	pending, err := compiled.PendingInterrupts(context.Background(), options)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	_, err = compiled.Resume(context.Background(), options, graph.WithResume(pending[0].ID, "先比较候选方向"))
	if err != nil || len(notes) != 1 || !strings.Contains(notes[0].Text, "时间序列") || strings.Contains(notes[0].Text, "代码修复") {
		t.Fatalf("离线笔记不能无视当前主题套用代码修复模板: notes=%v err=%v", notes, err)
	}
}

func TestResearchGraphPublishesMeaningfulProgressBeforeInterrupt(t *testing.T) {
	var notes []domain.Note
	compiled, err := BuildResearchGraph(testGraphDeps(&notes))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := state.NewMemoryStore()
	reader, err := compiled.StreamEvents(context.Background(), []schema.Message{schema.UserMessage("帮我整理下一步关注点并保存为笔记")}, stream.Config{Buffer: 64}, graph.WithCheckpoint(checkpoint, "progress-turn"), graph.WithDurability(graph.DurabilitySync))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var stages []string
	for {
		event, recvErr := reader.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) || errors.Is(recvErr, graph.ErrInterrupted) {
				break
			}
			var interrupted *graph.InterruptedError
			if errors.As(recvErr, &interrupted) {
				break
			}
			t.Fatal(recvErr)
		}
		if event.Message == nil || event.Message.Snapshot == nil {
			continue
		}
		progress, ok := parseProgressMessage(*event.Message.Snapshot)
		if ok {
			stages = append(stages, progress.Stage)
		}
	}
	for _, want := range []string{"context", "goal", "scope"} {
		found := false
		for _, stage := range stages {
			if stage == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("进度事件缺少 %q，收到=%v", want, stages)
		}
	}
}
