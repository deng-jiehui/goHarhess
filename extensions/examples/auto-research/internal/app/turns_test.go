package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/autoresearch/internal/infra/sqlite"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func TestNewTurnServiceUsesFiveMinuteResearchTimeout(t *testing.T) {
	service := NewTurnService(nil, nil, nil)
	if service.runTimeout != 5*time.Minute {
		t.Fatalf("研究轮次超时=%s，期望 5m", service.runTimeout)
	}
}

func TestTurnServiceAcceptsWorkspacePolicy(t *testing.T) {
	service := NewTurnService(nil, nil, nil)
	policy := WorkspacePolicy{Root: t.TempDir(), Shell: "pwsh"}
	service.SetWorkspacePolicy(policy)
	if service.workspacePolicy.Root != policy.Root || service.workspacePolicy.Shell != policy.Shell {
		t.Fatalf("工作区策略=%+v，期望=%+v", service.workspacePolicy, policy)
	}
}

func TestTurnServiceFocusInterruptResumeAndIdempotency(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "研究项目")
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.CreateSession(ctx, domain.Scope{UserID: "local-user", ProjectID: project.ID}, "目标梳理")
	if err != nil {
		t.Fatal(err)
	}
	service := NewTurnService(store, store, store)
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	first, err := service.Send(ctx, scope, session.ID, SendInput{Text: "帮我整理下一步关注点并保存为笔记", IdempotencyKey: "message-1"})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.Send(ctx, scope, session.ID, SendInput{Text: "帮我整理下一步关注点并保存为笔记", IdempotencyKey: "message-1"})
	if err != nil || !replay.Replayed || replay.Turn.ID != first.Turn.ID {
		t.Fatalf("幂等重放=%+v, err=%v", replay, err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	turn, err := service.Get(ctx, scope, session.ID, first.Turn.ID)
	if err != nil || turn.State != domain.TurnAwaitingInput {
		t.Fatalf("首次运行状态=%+v, err=%v", turn, err)
	}
	var pending []map[string]any
	if err = json.Unmarshal(turn.PendingQuestions, &pending); err != nil || len(pending) != 1 {
		t.Fatalf("待回答问题=%s, err=%v", turn.PendingQuestions, err)
	}
	interruptID, _ := pending[0]["id"].(string)
	resumed, err := service.Resume(ctx, scope, session.ID, first.Turn.ID, ResumeInput{Revision: turn.Revision, IdempotencyKey: "answer-1", Answers: []AnswerInput{{InterruptID: interruptID, Text: "仓库级缺陷"}}})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Turn.State != domain.TurnQueued {
		t.Fatalf("回答后未重新排队: %+v", resumed.Turn)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	turn, err = service.Get(ctx, scope, session.ID, first.Turn.ID)
	if err != nil || turn.State != domain.TurnCompleted {
		t.Fatalf("恢复后状态=%+v, err=%v", turn, err)
	}
	notes, err := service.Notes(ctx, scope, 20)
	if err != nil || len(notes) != 1 {
		t.Fatalf("恢复后笔记=%v, err=%v", notes, err)
	}
}

func TestTurnServicePersistsRuntimeEvents(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "运行观测")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "问答")
	if err != nil {
		t.Fatal(err)
	}
	service := NewTurnService(store, store, store)
	_, err = service.Send(ctx, scope, session.ID, SendInput{Text: "请回答项目里已有笔记说明了什么", IdempotencyKey: "runtime-events-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := service.Events(ctx, scope, session.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make(map[string]bool)
	for _, event := range events {
		if strings.HasPrefix(event.Kind, "runtime.") {
			kinds[event.Kind] = true
		}
	}
	for _, kind := range []string{"runtime.run", "runtime.node", "runtime.llm"} {
		if !kinds[kind] {
			t.Fatalf("缺少运行事件 %q，事件=%+v", kind, events)
		}
	}
}

func TestTurnServicePersistsPublicResearchProgressBeforeCompletion(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "公开进度")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "会话")
	if err != nil {
		t.Fatal(err)
	}
	service := NewTurnService(store, store, store)
	sent, err := service.Send(ctx, scope, session.ID, SendInput{Text: "请回答项目里已有笔记说明了什么", IdempotencyKey: "progress-events-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := service.Events(ctx, scope, session.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	progress, assistantDelta, completed := 0, 0, -1
	for index, event := range events {
		if event.Kind == "research.progress" {
			progress++
		}
		if event.Kind == "turn.completed" {
			completed = index
		}
		if event.Kind == "assistant.delta" {
			assistantDelta++
		}
		if event.Kind == "assistant.raw" {
			t.Fatal("不应持久化内部模型原始消息")
		}
	}
	if progress == 0 || assistantDelta == 0 || completed < 0 {
		t.Fatalf("事件缺少公开进度、答复增量或完成事件: progress=%d assistantDelta=%d completed=%d events=%+v", progress, assistantDelta, completed, events)
	}
	for index, event := range events {
		if event.Kind == "research.progress" && index > completed {
			t.Fatalf("完成后不应追加本轮进度: index=%d completed=%d", index, completed)
		}
	}
	_ = sent
}

func TestTurnServicePersistsWorkspaceToolEvents(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "工作区工具")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "代码检查")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "README.md", "service workspace\n")
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if strings.Contains(request.Messages[len(request.Messages)-1].JoinedText(""), "严格 JSON") {
			return []schema.Message{schema.AssistantMessage(`{"route":"workspace","scope":"","save":false}`)}, nil
		}
		for _, message := range request.Messages {
			if message.Role == schema.RoleTool {
				return []schema.Message{schema.AssistantMessage("已读取工作区文件。")}, nil
			}
		}
		return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("read-service", "read_file", `{"path":"README.md"}`))}, nil
	})
	service := NewTurnService(store, store, store, model)
	service.SetWorkspacePolicy(WorkspacePolicy{Root: root, Shell: "pwsh"})
	sent, err := service.Send(ctx, scope, session.ID, SendInput{Text: "请读取 README.md", IdempotencyKey: "workspace-event-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := service.Events(ctx, scope, session.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Kind == "workspace.tool" && strings.Contains(string(event.PayloadJSON), "read_file") {
			found = true
		}
	}
	if !found {
		t.Fatalf("未找到工作区工具事件，events=%+v", events)
	}
	_ = sent
}

func TestTurnServiceAbandonsUnfinishedCheckpointBeforeNewExecution(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "残留运行恢复")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "会话")
	if err != nil {
		t.Fatal(err)
	}
	service := NewTurnService(store, store, store)
	sent, err := service.Send(ctx, scope, session.ID, SendInput{Text: "请回答项目里已有笔记说明了什么", IdempotencyKey: "stale-checkpoint-1"})
	if err != nil {
		t.Fatal(err)
	}
	conversationID := "autoresearch:" + sent.Turn.ID
	if _, err = service.checkpoints.Save(ctx, state.Checkpoint{
		ConversationID: conversationID,
		RunID:          "stale-run",
		Status:         state.StatusInterrupted,
		SchemaVersion:  state.CurrentCheckpointSchemaVersion,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatalf("存在残留 checkpoint 时新执行失败: %v", err)
	}
	finished, err := service.Get(ctx, scope, session.ID, sent.Turn.ID)
	if err != nil || finished.State != domain.TurnCompleted {
		t.Fatalf("残留 checkpoint 未恢复完成: %+v, err=%v", finished, err)
	}
	checkpoint, found, err := service.checkpoints.Load(ctx, conversationID)
	if err != nil || !found || checkpoint.Unfinished() {
		t.Fatalf("残留 checkpoint 未被放弃: %+v, found=%v, err=%v", checkpoint, found, err)
	}
}

func TestTurnServiceResumesFromSQLiteCheckpointAfterStoreReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := sqlite.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	project, err := store.CreateProject(ctx, "local-user", "重启恢复")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "会话")
	if err != nil {
		t.Fatal(err)
	}
	service := NewTurnService(store, store, store)
	service.SetCheckpointFactory(func(scope domain.Scope, sessionID, turnID string) state.Store {
		return sqlite.NewCheckpointStore(store, scope, sessionID, turnID)
	})
	sent, err := service.Send(ctx, scope, session.ID, SendInput{Text: "整理下一步关注点并保存笔记", IdempotencyKey: "restart-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	paused, err := service.Get(ctx, scope, session.ID, sent.Turn.ID)
	if err != nil || paused.State != domain.TurnAwaitingInput {
		t.Fatalf("暂停状态=%+v err=%v", paused, err)
	}
	var questions []map[string]any
	if err = json.Unmarshal(paused.PendingQuestions, &questions); err != nil {
		t.Fatal(err)
	}
	interruptID, _ := questions[0]["id"].(string)
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restarted := NewTurnService(store, store, store)
	restarted.SetCheckpointFactory(func(scope domain.Scope, sessionID, turnID string) state.Store {
		return sqlite.NewCheckpointStore(store, scope, sessionID, turnID)
	})
	paused, err = restarted.Get(ctx, scope, session.ID, sent.Turn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.Resume(ctx, scope, session.ID, sent.Turn.ID, ResumeInput{Revision: paused.Revision, IdempotencyKey: "restart-answer", Answers: []AnswerInput{{InterruptID: interruptID, Text: "仓库级缺陷"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	finished, err := restarted.Get(ctx, scope, session.ID, sent.Turn.ID)
	if err != nil || finished.State != domain.TurnCompleted {
		t.Fatalf("重启后状态=%+v err=%v", finished, err)
	}
	notes, err := restarted.Notes(ctx, scope, 20)
	if err != nil || len(notes) != 1 {
		t.Fatalf("重启后笔记=%v err=%v", notes, err)
	}
}

func TestTurnServiceKeepsConfirmedAnswersInCurrentAndNextTurn(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "检索研究")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "计算预算")
	if err != nil {
		t.Fatal(err)
	}
	followUp, sawHistory := false, false
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if interactionRouteRequest(request) {
			if followUp {
				sawHistory = strings.Contains(request.Messages[0].JoinedText(""), "用户已确认：可使用什么资源？：仅 CPU")
			}
			return []schema.Message{schema.AssistantMessage(`{"route":"answer","scope":"","save":false}`)}, nil
		}
		if followUp {
			return []schema.Message{schema.AssistantMessage("继续制定小规模基线。")}, nil
		}
		if len(request.Functions) > 0 {
			if request.Messages[len(request.Messages)-1].Role == schema.RoleTool {
				return []schema.Message{schema.AssistantMessage("先采用小规模方案。")}, nil
			}
			return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("ask-service-budget", "ask_user", `{"question":"可使用什么资源？"}`))}, nil
		}
		if !strings.Contains(request.Messages[0].JoinedText(""), "用户已确认：可使用什么资源？：仅 CPU") {
			t.Error("本轮最终答复模型必须看到真实确认答案，不能只依赖草稿概括")
		}
		return []schema.Message{schema.AssistantMessage("已给出第一轮建议。")}, nil
	})
	service := NewTurnService(store, store, store, model)
	sent, err := service.Send(ctx, scope, session.ID, SendInput{Text: "给我一份检索实验建议", IdempotencyKey: "confirmed-first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	paused, err := service.Get(ctx, scope, session.ID, sent.Turn.ID)
	if err != nil || paused.State != domain.TurnAwaitingInput {
		t.Fatalf("未等待用户回答: turn=%v err=%v", paused, err)
	}
	var pending []struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(paused.PendingQuestions, &pending); err != nil || len(pending) != 1 {
		t.Fatalf("pending=%s err=%v", paused.PendingQuestions, err)
	}
	if _, err = service.Resume(ctx, scope, session.ID, sent.Turn.ID, ResumeInput{Revision: paused.Revision, IdempotencyKey: "confirmed-answer", Answers: []AnswerInput{{InterruptID: pending[0].ID, Text: "仅 CPU"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	followUp = true
	if _, err = service.Send(ctx, scope, session.ID, SendInput{Text: "根据刚才确认的条件继续", IdempotencyKey: "confirmed-next"}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	if !sawHistory {
		t.Fatal("经过 SQLite 消息持久化后，下一轮丢失了用户确认答案")
	}
}

func TestTurnServiceRejectsOldDefinitionBeforeModelOrCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "旧版任务")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "旧版会话")
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "旧任务", IdempotencyKey: "old-definition", DefinitionVersion: "autoresearch-phase2-graph-v1", ContextJSON: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	modelCalls := 0
	service := NewTurnService(store, store, store, llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		modelCalls++
		return nil, nil
	}))
	if _, err = service.RunQueued(ctx); err == nil || !strings.Contains(err.Error(), "recovery_required") {
		t.Fatalf("旧版轮次应明确拒绝自动套用新图: %v", err)
	}
	turn, err := service.Get(ctx, scope, session.ID, admitted.Turn.ID)
	if err != nil || turn.State != domain.TurnFailed || turn.ErrorCode != "recovery_required" || modelCalls != 0 {
		t.Fatalf("旧版本检查没有在执行前生效: turn=%v modelCalls=%d err=%v", turn, modelCalls, err)
	}
	if _, found, err := service.checkpoints.Load(ctx, "autoresearch:"+turn.ID); err != nil || found {
		t.Fatalf("旧版运行不应读写新检查点: found=%v err=%v", found, err)
	}
}
