package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

func runtimeFixture(t *testing.T) (*Store, context.Context, domain.Scope, domain.Session, domain.Turn) {
	t.Helper()
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	project, err := store.CreateProject(ctx, "alice", "运行时项目")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "alice", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "会话")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "目标一", IdempotencyKey: "key-1", DefinitionVersion: "v1", ContextJSON: []byte(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	return store, ctx, scope, session, result.Turn
}

func TestTurnAdmissionIsIdempotentAndSingleActive(t *testing.T) {
	store, ctx, scope, session, first := runtimeFixture(t)
	replay, err := store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "目标一", IdempotencyKey: "key-1", DefinitionVersion: "v1", ContextJSON: []byte(`{"x":1}`)})
	if err != nil || !replay.Replayed || replay.Turn.ID != first.ID {
		t.Fatalf("重复接收=%+v, err=%v", replay, err)
	}
	if _, err = store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "目标二", IdempotencyKey: "key-1", DefinitionVersion: "v1", ContextJSON: []byte(`{"x":2}`)}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("相同幂等键不同正文未冲突: %v", err)
	}
	if _, err = store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "目标二", IdempotencyKey: "key-2", DefinitionVersion: "v1", ContextJSON: []byte(`{}`)}); !errors.Is(err, domain.ErrBusy) {
		t.Fatalf("活动轮未阻止第二个目标: %v", err)
	}
}

func TestNoteToolKeyIsIdempotentAndConflictsOnDifferentText(t *testing.T) {
	store, ctx, scope, session, turn := runtimeFixture(t)
	note := domain.Note{ProjectID: scope.ProjectID, SourceSessionID: session.ID, SourceTurnID: turn.ID, ToolKey: "stable-note", Text: "关注仓库级缺陷"}
	one, err := store.SaveNote(ctx, scope, note)
	if err != nil {
		t.Fatal(err)
	}
	two, err := store.SaveNote(ctx, scope, note)
	if err != nil || two.ID != one.ID {
		t.Fatalf("相同工具键未重放: one=%+v two=%+v err=%v", one, two, err)
	}
	note.Text = "另一份正文"
	if _, err = store.SaveNote(ctx, scope, note); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("相同工具键不同正文未冲突: %v", err)
	}
}

func TestSaveNoteRejectsOversizedTextWithActionableError(t *testing.T) {
	store, ctx, scope, session, turn := runtimeFixture(t)
	_, err := store.SaveNote(ctx, scope, domain.Note{
		ProjectID:       scope.ProjectID,
		SourceSessionID: session.ID,
		SourceTurnID:    turn.ID,
		ToolKey:         "oversized-note",
		Text:            strings.Repeat("长", domain.MaxNoteRunes+1),
	})
	if !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "项目笔记超过") {
		t.Fatalf("超长笔记错误不明确: %v", err)
	}
}

func TestMessagesAndSnapshotPersist(t *testing.T) {
	store, ctx, scope, session, turn := runtimeFixture(t)
	if err := store.SaveMessages(ctx, scope, session.ID, 1, []domain.MessageRecord{{SessionID: session.ID, TurnID: turn.ID, ContentJSON: []byte(`{"role":"user","content_blocks":[]}`), Committed: true}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx, scope, session.ID, 20)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Turn.ID != turn.ID || len(snapshot.Messages) != 1 {
		t.Fatalf("快照=%+v", snapshot)
	}
}

func TestSnapshotUsesRecentEventsSoLifecycleEndsRemainVisible(t *testing.T) {
	store, ctx, scope, session, turn := runtimeFixture(t)
	for index := 0; index < 105; index++ {
		if _, err := store.AppendEvent(ctx, scope, domain.SessionEvent{
			SessionID:   session.ID,
			TurnID:      turn.ID,
			Kind:        fmt.Sprintf("runtime.test.%03d", index),
			PayloadJSON: []byte(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}

	snapshot, err := store.Snapshot(ctx, scope, session.ID, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 100 {
		t.Fatalf("快照事件数量=%d, want 100", len(snapshot.Events))
	}
	if snapshot.Events[0].Kind != "runtime.test.005" || snapshot.Events[99].Kind != "runtime.test.104" {
		t.Fatalf("快照没有返回最近事件并保持顺序: first=%q last=%q", snapshot.Events[0].Kind, snapshot.Events[99].Kind)
	}
}
