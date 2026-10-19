package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/autoresearch/internal/infra/sqlite"
	"github.com/goharness/autoresearch/internal/transport/httpapi"
)

func TestProjectNotesDeleteIsScopedAndDoesNotEraseHistoryOrResurrectOnRetry(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "时间序列")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "早期研究")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "保存旧方向", IdempotencyKey: "old-study", DefinitionVersion: "v2", ContextJSON: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SaveMessages(ctx, scope, session.ID, 1, []domain.MessageRecord{{TurnID: turn.Turn.ID, ContentJSON: []byte(`{"role":"assistant","content_blocks":[{"type":"text","text":{"text":"历史讨论"}}]}`), Committed: true}}); err != nil {
		t.Fatal(err)
	}
	note := domain.Note{SourceSessionID: session.ID, SourceTurnID: turn.Turn.ID, ToolKey: "saved-old-topic", Text: "LLM Agent 代码修复的历史笔记"}
	saved, err := store.SaveNote(ctx, scope, note)
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.CreateProject(ctx, "local-user", "另一个项目")
	if err != nil {
		t.Fatal(err)
	}
	turns := app.NewTurnService(store, store, store)
	h := httpapi.NewHandlerWithRuntime(app.NewProjectService(store), app.NewSessionService(store), turns, store, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	path := "/api/projects/" + project.ID + "/notes"
	listed := call(t, h, http.MethodGet, path, nil, http.StatusOK)
	var body struct {
		Items []domain.Note `json:"items"`
	}
	if err = json.Unmarshal(listed.Body.Bytes(), &body); err != nil || len(body.Items) != 1 || body.Items[0].SourceSessionID != session.ID {
		t.Fatalf("新会话之外也应能读取项目笔记及来源: body=%s err=%v", listed.Body.String(), err)
	}
	call(t, h, http.MethodDelete, "/api/projects/"+other.ID+"/notes/"+saved.ID, nil, http.StatusNotFound)
	call(t, h, http.MethodDelete, path+"/"+saved.ID, nil, http.StatusNoContent)
	call(t, h, http.MethodDelete, path+"/"+saved.ID, nil, http.StatusNoContent)
	listed = call(t, h, http.MethodGet, path, nil, http.StatusOK)
	if err = json.Unmarshal(listed.Body.Bytes(), &body); err != nil || len(body.Items) != 0 {
		t.Fatalf("删除后仍展示笔记: body=%s err=%v", listed.Body.String(), err)
	}
	if _, err = store.SaveNote(ctx, scope, note); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("重放旧保存调用不能复活已删除笔记: %v", err)
	}
	snapshot, err := store.Snapshot(ctx, scope, session.ID, 20)
	if err != nil || len(snapshot.Notes) != 0 || len(snapshot.Messages) != 1 || !strings.Contains(string(snapshot.Messages[0].ContentJSON), "历史讨论") {
		t.Fatalf("删除笔记不应影响会话历史: snapshot=%v err=%v", snapshot, err)
	}
	call(t, h, http.MethodDelete, path+"/missing", nil, http.StatusNotFound)
}
