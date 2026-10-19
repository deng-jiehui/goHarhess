package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/autoresearch/internal/infra/sqlite"
	"github.com/goharness/autoresearch/internal/transport/httpapi"
)

func TestTurnHTTPAcceptsRunsResumesAndReadsSnapshot(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "HTTP 研究项目")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "会话")
	if err != nil {
		t.Fatal(err)
	}
	turns := app.NewTurnService(store, store, store)
	h := httpapi.NewHandlerWithRuntime(app.NewProjectService(store), app.NewSessionService(store), turns, store, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	path := "/api/projects/" + project.ID + "/sessions/" + session.ID
	accepted := call(t, h, http.MethodPost, path+"/messages", map[string]any{"text": "帮我整理下一步关注点并保存为笔记", "idempotency_key": "http-message-1"}, http.StatusAccepted)
	var sent struct {
		Turn domain.Turn `json:"turn"`
	}
	if err = json.Unmarshal(accepted.Body.Bytes(), &sent); err != nil {
		t.Fatal(err)
	}
	if _, err = turns.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	turnResponse := call(t, h, http.MethodGet, path+"/turns/"+sent.Turn.ID, nil, http.StatusOK)
	var turn domain.Turn
	if err = json.Unmarshal(turnResponse.Body.Bytes(), &turn); err != nil || turn.State != domain.TurnAwaitingInput {
		t.Fatalf("HTTP 查询轮=%+v err=%v", turn, err)
	}
	var questions []map[string]any
	if err = json.Unmarshal(turn.PendingQuestions, &questions); err != nil || len(questions) != 1 {
		t.Fatalf("问题=%s err=%v", turn.PendingQuestions, err)
	}
	interruptID, _ := questions[0]["id"].(string)
	call(t, h, http.MethodPost, path+"/turns/"+sent.Turn.ID+"/resume", map[string]any{
		"revision": turn.Revision, "idempotency_key": "http-answer-1", "answers": []map[string]string{{"interrupt_id": interruptID, "text": "仓库级缺陷"}},
	}, http.StatusAccepted)
	if _, err = turns.RunQueued(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := call(t, h, http.MethodGet, path+"/snapshot?limit=20", nil, http.StatusOK)
	if len(snapshot.Body.Bytes()) == 0 {
		t.Fatal("快照为空")
	}
	call(t, h, http.MethodGet, path+"/events?limit=20", nil, http.StatusOK)
}

func TestTurnEventsSSEWaitsForEventsWhileTurnIsActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "local-user", "SSE 进度")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "local-user", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "会话")
	if err != nil {
		t.Fatal(err)
	}
	turns := app.NewTurnService(store, store, store)
	accepted, err := turns.Send(ctx, scope, session.ID, app.SendInput{Text: "请读取项目资料", IdempotencyKey: "sse-progress-1"})
	if err != nil {
		t.Fatal(err)
	}
	h := httpapi.NewHandlerWithRuntime(app.NewProjectService(store), app.NewSessionService(store), turns, store, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	path := "/api/projects/" + project.ID + "/sessions/" + session.ID
	req := httptest.NewRequest(http.MethodGet, path+"/events?after=1", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(recorder, req)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	if _, err = store.AppendEvent(ctx, scope, domain.SessionEvent{SessionID: session.ID, TurnID: accepted.Turn.ID, Kind: "research.progress", PayloadJSON: []byte(`{"id":"turn:progress","stage":"context","status":"completed","title":"读取项目资料","summary":"已读取"}`)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE 未在取消后结束")
	}
	if !strings.Contains(recorder.Body.String(), "research.progress") {
		t.Fatalf("SSE 未收到新增进度事件: %s", recorder.Body.String())
	}
}
