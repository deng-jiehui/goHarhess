package app_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/autoresearch/internal/infra/sqlite"
)

func TestProjectsPersistAndEnforceOwner(t *testing.T) {
	ctx, dir := context.Background(), t.TempDir()
	store, err := sqlite.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := app.NewProjectService(store)
	p, err := svc.Create(ctx, "alice", "  检索模型  ")
	if err != nil {
		t.Fatal(err)
	}
	if p.Title != "检索模型" {
		t.Fatalf("未规范标题: %q", p.Title)
	}
	if _, err = svc.Get(ctx, domain.Scope{UserID: "bob", ProjectID: p.ID}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("越权查询: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p, err = app.NewProjectService(store).Get(ctx, domain.Scope{UserID: "alice", ProjectID: p.ID})
	if err != nil || p.Title != "检索模型" {
		t.Fatalf("重启后数据丢失: %+v, %v", p, err)
	}
}

func TestSessionCannotBeReadOrUpdatedThroughAnotherProject(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p1, _ := store.CreateProject(ctx, "alice", "项目一")
	p2, _ := store.CreateProject(ctx, "alice", "项目二")
	svc := app.NewSessionService(store)
	session, err := svc.Create(ctx, domain.Scope{UserID: "alice", ProjectID: p1.ID}, "阅读")
	if err != nil {
		t.Fatal(err)
	}
	wrong := domain.Scope{UserID: "alice", ProjectID: p2.ID}
	if _, err = svc.Get(ctx, wrong, session.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("跨项目读取: %v", err)
	}
	title := "不能覆盖"
	if _, err = svc.Update(ctx, wrong, session.ID, app.SessionPatch{Title: &title}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("跨项目修改: %v", err)
	}
}

func TestSessionDeletionRejectsActiveTurn(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "alice", "活动会话")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "alice", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "运行中的会话")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "正在执行", IdempotencyKey: "active-1", DefinitionVersion: "test"}); err != nil {
		t.Fatal(err)
	}
	if err = app.NewSessionService(store).Delete(ctx, scope, session.ID); !errors.Is(err, domain.ErrBusy) {
		t.Fatalf("删除活动会话错误 = %v, want %v", err, domain.ErrBusy)
	}
	if _, err = store.GetSession(ctx, scope, session.ID); err != nil {
		t.Fatalf("活动会话不应被删除: %v", err)
	}
}
