package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

func TestPaperImportIsIdempotentWithinProjectAndIsolatedAcrossProjects(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p1, err := store.CreateProject(ctx, "alice", "项目一")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := store.CreateProject(ctx, "alice", "项目二")
	if err != nil {
		t.Fatal(err)
	}
	paper := domain.Paper{SourceKey: "doi:10.1234/test", Title: "测试论文", Abstract: "摘要", Note: "我的笔记", Tags: []string{"时间序列"}}
	one, err := store.ImportPaper(ctx, domain.Scope{UserID: "alice", ProjectID: p1.ID}, paper)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.ImportPaper(ctx, domain.Scope{UserID: "alice", ProjectID: p1.ID}, domain.Paper{SourceKey: paper.SourceKey, Title: "新标题"})
	if err != nil || duplicate.ID != one.ID || duplicate.Title != one.Title || duplicate.Note != one.Note {
		t.Fatalf("重复导入未复用原记录: one=%+v duplicate=%+v err=%v", one, duplicate, err)
	}
	other, err := store.ImportPaper(ctx, domain.Scope{UserID: "alice", ProjectID: p2.ID}, paper)
	if err != nil || other.ID == one.ID {
		t.Fatalf("跨项目论文未隔离: one=%+v other=%+v err=%v", one, other, err)
	}
	if _, err = store.GetPaper(ctx, domain.Scope{UserID: "bob", ProjectID: p1.ID}, one.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("其他用户读取了论文: %v", err)
	}
}

func TestUpdatePaperChecksRevision(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "alice", "项目")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "alice", ProjectID: project.ID}
	paper, err := store.ImportPaper(ctx, scope, domain.Paper{SourceKey: "arxiv:2401.00001", Title: "标题"})
	if err != nil {
		t.Fatal(err)
	}
	paper.Note = "新笔记"
	if err = store.UpdatePaper(ctx, scope, paper, paper.Revision); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdatePaper(ctx, scope, paper, paper.Revision); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("旧 revision 未拒绝: %v", err)
	}
}
