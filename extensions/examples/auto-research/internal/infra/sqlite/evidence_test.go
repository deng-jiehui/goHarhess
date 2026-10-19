package sqlite

import (
	"context"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

func TestEvidenceRepositoryStoresFrozenDocumentAndEvidence(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	project, err := store.CreateProject(ctx, "alice", "证据项目")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "alice", ProjectID: project.ID}
	paper, err := store.ImportPaper(ctx, scope, domain.Paper{SourceKey: "arxiv:2401.00001", Title: "论文"})
	if err != nil {
		t.Fatal(err)
	}
	document := domain.SourceDocument{ID: "doc-1", PaperID: paper.ID, Text: "方法使用十个样本。", Locator: "abstract"}
	document.SHA256 = domain.SourceTextSHA256(document.Text)
	saved, err := store.SaveDocument(ctx, scope, document)
	if err != nil || saved.ID != document.ID {
		t.Fatalf("保存文档=%+v err=%v", saved, err)
	}
	evidence := domain.Evidence{ProjectID: project.ID, PaperID: paper.ID, DocumentID: document.ID, Field: "dataset", Value: "十个样本", Quote: "方法使用十个样本", StartRune: 0, EndRune: 9, Locator: "abstract"}
	got, err := store.SaveEvidence(ctx, scope, evidence)
	if err != nil || got.ID == "" {
		t.Fatalf("保存证据=%+v err=%v", got, err)
	}
	items, err := store.ListEvidence(ctx, scope, paper.ID)
	if err != nil || len(items) != 1 || items[0].Quote != evidence.Quote {
		t.Fatalf("证据列表=%+v err=%v", items, err)
	}
}
