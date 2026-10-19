package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

func TestDatabaseReopensWithoutRepeatingMigration(t *testing.T) {
	ctx, dir := context.Background(), t.TempDir()
	for range 2 {
		store, err := Open(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err = store.db.QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations WHERE version = 1").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("基础迁移记录 = %d", count)
		}
		var foreignKeys int
		if err = store.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
			t.Fatal(err)
		}
		if foreignKeys != 1 {
			t.Fatal("未启用外键约束")
		}
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDatabasePathOccupiedDoesNotDestroyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blocked")
	if err := os.WriteFile(path, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(context.Background(), path); err == nil {
		store.Close()
		t.Fatal("数据目录被文件占用仍然启动")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "preserve" {
		t.Fatalf("原文件遭到修改: %q, %v", data, err)
	}
}

func TestNoteDeletionMigrationKeepsExistingNotes(t *testing.T) {
	ctx, dir := context.Background(), t.TempDir()
	db, err := sql.Open("sqlite", filepath.ToSlash(filepath.Join(dir, "autoresearch.db")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err = db.ExecContext(ctx, `CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	// 使用真实的旧版迁移构造已有数据库，验证升级不会丢失历史笔记。
	for index, name := range []string{"001_bootstrap.sql", "002_projects_sessions.sql", "003_agent_runtime.sql"} {
		body, readErr := migrations.ReadFile("migrations/" + name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err = db.ExecContext(ctx, string(body)); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (?)`, index+1); err != nil {
			t.Fatal(err)
		}
	}
	old := &Store{db: db}
	project, err := old.CreateProject(ctx, "alice", "已有研究项目")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "alice", ProjectID: project.ID}
	saved, err := old.SaveNote(ctx, scope, domain.Note{Text: "已有时间序列研究结论", ToolKey: "legacy-note"})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	notes, err := upgraded.ListNotes(ctx, scope, 20)
	if err != nil || len(notes) != 1 || notes[0].ID != saved.ID || notes[0].Text != "已有时间序列研究结论" {
		t.Fatalf("升级影响了原有笔记: notes=%v err=%v", notes, err)
	}
	if err = upgraded.DeleteNote(ctx, domain.Scope{UserID: "bob", ProjectID: project.ID}, saved.ID); err != domain.ErrNotFound {
		t.Fatalf("其他用户不应删除已有笔记: %v", err)
	}
	if err = upgraded.DeleteNote(ctx, scope, saved.ID); err != nil {
		t.Fatal(err)
	}
	notes, err = upgraded.ListNotes(ctx, scope, 20)
	if err != nil || len(notes) != 0 {
		t.Fatalf("升级后的删除接口未生效: notes=%v err=%v", notes, err)
	}
}
