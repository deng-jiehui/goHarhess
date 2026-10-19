package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct{ db *sql.DB }

func Open(ctx context.Context, dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("创建数据目录: %w", err)
	}
	dbPath, err := filepath.Abs(filepath.Join(dataDir, "autoresearch.db"))
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", filepath.ToSlash(dbPath)+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	// 本地版串行使用连接；跨连接与后续进程的更新仍由 SQL 版本条件保护。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db}
	if err = s.migrate(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("初始化数据库: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error                   { return s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)`); err != nil {
		return err
	}
	for _, entry := range entries {
		version, err := strconv.Atoi(strings.SplitN(entry.Name(), "_", 2)[0])
		if err != nil {
			return err
		}
		var exists int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations WHERE version=?", version).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			continue
		}
		body, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("迁移 %d: %w", version, err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version) VALUES (?)", version); err != nil {
			return err
		}
	}
	return tx.Commit()
}
