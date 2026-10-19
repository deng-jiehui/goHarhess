package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func TestCheckpointStorePersistsAcrossReopenAndRejectsStaleSave(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	project, err := store.CreateProject(ctx, "alice", "检查点项目")
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.Scope{UserID: "alice", ProjectID: project.ID}
	session, err := store.CreateSession(ctx, scope, "会话")
	if err != nil {
		t.Fatal(err)
	}
	turn, err := store.AdmitTurn(ctx, scope, session.ID, domain.TurnDraft{InputText: "目标", IdempotencyKey: "cp-1", DefinitionVersion: "v1", ContextJSON: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := NewCheckpointStore(store, scope, session.ID, turn.Turn.ID)
	first, err := checkpoints.Save(ctx, state.Checkpoint{ConversationID: "autoresearch:" + turn.Turn.ID, Messages: []schema.Message{schema.UserMessage("目标")}, Values: map[string]any{"step": 1}, Status: state.StatusInterrupted})
	if err != nil {
		t.Fatal(err)
	}
	stale := first.Clone()
	second, err := checkpoints.Save(ctx, first)
	if err != nil || second.Version != 2 {
		t.Fatalf("第二版检查点=%+v err=%v", second, err)
	}
	if _, err = checkpoints.Save(ctx, stale); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("旧检查点未冲突: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	loaded, found, err := NewCheckpointStore(store, scope, session.ID, turn.Turn.ID).Load(ctx, "autoresearch:"+turn.Turn.ID)
	if err != nil || !found || loaded.Version != 2 || loaded.Values["step"] != float64(1) {
		t.Fatalf("重启后检查点=%+v found=%v err=%v", loaded, found, err)
	}
}
