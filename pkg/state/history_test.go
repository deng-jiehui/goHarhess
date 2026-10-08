package state

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryHistoryAtomicConflictAndDeletion(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	first, err := s.Save(ctx, Checkpoint{ConversationID: "history"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Save(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || second.ID == first.ID || second.ParentID != first.ID {
		t.Fatalf("broken history: %+v %+v", first, second)
	}
	if _, err := s.Save(ctx, first); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale write: %v", err)
	}
	page, err := s.List(ctx, "history", ListOptions{Limit: 1})
	if err != nil || len(page) != 1 || page[0].ID != second.ID {
		t.Fatalf("latest page: %+v %v", page, err)
	}
	page, err = s.List(ctx, "history", ListOptions{Before: second.ID, Limit: 10})
	if err != nil || len(page) != 1 || page[0].ID != first.ID {
		t.Fatalf("older page: %+v %v", page, err)
	}
	old, found, err := s.Get(ctx, "history", first.ID)
	if err != nil || !found || old.Version != 1 {
		t.Fatalf("historical record: %+v %v", old, err)
	}
	if err := s.Delete(ctx, "history"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.Get(ctx, "history", first.ID); err != nil || found {
		t.Fatalf("deleted history: %t %v", found, err)
	}
}

func TestRejectOldCheckpointFormat(t *testing.T) {
	if _, err := DecodeCheckpoint([]byte(`{"schema_version":1,"conversation_id":"old"}`)); err == nil {
		t.Fatal("old format accepted")
	}
}

func TestMemoryRejectsStaleRecordAfterDeleteAndRecreate(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	old, err := s.Save(ctx, Checkpoint{ConversationID: "recreated"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "recreated"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Save(ctx, Checkpoint{ConversationID: "recreated"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Save(ctx, old); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale generation accepted: %v", err)
	}
}
