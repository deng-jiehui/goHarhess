package tenant

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

func scopedCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, err := With(context.Background(), tool.Subject{ID: "u-1", TenantID: "acme"})
	if err != nil {
		t.Fatalf("With: %v", err)
	}
	return ctx
}

func TestWithRejectsInvalidIDs(t *testing.T) {
	for _, subject := range []tool.Subject{
		{ID: "u1", TenantID: ""},
		{ID: "u1", TenantID: "bad/tenant"},
		{ID: "bad user", TenantID: "acme"},
		{ID: "u1", TenantID: string(make([]byte, 65))},
	} {
		if _, err := With(context.Background(), subject); !errors.Is(err, ErrInvalidID) {
			t.Errorf("subject %+v: err=%v", subject, err)
		}
	}
}

func TestWithSetsSubjectMetadataScope(t *testing.T) {
	ctx := scopedCtx(t)
	scope, ok := FromContext(ctx)
	if !ok || scope.TenantID != "acme" || scope.UserID != "u-1" {
		t.Fatalf("scope=%+v ok=%v", scope, ok)
	}
	subject, err := tool.RequireSubject(ctx)
	if err != nil || subject.TenantID != "acme" || subject.ID != "u-1" {
		t.Fatalf("subject=%+v err=%v", subject, err)
	}
	metadata := observe.MetadataFromContext(ctx)
	if metadata[observe.AttrTenantID] != "acme" || metadata[observe.AttrUserID] != "u-1" {
		t.Fatalf("metadata=%v", metadata)
	}
}

func TestSessionKey(t *testing.T) {
	ctx := scopedCtx(t)
	key, err := SessionKey(ctx, "order-42")
	if err != nil {
		t.Fatal(err)
	}
	if key != "t/acme/u/u-1/s/order-42" {
		t.Fatalf("key=%q", key)
	}
	if _, err := SessionKey(context.Background(), "s"); !errors.Is(err, ErrNoScope) {
		t.Fatalf("no scope err=%v", err)
	}
	if _, err := SessionKey(ctx, ""); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("empty err=%v", err)
	}
}

func TestStoreRejectsForeignTenant(t *testing.T) {
	ctx := scopedCtx(t)
	store := NewStore(state.NewMemoryStore())
	own := "t/acme/u/u-1/s/chat"
	foreign := "t/other/u/u-9/s/chat"

	for _, key := range []string{foreign, "plain-session", "t/acme"} {
		if _, _, err := store.Load(ctx, key); !errors.Is(err, ErrTenantMismatch) {
			t.Errorf("Load %q err=%v", key, err)
		}
		if err := store.Delete(ctx, key); !errors.Is(err, ErrTenantMismatch) {
			t.Errorf("Delete %q err=%v", key, err)
		}
		if _, err := store.Save(ctx, state.Checkpoint{ConversationID: key}); !errors.Is(err, ErrTenantMismatch) {
			t.Errorf("Save %q err=%v", key, err)
		}
		if _, err := store.List(ctx, key, state.ListOptions{}); !errors.Is(err, ErrTenantMismatch) {
			t.Errorf("List %q err=%v", key, err)
		}
		if _, _, err := store.Get(ctx, key, "id"); !errors.Is(err, ErrTenantMismatch) {
			t.Errorf("Get %q err=%v", key, err)
		}
	}

	// 本租户 key 放行；子 Agent 与嵌套运行的后缀 key 前缀不变同样放行。
	for _, key := range []string{own, own + "/agents/a1", own + "/run-2/sub"} {
		cp, err := store.Save(ctx, state.Checkpoint{ConversationID: key, Status: state.StatusCompleted})
		if err != nil {
			t.Fatalf("Save %q: %v", key, err)
		}
		if _, ok, err := store.Load(ctx, key); err != nil || !ok || cp.ConversationID != key {
			t.Fatalf("Load %q: ok=%v err=%v", key, ok, err)
		}
	}

	// 无作用域一律拒绝。
	if _, _, err := store.Load(context.Background(), own); !errors.Is(err, ErrNoScope) {
		t.Fatalf("no scope err=%v", err)
	}
}

func TestStrictUserIsolatesSameTenant(t *testing.T) {
	ctx := scopedCtx(t)
	store := NewStore(state.NewMemoryStore(), StrictUser())
	mine := "t/acme/u/u-1/s/chat"
	otherUser := "t/acme/u/u-2/s/chat"
	if _, _, err := store.Load(ctx, otherUser); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("other user err=%v", err)
	}
	if _, err := store.Save(ctx, state.Checkpoint{ConversationID: mine, Status: state.StatusCompleted}); err != nil {
		t.Fatalf("own key: %v", err)
	}
}

type rbStore struct {
	state.Store
	called bool
}

func (s *rbStore) RollbackBasis(ctx context.Context, conversationID string) (state.Checkpoint, bool, error) {
	s.called = true
	return state.Checkpoint{ConversationID: conversationID}, true, nil
}

func TestRollbackBasisForwarded(t *testing.T) {
	ctx := scopedCtx(t)
	inner := &rbStore{Store: state.NewMemoryStore()}
	store := NewStore(inner)
	own := "t/acme/u/u-1/s/chat"
	if _, ok, err := store.(state.RollbackBasisReader).RollbackBasis(ctx, own); err != nil || !ok || !inner.called {
		t.Fatalf("forward ok=%v err=%v called=%v", ok, err, inner.called)
	}
	if _, _, err := store.(state.RollbackBasisReader).RollbackBasis(ctx, "t/x/u/y/s/z"); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("foreign err=%v", err)
	}
	// 内层不实现时返回未找到，调用方回退父链遍历。
	plain := NewStore(state.NewMemoryStore())
	if _, ok, err := plain.(state.RollbackBasisReader).RollbackBasis(ctx, own); err != nil || ok {
		t.Fatalf("no reader ok=%v err=%v", ok, err)
	}
}
