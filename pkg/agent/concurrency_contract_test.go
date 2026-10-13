package agent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// 并发契约（阶段六 8.1）：同一会话两个并发 Invoke，恰好一个成功，
// 另一个得到 state.ErrUnfinishedRun 或 state.ErrConflict，不出现两份结果。
func TestSameSessionConcurrentInvokeSingleWinner(t *testing.T) {
	store := state.NewMemoryStore()
	release := make(chan struct{})
	a, err := agent.New(llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		select {
		case <-release:
			return []schema.Message{schema.AssistantMessage("ok")}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithConversationID("shared")
	errs := make(chan error, 2)
	go func() {
		_, err := a.Invoke(context.Background(), []schema.Message{schema.UserMessage("q")}, option)
		errs <- err
	}()
	go func() {
		_, err := a.Invoke(context.Background(), []schema.Message{schema.UserMessage("q")}, option)
		errs <- err
	}()
	// 让两次调用尽量重叠：先到的在模型里等待，后到者要么早拒要么 CAS 撞车。
	time.Sleep(80 * time.Millisecond)
	close(release)
	success, rejected := 0, 0
	for i := 0; i < 2; i++ {
		err := <-errs
		switch {
		case err == nil:
			success++
		case errors.Is(err, state.ErrUnfinishedRun) || errors.Is(err, state.ErrConflict):
			rejected++
		default:
			t.Fatalf("unexpected error %v", err)
		}
	}
	if success != 1 || rejected != 1 {
		t.Fatalf("success=%d rejected=%d, want exactly one winner", success, rejected)
	}
	latest, found, err := store.Load(context.Background(), "shared")
	if err != nil || !found || latest.Status != state.StatusCompleted {
		t.Fatalf("latest status=%v found=%v err=%v", latest.Status, found, err)
	}
}
