package subagent_test

import (
	"context"
	"sync"
	"testing"

	"github.com/goharness/goharness/extensions/subagent"
	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// 并发契约（阶段六 8.1 多 Agent 条款）：父 Agent 调子 Agent 时，子 Agent
// 经共享事件流转发的事件同样带租户属性。
func TestSubagentEventsCarryTenantAttrs(t *testing.T) {
	store := state.NewMemoryStore()
	m := newManager(t, baseConfig(store))
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "调研", "name": "scout"}))

	var mu sync.Mutex
	var events []observe.Event
	collector := observe.ObserverFunc(func(_ context.Context, e observe.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	ctx := observe.With(context.Background(), collector)
	var err error
	ctx, err = tenant.With(ctx, tool.Subject{TenantID: "tenant-a", ID: "user-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Invoke(ctx, []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("no events captured")
	}
	runNames := map[string]bool{}
	for _, e := range events {
		if e.Attrs[observe.AttrTenantID] != "tenant-a" || e.Attrs[observe.AttrUserID] != "user-1" {
			t.Fatalf("event kind=%s phase=%s name=%q attrs=%v missing tenant attrs", e.Kind, e.Phase, e.Name, e.Attrs)
		}
		if e.Kind == observe.KindRun {
			runNames[e.Name] = true
		}
	}
	if len(runNames) < 2 {
		t.Fatalf("run event names = %v, want parent and child runs", runNames)
	}
	_ = subagent.Config{}
}
