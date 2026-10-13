package subagent_test

import (
	"testing"

	"github.com/goharness/goharness/extensions/subagent"
	"github.com/goharness/goharness/pkg/schema"
)

func toolResultMsg(callID, text string) schema.Message {
	return schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock(
		callID, "noop", false, schema.NewTextBlock(text)))
}

func callMsg(id string) schema.Message {
	return schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(id, "noop", "{}"))
}

// 过滤后的消息序列不允许出现孤立工具结果。
func requirePairs(t *testing.T, messages []schema.Message) {
	t.Helper()
	calls := map[string]bool{}
	for _, m := range messages {
		for _, c := range m.FunctionToolCalls() {
			calls[c.ID] = true
		}
		for _, r := range m.FunctionToolResults() {
			if !calls[r.CallID] {
				t.Fatalf("orphan tool result %q in %v", r.CallID, messages)
			}
		}
	}
}

func historySeq() []schema.Message {
	return []schema.Message{
		schema.SystemMessage("父系统提示"),
		schema.UserMessage("u1"),
		callMsg("c1"),
		toolResultMsg("c1", "r1"),
		schema.AssistantMessage("中间回复"),
		schema.UserMessage("u2"),
	}
}

func roles(messages []schema.Message) []schema.Role {
	out := make([]schema.Role, len(messages))
	for i, m := range messages {
		out[i] = m.Role
	}
	return out
}

func TestLastNKeepsPairs(t *testing.T) {
	got := subagent.LastN(3)(historySeq())
	requirePairs(t, got)
	// 截断会裁掉 c1 的调用块，孤立的结果消息也一并去掉。
	if len(got) != 2 || got[0].JoinedText("") != "中间回复" || got[1].JoinedText("") != "u2" {
		t.Fatalf("got=%v", roles(got))
	}
	if all := subagent.LastN(10)(historySeq()); len(all) != 6 {
		t.Fatalf("all=%d", len(all))
	}
	if none := subagent.LastN(0)(historySeq()); len(none) != 0 {
		t.Fatalf("none=%d", len(none))
	}
}

func TestDropToolResultsRemovesCallsToo(t *testing.T) {
	got := subagent.DropToolResults()(historySeq())
	requirePairs(t, got)
	want := []schema.Role{schema.RoleSystem, schema.RoleUser, schema.RoleAssistant, schema.RoleUser}
	if len(got) != len(want) {
		t.Fatalf("got=%v", roles(got))
	}
	for i, role := range want {
		if got[i].Role != role {
			t.Fatalf("got=%v", roles(got))
		}
	}
	// 带调用的助手消息被剥光后整条去掉，不残留空消息。
	for _, m := range got {
		if len(m.FunctionToolCalls()) != 0 || len(m.FunctionToolResults()) != 0 {
			t.Fatalf("residual tool block in %+v", m)
		}
	}
}

func TestDropSystem(t *testing.T) {
	got := subagent.DropSystem()(historySeq())
	if len(got) != 5 {
		t.Fatalf("got=%v", roles(got))
	}
	for _, m := range got {
		if m.Role == schema.RoleSystem {
			t.Fatal("system message survived")
		}
	}
}

func TestUserAndFinal(t *testing.T) {
	got := subagent.UserAndFinal()(historySeq())
	if len(got) != 3 {
		t.Fatalf("got=%v", roles(got))
	}
	if got[0].JoinedText("") != "u1" || got[1].JoinedText("") != "中间回复" || got[2].JoinedText("") != "u2" {
		t.Fatalf("got=%v", got)
	}
}
