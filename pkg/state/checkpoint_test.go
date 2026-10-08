package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func TestCheckpointJSONIsPortableConversationHistory(t *testing.T) {
	checkpoint := state.Checkpoint{
		ConversationID: "portable-session",
		Version:        4,
		Messages:       []schema.Message{schema.UserMessage("committed")},
		Values:         map[string]any{"tenant": "a", "nested": map[string]any{"count": 2}},
	}
	data, err := checkpoint.JSON()
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, field := range []string{`"schema_version":3`, `"conversation_id":"portable-session"`, `"messages"`, `"values"`} {
		if !strings.Contains(text, field) {
			t.Fatalf("checkpoint JSON %s does not contain %s", text, field)
		}
	}
	if strings.Contains(text, `"status"`) || strings.Contains(text, `"continuation"`) {
		t.Fatalf("conversation checkpoint leaked execution state: %s", text)
	}
	decoded, err := state.DecodeCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != state.CurrentCheckpointSchemaVersion || decoded.ConversationID != "portable-session" || decoded.Values["tenant"] != "a" {
		t.Fatalf("decoded checkpoint = %#v", decoded)
	}
	checkpoint.Values["nested"].(map[string]any)["count"] = 99
	if decoded.Values["nested"].(map[string]any)["count"] != float64(2) {
		t.Fatalf("decoded values share input map: %#v", decoded.Values)
	}
	if _, err := json.Marshal(decoded); err != nil {
		t.Fatalf("decoded checkpoint is not JSON clean: %v", err)
	}
}

func TestCheckpointRejectsRuntimeObjectsInPublicFields(t *testing.T) {
	tests := []state.Checkpoint{
		{ConversationID: "bad-values", Values: map[string]any{"handler": func() {}}},
	}
	for _, checkpoint := range tests {
		if _, err := checkpoint.JSON(); err == nil || !strings.Contains(err.Error(), "not JSON serializable") {
			t.Fatalf("Checkpoint.JSON error = %v", err)
		}
	}
}

func TestDecodeCheckpointRejectsLegacyFormats(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`{"schema_version":1,"conversation_id":"old","status":"interrupted"}`),
		[]byte(`{"schema_version":2,"conversation_id":"old","execution":{"schema_version":4}}`),
		[]byte(`{"schema_version":1,"conversation_id":"old","continuation":{"point":"before_llm"}}`),
	} {
		if _, err := state.DecodeCheckpoint(data); !errors.Is(err, state.ErrInvalidCheckpoint) {
			t.Fatalf("DecodeCheckpoint(%s) error=%v", data, err)
		}
	}
}

// Clone 保留 Extra 的具体 Go 类型并隔离嵌套可变值，不再经 JSON 归一化。
func TestCheckpointClonePreservesConcreteExtraTypes(t *testing.T) {
	msg := schema.UserMessage("hi")
	msg.Extra = map[string]any{"count": 3, "nested": map[string]any{"k": "v"}}
	checkpoint := state.Checkpoint{ConversationID: "clone-types", Messages: []schema.Message{msg}}
	cloned := checkpoint.Clone()
	if v, ok := cloned.Messages[0].Extra["count"].(int); !ok || v != 3 {
		t.Fatalf("cloned extra count=%v (%T), want int 3", cloned.Messages[0].Extra["count"], cloned.Messages[0].Extra["count"])
	}
	cloned.Messages[0].Extra["nested"].(map[string]any)["k"] = "changed"
	cloned.Messages[0].ContentBlocks[0].Text.Text = "changed"
	if checkpoint.Messages[0].Extra["nested"].(map[string]any)["k"] != "v" {
		t.Fatal("clone mutation leaked into original extra")
	}
	if checkpoint.Messages[0].JoinedText("") != "hi" {
		t.Fatal("clone mutation leaked into original content")
	}
}

func TestCheckpointSubAgentsIsolatedValidatedAndCarried(t *testing.T) {
	store := state.NewMemoryStore()
	raw := json.RawMessage(`{"format":1,"agents":[]}`)
	saved, err := store.Save(context.Background(), state.Checkpoint{ConversationID: "sub", SubAgents: raw})
	if err != nil {
		t.Fatal(err)
	}
	raw[2] = 'X'
	loaded, found, err := store.Load(context.Background(), "sub")
	if err != nil || !found || string(loaded.SubAgents) != `{"format":1,"agents":[]}` {
		t.Fatalf("stored sub agents share caller memory: %s %v", loaded.SubAgents, err)
	}
	loaded.SubAgents[2] = 'Y'
	again, _, _ := store.Load(context.Background(), "sub")
	if string(again.SubAgents) != `{"format":1,"agents":[]}` {
		t.Fatalf("loaded sub agents share store memory: %s", again.SubAgents)
	}
	data, err := saved.JSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := state.DecodeCheckpoint(data)
	if err != nil || string(decoded.SubAgents) != `{"format":1,"agents":[]}` {
		t.Fatalf("portable round trip: %s %v", decoded.SubAgents, err)
	}
	if _, err := store.Save(context.Background(), state.Checkpoint{ConversationID: "bad", SubAgents: json.RawMessage(`{`)}); !errors.Is(err, state.ErrInvalidCheckpoint) {
		t.Fatalf("invalid sub agents JSON accepted: %v", err)
	}
}
