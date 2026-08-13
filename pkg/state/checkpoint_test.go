package state_test

import (
	"encoding/json"
	"strings"
	"testing"

	"goharness/pkg/schema"
	"goharness/pkg/state"
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
	for _, field := range []string{`"schema_version":1`, `"conversation_id":"portable-session"`, `"messages"`, `"values"`} {
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

func TestDecodeCheckpointRejectsRemovedExecutionFields(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`{"schema_version":1,"conversation_id":"old","status":"interrupted"}`),
		[]byte(`{"schema_version":1,"conversation_id":"old","continuation":{"point":"before_llm"}}`),
	} {
		if _, err := state.DecodeCheckpoint(data); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("DecodeCheckpoint(%s) error=%v", data, err)
		}
	}
}
