package state_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"goharness/pkg/schema"
	"goharness/pkg/state"
)

func TestCheckpointJSONIsPortableAndRoundTripsContinuation(t *testing.T) {
	checkpoint := state.Checkpoint{
		ConversationID: "portable-session",
		Version:        4,
		Status:         state.CheckpointStatusInterrupted,
		Messages:       []schema.Message{schema.UserMessage("committed")},
		Values:         map[string]any{"tenant": "a", "count": 2},
		Continuation: &state.Continuation{
			AgentKind: "fn_call",
			Point:     "before_llm",
			Input:     []schema.Message{schema.UserMessage("current")},
			Messages:  []schema.Message{schema.UserMessage("current")},
			Response:  []schema.Message{schema.AssistantMessage("partial")},
			LLMCalls:  1,
			Cursor:    map[string]any{"step": 2},
			CursorStack: state.CursorStack{
				{Graph: "fn-call", Node: "tools", Step: 1, Iteration: 0},
				{Graph: "nested", Node: "llm", Step: 2, Iteration: 1},
			},
			Reason:   "operator approval",
			Metadata: map[string]any{"ticket": "T-1"},
			Engine: &state.EngineCheckpoint{
				SchemaVersion: state.CurrentEngineCheckpointSchemaVersion,
				AgentKind:     "fn_call",
				Point:         "before_llm",
				LLMCalls:      1,
				Round:         1,
				LastAgent:     "worker",
				CursorStack:   state.CursorStack{{Graph: "fn-call", Node: "tools", Step: 1}},
			},
			GraphState:    &state.GraphStateCheckpoint{CodecID: "recipe-state", Version: 3, Data: []byte{1, 2, 3}},
			InterruptedAt: time.Date(2026, time.August, 3, 1, 2, 3, 0, time.UTC),
		},
	}
	data, err := checkpoint.JSON()
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, field := range []string{`"schema_version":1`, `"status":"interrupted"`, `"continuation"`, `"messages"`} {
		if !strings.Contains(text, field) {
			t.Fatalf("checkpoint JSON %s does not contain %s", text, field)
		}
	}
	decoded, err := state.DecodeCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != state.CurrentCheckpointSchemaVersion || decoded.Continuation == nil || decoded.Continuation.Reason != "operator approval" || len(decoded.Continuation.CursorStack) != 2 || decoded.Continuation.Engine == nil || decoded.Continuation.Engine.LastAgent != "worker" || decoded.Continuation.GraphState == nil || decoded.Continuation.GraphState.CodecID != "recipe-state" {
		t.Fatalf("decoded checkpoint = %#v", decoded)
	}
	checkpoint.Continuation.GraphState.Data[0] = 99
	if decoded.Continuation.GraphState.Data[0] != 1 {
		t.Fatalf("decoded graph state shares input bytes: %#v", decoded.Continuation.GraphState.Data)
	}
	if _, err := json.Marshal(decoded); err != nil {
		t.Fatalf("decoded checkpoint is not JSON clean: %v", err)
	}
}

func TestCheckpointRejectsRuntimeObjectsInPublicFields(t *testing.T) {
	tests := []state.Checkpoint{
		{ConversationID: "bad-values", Values: map[string]any{"handler": func() {}}},
		{
			ConversationID: "bad-continuation",
			Status:         state.CheckpointStatusInterrupted,
			Continuation: &state.Continuation{
				AgentKind: "basic", Point: "before_llm", Messages: []schema.Message{schema.UserMessage("hello")},
				Metadata: map[string]any{"client": make(chan struct{})},
			},
		},
	}
	for _, checkpoint := range tests {
		if _, err := checkpoint.JSON(); err == nil || !strings.Contains(err.Error(), "not JSON serializable") {
			t.Fatalf("Checkpoint.JSON error = %v", err)
		}
	}
}
