package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/state"
)

func TestInterruptedErrorReturnsStoredCheckpoint(t *testing.T) {
	g := inputGraph(t, func(ctx context.Context, _ string) (string, error) {
		_, err := graph.Interrupt(ctx, "approve")
		return "ok", err
	})
	store := state.NewMemoryStore()
	_, err := g.Invoke(context.Background(), "input", graph.WithCheckpoint(store, "unified"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		t.Fatalf("%v", err)
	}
	saved, ok, err := store.Load(context.Background(), "unified")
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	if stopped.Checkpoint.ID == "" || !reflect.DeepEqual(stopped.Checkpoint, saved) {
		t.Fatal("interrupt did not return the committed checkpoint")
	}
	data, err := stopped.Checkpoint.JSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := state.DecodeCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	var execution map[string]json.RawMessage
	if err = json.Unmarshal(decoded.Execution, &execution); err != nil {
		t.Fatal(err)
	}
	if _, ok := execution["schema_version"]; ok {
		t.Fatal("execution has an independent format version")
	}
	if _, ok := execution["created_at"]; ok {
		t.Fatal("execution has an independent creation time")
	}
	stopped.Checkpoint.Execution[0] = 'x'
	saved, _, err = store.Load(context.Background(), "unified")
	if err != nil || !json.Valid(saved.Execution) {
		t.Fatal("returned checkpoint aliases stored data")
	}
	if len(stopped.PendingInterrupts()) != 1 {
		t.Fatal("checkpoint copy mutation changed pending input")
	}
}

func TestCheckpointImportResumesPendingInput(t *testing.T) {
	g := inputGraph(t, func(ctx context.Context, _ string) (string, error) {
		v, err := graph.Interrupt(ctx, "name")
		if err != nil {
			return "", err
		}
		return v.(string), nil
	})
	source := state.NewMemoryStore()
	_, err := g.Invoke(context.Background(), "", graph.WithCheckpoint(source, "transfer"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		t.Fatal(err)
	}
	data, err := stopped.Checkpoint.JSON()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := state.DecodeCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	// 导入另一份存储时清除乐观锁标识，新存储为它分配本地记录 ID。
	imported.ID = ""
	imported.ParentID = ""
	imported.Version = 0
	target := state.NewMemoryStore()
	saved, err := target.Save(context.Background(), imported)
	if err != nil {
		t.Fatal(err)
	}
	if saved.RunID != stopped.Checkpoint.RunID {
		t.Fatal("import lost run identity")
	}
	opt := graph.WithCheckpoint(target, "transfer")
	questions, err := g.PendingInterrupts(context.Background(), opt)
	if err != nil || len(questions) != 1 {
		t.Fatalf("%v %v", questions, err)
	}
	output, err := g.Resume(context.Background(), opt, graph.WithResume(questions[0].ID, "Ada"))
	if err != nil || output != "Ada" {
		t.Fatalf("%q %v", output, err)
	}
}

func TestCheckpointRejectsInvalidExecutionBeforeRunning(t *testing.T) {
	for _, kind := range []string{"journal", "unknown-field", "old-nested-version"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			g := inputGraph(t, func(ctx context.Context, _ string) (string, error) {
				calls++
				_, err := graph.Interrupt(ctx, "approve")
				return "", err
			})
			store := state.NewMemoryStore()
			option := graph.WithCheckpoint(store, kind)
			_, err := g.Invoke(context.Background(), "", option)
			if !errors.Is(err, graph.ErrInterrupted) {
				t.Fatal(err)
			}
			saved, _, err := store.Load(context.Background(), kind)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err = json.Unmarshal(saved.Execution, &fields); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "journal":
				var records []map[string]json.RawMessage
				if err = json.Unmarshal(fields["interrupts"], &records); err != nil {
					t.Fatal(err)
				}
				records[0]["index"] = json.RawMessage("7")
				fields["interrupts"], err = json.Marshal(records)
			case "unknown-field":
				fields["unexpected"] = json.RawMessage("true")
			case "old-nested-version":
				fields["schema_version"] = json.RawMessage("4")
			}
			if err != nil {
				t.Fatal(err)
			}
			saved.Execution, err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.Save(context.Background(), saved); err != nil {
				t.Fatal(err)
			}
			if _, err = g.Resume(context.Background(), option); !errors.Is(err, state.ErrInvalidCheckpoint) {
				t.Fatalf("%v", err)
			}
			if calls != 1 {
				t.Fatal("invalid checkpoint executed a node")
			}
		})
	}
}

type legacyCheckpointStore struct{ state.Store }

func (s legacyCheckpointStore) Load(ctx context.Context, key string) (state.Checkpoint, bool, error) {
	c, ok, err := s.Store.Load(ctx, key)
	c.SchemaVersion = 2
	return c, ok, err
}

func TestResumeChecksOuterCheckpointVersion(t *testing.T) {
	store := state.NewMemoryStore()
	g := inputGraph(t, func(ctx context.Context, _ string) (string, error) {
		_, err := graph.Interrupt(ctx, "approve")
		return "", err
	})
	if _, err := g.Invoke(context.Background(), "", graph.WithCheckpoint(store, "legacy")); !errors.Is(err, graph.ErrInterrupted) {
		t.Fatal(err)
	}
	if _, err := g.Resume(context.Background(), graph.WithCheckpoint(legacyCheckpointStore{store}, "legacy")); !errors.Is(err, state.ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
}
