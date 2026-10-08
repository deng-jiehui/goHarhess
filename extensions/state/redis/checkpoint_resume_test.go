package redisstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/goharness/goharness/extensions/state/redis"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

type approvalState struct{ Text string }

func TestGraphCheckpointResumesFromNewRedisStore(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	must(err)
	g := graph.NewGraph[string, string]("approval")
	must(g.SetStateFactory(func(_ context.Context, input string) (*approvalState, error) { return &approvalState{Text: input}, nil }))
	must(g.AddNode("approve", runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, input)
		if err != nil {
			return "", err
		}
		return answer.(string), nil
	}), graph.WithPreHandler(func(_ context.Context, s *approvalState) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "approve"))
	must(g.AddEdge("approve", graph.End))
	compiled, err := g.Compile()
	must(err)
	_, err = compiled.Invoke(context.Background(), "approve?", graph.WithCheckpoint(store, "approval"))
	var paused *graph.InterruptedError
	if !errors.As(err, &paused) {
		t.Fatal(err)
	}
	if paused.Checkpoint.ID == "" || paused.Checkpoint.SchemaVersion != state.CurrentCheckpointSchemaVersion {
		t.Fatalf("checkpoint: %+v", paused.Checkpoint)
	}
	other, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{Client: client})
	must(err)
	saved, found, err := other.Get(context.Background(), "approval", paused.Checkpoint.ID)
	must(err)
	if !found || saved.RunID != paused.Checkpoint.RunID || saved.Status != state.StatusInterrupted {
		t.Fatalf("stored checkpoint: %+v", saved)
	}
	option := graph.WithCheckpoint(other, "approval")
	questions, err := compiled.PendingInterrupts(context.Background(), option)
	must(err)
	if len(questions) != 1 {
		t.Fatalf("questions: %+v", questions)
	}
	out, err := compiled.Resume(context.Background(), option, graph.WithResume(questions[0].ID, "approved"))
	must(err)
	if out != "approved" {
		t.Fatalf("output: %q", out)
	}
	final, found, err := other.Load(context.Background(), "approval")
	must(err)
	if !found || final.Status != state.StatusCompleted || final.RunID != saved.RunID {
		t.Fatalf("final checkpoint: %+v", final)
	}
}
