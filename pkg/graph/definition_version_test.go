package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

type versionedState struct {
	Text string
}

func versionedInputGraph(t *testing.T, version string) *graph.CompiledGraph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("versioned-form")
	if version != "" {
		if err := g.SetDefinitionVersion(version); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.SetStateFactory(func(_ context.Context, text string) (*versionedState, error) { return &versionedState{Text: text}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("form", runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) {
		v, err := graph.Interrupt(ctx, "name")
		if err != nil {
			return "", err
		}
		return v.(string), nil
	}), graph.WithPreHandler(func(_ context.Context, s *versionedState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *versionedState, out string) error { s.Text = out; return nil })); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "form"}, {"form", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func interruptVersionedRun(t *testing.T, g *graph.CompiledGraph[string, string], store state.Store, key string) {
	t.Helper()
	_, err := g.Invoke(context.Background(), "", graph.WithCheckpoint(store, key))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("invoke error = %v, want ErrInterrupted", err)
	}
}

func pendingName(t *testing.T, g *graph.CompiledGraph[string, string], opt runnable.Option) string {
	t.Helper()
	questions, err := g.PendingInterrupts(context.Background(), opt)
	if err != nil || len(questions) != 1 {
		t.Fatalf("%v %v", questions, err)
	}
	return questions[0].ID
}

func TestDefinitionVersionSameAllowsResume(t *testing.T) {
	store := state.NewMemoryStore()
	g := versionedInputGraph(t, "v1")
	interruptVersionedRun(t, g, store, "k")
	opt := graph.WithCheckpoint(store, "k")
	out, err := g.Resume(context.Background(), opt, graph.WithResume(pendingName(t, g, opt), "Ada"))
	if err != nil || out != "Ada" {
		t.Fatalf("%q %v", out, err)
	}
}

func TestDefinitionVersionMismatchRejectsResume(t *testing.T) {
	store := state.NewMemoryStore()
	g1 := versionedInputGraph(t, "v1")
	interruptVersionedRun(t, g1, store, "k")
	g2 := versionedInputGraph(t, "v2")
	_, err := g2.Resume(context.Background(), graph.WithCheckpoint(store, "k"))
	if !errors.Is(err, graph.ErrDefinitionVersionMismatch) {
		t.Fatalf("err = %v, want ErrDefinitionVersionMismatch", err)
	}
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("err = %v must also match ErrGraphRevisionMismatch", err)
	}
}

func TestDefinitionVersionOneSideUnsetRejectsResume(t *testing.T) {
	store := state.NewMemoryStore()
	versioned := versionedInputGraph(t, "v1")
	plain := versionedInputGraph(t, "")

	// 保存侧带版本、恢复侧未设置 → 拒绝
	interruptVersionedRun(t, versioned, store, "k1")
	if _, err := plain.Resume(context.Background(), graph.WithCheckpoint(store, "k1")); !errors.Is(err, graph.ErrDefinitionVersionMismatch) {
		t.Fatalf("saved-versioned resume err = %v", err)
	}
	// 保存侧未设置、恢复侧带版本 → 拒绝
	interruptVersionedRun(t, plain, store, "k2")
	if _, err := versioned.Resume(context.Background(), graph.WithCheckpoint(store, "k2")); !errors.Is(err, graph.ErrDefinitionVersionMismatch) {
		t.Fatalf("saved-plain resume err = %v", err)
	}
}

func TestDefinitionVersionUnsetKeepsExistingBehavior(t *testing.T) {
	store := state.NewMemoryStore()
	g := versionedInputGraph(t, "")
	interruptVersionedRun(t, g, store, "k")
	opt := graph.WithCheckpoint(store, "k")
	out, err := g.Resume(context.Background(), opt, graph.WithResume(pendingName(t, g, opt), "Ada"))
	if err != nil || out != "Ada" {
		t.Fatalf("%q %v", out, err)
	}
}

func TestSetDefinitionVersionRejectsInvalidValues(t *testing.T) {
	g := graph.NewGraph[string, string]("v")
	if err := g.SetDefinitionVersion(strings.Repeat("x", 129)); err == nil {
		t.Fatal("129-char version accepted")
	}
	if err := g.SetDefinitionVersion("a\nb"); err == nil {
		t.Fatal("version with newline accepted")
	}
	if err := g.SetDefinitionVersion(fmt.Sprintf("%128s", "v")); err != nil {
		t.Fatalf("128-char version rejected: %v", err)
	}
}
