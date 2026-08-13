package graph_test

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

type streamState struct {
	Messages []schema.Message
	Output   []schema.Message
}

func newStreamGraph(t *testing.T, name string) *graph.Graph[[]schema.Message, []schema.Message] {
	t.Helper()
	g := graph.NewGraph[[]schema.Message, []schema.Message](name)
	if err := g.SetStateFactory(func(_ context.Context, input []schema.Message) (*streamState, error) {
		return &streamState{Messages: schema.CloneMessages(input)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

func addStreamNode(t *testing.T, g *graph.Graph[[]schema.Message, []schema.Message], name string, node runnable.Runnable[*streamState, *streamState]) {
	t.Helper()
	if err := g.AddNode(name, node,
		graph.WithPreHandler(func(_ context.Context, current *streamState) (*streamState, error) { return current, nil }),
		graph.WithPostHandler(func(_ context.Context, current *streamState, output *streamState) error {
			if current != output {
				*current = *output
			}
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
}

func addStreamFinish(t *testing.T, g *graph.Graph[[]schema.Message, []schema.Message], from string) {
	t.Helper()
	finish := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, output []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(output), nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *streamState) ([]schema.Message, error) {
		return schema.CloneMessages(current.Output), nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(from, "finish"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		t.Fatal(err)
	}
}

func TestMessageStreamSeparatesDataAndControlPlanes(t *testing.T) {
	g := newStreamGraph(t, "planes")
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventDelta, Messages: []schema.Message{schema.AssistantMessage("hel")}})
		message := schema.AssistantMessage("hello")
		current.Messages = append(current.Messages, message)
		current.Output = append(current.Output, message)
		return current, nil
	})
	addStreamNode(t, g, "model", node)
	_ = g.AddEdge(graph.Start, "model")
	addStreamFinish(t, g, "model")
	var mu sync.Mutex
	var control []graph.EventType
	ctx := graph.WithEventSink(context.Background(), graph.EventSinkFunc(func(_ context.Context, event graph.Event) {
		mu.Lock()
		control = append(control, event.Type)
		mu.Unlock()
	}))
	stream, err := graph.StreamMessages(ctx, g, []schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	var data []schema.MessageEventKind
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		data = append(data, item.Value.Kind)
	}
	if fmt.Sprint(data) != fmt.Sprint([]schema.MessageEventKind{schema.MessageEventDelta, schema.MessageEventSnapshot, schema.MessageEventDone}) {
		t.Fatalf("data=%#v", data)
	}
	mu.Lock()
	defer mu.Unlock()
	if control[0] != graph.EventGraphStart || control[len(control)-1] != graph.EventGraphEnd {
		t.Fatalf("control=%#v", control)
	}
}

func TestStreamDoesNotDuplicateAuthoritativeSnapshot(t *testing.T) {
	g := newStreamGraph(t, "snapshot")
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		current.Output = []schema.Message{schema.AssistantMessage("answer")}
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: current.Output})
		return current, nil
	})
	addStreamNode(t, g, "model", node)
	_ = g.AddEdge(graph.Start, "model")
	addStreamFinish(t, g, "model")
	stream, err := graph.StreamMessages(context.Background(), g, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := 0
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Value.Kind == schema.MessageEventSnapshot {
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Fatalf("snapshots=%d", snapshots)
	}
}

func TestStreamEmitsEmptyAuthoritativeFinalSnapshot(t *testing.T) {
	g := newStreamGraph(t, "empty-final")
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: []schema.Message{schema.AssistantMessage("stale")}})
		return current, nil
	})
	addStreamNode(t, g, "model", node)
	_ = g.AddEdge(graph.Start, "model")
	addStreamFinish(t, g, "model")
	stream, err := graph.StreamMessages(context.Background(), g, nil)
	if err != nil {
		t.Fatal(err)
	}
	var snapshots [][]schema.Message
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Value.Kind == schema.MessageEventSnapshot {
			snapshots = append(snapshots, item.Value.Messages)
		}
	}
	if len(snapshots) != 2 || len(snapshots[0]) != 1 || len(snapshots[1]) != 0 {
		t.Fatalf("snapshots=%#v", snapshots)
	}
}

func TestCursorResumeUsesRuntimeStatePreHandler(t *testing.T) {
	g := newStreamGraph(t, "resume")
	depth := 0
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		depth = len(state.CursorStackFromContext(ctx))
		current.Output = []schema.Message{schema.AssistantMessage("resumed")}
		return current, nil
	})
	addStreamNode(t, g, "work", node)
	_ = g.AddEdge(graph.Start, "work")
	addStreamFinish(t, g, "work")
	ctx := state.WithCursorStack(context.Background(), state.CursorStack{{Graph: "resume", Node: "work", Step: 0, Iteration: 2}})
	output, err := g.Invoke(ctx, nil)
	if err != nil || depth != 1 || len(output) != 1 {
		t.Fatalf("depth=%d output=%#v err=%v", depth, output, err)
	}
}

func TestControlEventDoesNotExposeState(t *testing.T) {
	if _, exists := reflect.TypeOf(graph.Event{}).FieldByName("State"); exists {
		t.Fatal("control Event exposes runtime State")
	}
}

type childRequest struct{ Text string }
type childResponse struct{ Text string }
type childGraphState struct {
	Text   string
	Visits int
}

func TestAddNodeMountsChildGraphWithStateMappings(t *testing.T) {
	child := graph.NewGraph[childRequest, childResponse]("child")
	if err := child.SetStateFactory(func(_ context.Context, input childRequest) (*childGraphState, error) {
		return &childGraphState{Text: input.Text, Visits: 100}, nil
	}); err != nil {
		t.Fatal(err)
	}
	childNode := runnable.Func[string, childResponse](func(_ context.Context, value string, _ ...runnable.Option) (childResponse, error) {
		return childResponse{Text: value + ":child"}, nil
	})
	if err := child.AddNode("work", childNode, graph.WithPreHandler(func(_ context.Context, current *childGraphState) (string, error) {
		current.Visits++
		return current.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = child.AddEdge(graph.Start, "work")
	_ = child.AddEdge("work", graph.End)

	parent := newTypedGraph[string, string](t, "parent", func(_ context.Context, input string) (*typedState, error) { return &typedState{Text: input}, nil })
	if err := parent.AddNode("child", child,
		graph.WithPreHandler(func(_ context.Context, current *typedState) (childRequest, error) {
			return childRequest{Text: current.Text}, nil
		}),
		graph.WithPostHandler(func(_ context.Context, current *typedState, output childResponse) error {
			current.History = append(current.History, output.Text)
			current.Count++
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, value string, _ ...runnable.Option) (string, error) { return value, nil })
	if err := parent.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *typedState) (string, error) {
		return fmt.Sprintf("%d:%s", current.Count, current.History[0]), nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "child"}, {"child", "finish"}, {"finish", graph.End}} {
		_ = parent.AddEdge(edge[0], edge[1])
	}
	output, err := parent.Invoke(context.Background(), "parent")
	if err != nil || output != "1:parent:child" {
		t.Fatalf("output=%q err=%v", output, err)
	}
}

func TestRecoveryArchitectureBoundaries(t *testing.T) {
	root := repositoryRoot(t)
	forbidden := map[string]struct{}{
		"Breakpointer": {}, "InterruptSignal": {}, "InterruptDecision": {},
		"WithRunBreakpointer": {}, "WithInterruptSignal": {}, "EngineCheckpoint": {},
		"LeaseProvider": {}, "FencingToken": {}, "WithGraphStateCheckpoint": {},
		"GraphStateCheckpointFromContext": {},
	}

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == "docs" || name == ".worktrees" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || filepath.Base(path) == "architecture_test.go" || len(path) >= len("_test.go") && path[len(path)-len("_test.go"):] == "_test.go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if _, blocked := forbidden[identifier.Name]; blocked {
				t.Errorf("forbidden recovery identifier %q in %s", identifier.Name, path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertPackageImportsNoProjectPackage(t, filepath.Join(root, "pkg", "control"))
	assertPackageDoesNotImport(t, filepath.Join(root, "pkg", "graph"), "goharness/pkg/tool")
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate architecture test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(current), "..", ".."))
}

func assertPackageImportsNoProjectPackage(t *testing.T, directory string) {
	t.Helper()
	assertImports(t, directory, func(path string) bool {
		return len(path) >= len("goharness/") && path[:len("goharness/")] == "goharness/"
	})
}

func assertPackageDoesNotImport(t *testing.T, directory, forbidden string) {
	t.Helper()
	assertImports(t, directory, func(path string) bool { return path == forbidden })
}

func assertImports(t *testing.T, directory string, forbidden func(string) bool) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || len(entry.Name()) >= len("_test.go") && entry.Name()[len(entry.Name())-len("_test.go"):] == "_test.go" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			value := imported.Path.Value
			if len(value) >= 2 {
				value = value[1 : len(value)-1]
			}
			if forbidden(value) {
				t.Errorf("forbidden import %q in %s", value, path)
			}
		}
	}
}
