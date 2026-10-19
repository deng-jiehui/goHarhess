package tool_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func TestFileArtifactStoreRoundTrip(t *testing.T) {
	store, err := tool.NewFileArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Save(context.Background(), tool.Artifact{ID: "call/1", ToolName: "search", CallID: "call-1", Data: []byte("0123456789")})
	if err != nil {
		t.Fatal(err)
	}
	if ref.ID != "call/1" || ref.Size != 10 || ref.SHA256 == "" {
		t.Fatalf("ref=%#v", ref)
	}
	if retry, err := store.Save(context.Background(), tool.Artifact{
		ID:       ref.ID,
		ToolName: "search",
		CallID:   "call-1",
		Data:     []byte("0123456789"),
	}); err != nil || retry.SHA256 != ref.SHA256 {
		t.Fatalf("idempotent retry ref=%#v err=%v", retry, err)
	}
	data, err := store.Read(context.Background(), ref.ID, 3, 4)
	if err != nil || string(data) != "3456" {
		t.Fatalf("data=%q err=%v", data, err)
	}
}

func TestFileArtifactStoreRejectsConflictingRewrite(t *testing.T) {
	store, err := tool.NewFileArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first := tool.Artifact{ID: "stable", ToolName: "search", Data: []byte("first")}
	if _, err := store.Save(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	_, err = store.Save(context.Background(), tool.Artifact{
		ID:       first.ID,
		ToolName: first.ToolName,
		Data:     []byte("different"),
	})
	if !errors.Is(err, tool.ErrArtifactStore) {
		t.Fatalf("err=%v, want ErrArtifactStore", err)
	}
	data, err := store.Read(context.Background(), first.ID, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "first" {
		t.Fatalf("stored data=%q, want original artifact", data)
	}
}

func TestToolsNodeExternalizesLargeResultIntoSummaryCard(t *testing.T) {
	store, err := tool.NewFileArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "search"}, func(context.Context, tool.CallInput) (tool.Result, error) {
			return tool.TextResult(strings.Repeat("result ", 100)), nil
		})},
		ArtifactStore:          store,
		ArtifactThresholdBytes: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(context.Background(), []schema.Message{
		schema.UserMessage("search"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "search", `{}`)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 {
		t.Fatalf("output=%#v", output)
	}
	text := output[0].FunctionToolResults()[0].ContentBlocks[0].Text.Text
	if !strings.Contains(text, "[tool result archived]") || !strings.Contains(text, "artifact_id:") || len(text) >= len(strings.Repeat("result ", 100)) {
		t.Fatalf("summary card=%q", text)
	}
}

type failingArtifactStore struct{}

func (failingArtifactStore) Save(context.Context, tool.Artifact) (tool.ArtifactRef, error) {
	return tool.ArtifactRef{}, errors.New("disk unavailable")
}

func (failingArtifactStore) Read(context.Context, string, int, int) ([]byte, error) {
	return nil, errors.New("not supported")
}

func TestToolsNodeReturnsArtifactStoreErrorWithoutSuccessCard(t *testing.T) {
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "search"}, func(context.Context, tool.CallInput) (tool.Result, error) {
			return tool.TextResult(strings.Repeat("result ", 100)), nil
		})},
		ArtifactStore:          failingArtifactStore{},
		ArtifactThresholdBytes: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(context.Background(), []schema.Message{
		schema.UserMessage("search"),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "search", `{}`)),
	})
	if !errors.Is(err, tool.ErrArtifactStore) {
		t.Fatalf("err=%v, want ErrArtifactStore", err)
	}
	if output != nil {
		t.Fatalf("output=%#v, want no successful tool result", output)
	}
}

func TestReadArtifactToolReadsByRange(t *testing.T) {
	store, err := tool.NewFileArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Save(context.Background(), tool.Artifact{ID: "one", ToolName: "search", Data: []byte("abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	reader := tool.NewReadArtifactTool(store)
	result, err := reader.Invoke(context.Background(), tool.CallInput{Args: map[string]any{
		"artifact_id": ref.ID,
		"offset":      1,
		"limit":       3,
	}})
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := result.ToContentBlocks()
	if err != nil || len(blocks) != 1 || blocks[0].Text.Text != "bcd" {
		t.Fatalf("blocks=%#v err=%v", blocks, err)
	}
}
