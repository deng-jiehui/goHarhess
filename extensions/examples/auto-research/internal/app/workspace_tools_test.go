package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/tool"
)

func TestWorkspaceReadToolsListDirectorySkipsGeneratedDirectories(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "src/main.go", "package main\n")
	writeTestWorkspaceFile(t, root, "node_modules/pkg/index.js", "ignored\n")
	writeTestWorkspaceFile(t, root, ".git/config", "ignored\n")
	writeTestWorkspaceFile(t, root, "dist/index.js", "ignored\n")

	readTools := toolMap(NewWorkspaceReadTools(WorkspacePolicy{Root: root}))
	result, err := readTools["list_directory"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":".","depth":3}`})
	if err != nil {
		t.Fatal(err)
	}
	var output ListDirectoryResult
	decodeToolData(t, result, &output)
	paths := make([]string, 0, len(output.Entries))
	for _, entry := range output.Entries {
		paths = append(paths, entry.Path)
	}
	joined := strings.Join(paths, "\n")
	if !strings.Contains(joined, "src/main.go") {
		t.Fatalf("directory listing = %q, want source file", joined)
	}
	for _, excluded := range []string{"node_modules", ".git", "dist"} {
		if strings.Contains(joined, excluded) {
			t.Fatalf("directory listing contains excluded path %q: %q", excluded, joined)
		}
	}
}

func TestWorkspaceReadToolsReadFileSupportsLineRangeAndLimit(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "notes.txt", "第一行\n第二行\n第三行\n")

	readTools := toolMap(NewWorkspaceReadTools(WorkspacePolicy{Root: root, MaxReadBytes: 128}))
	result, err := readTools["read_file"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"notes.txt","start_line":2,"end_line":2}`})
	if err != nil {
		t.Fatal(err)
	}
	var output ReadFileResult
	decodeToolData(t, result, &output)
	if output.Content != "第二行\n" {
		t.Fatalf("read content = %q, want second line", output.Content)
	}
	if output.StartLine != 2 || output.EndLine != 2 {
		t.Fatalf("line range = %d-%d, want 2-2", output.StartLine, output.EndLine)
	}
}

func TestWorkspaceReadToolsReturnsFullHashWhenContentIsTruncated(t *testing.T) {
	root := t.TempDir()
	content := strings.Repeat("large-file\n", 32)
	writeTestWorkspaceFile(t, root, "large.txt", content)

	readTools := toolMap(NewWorkspaceReadTools(WorkspacePolicy{Root: root, MaxReadBytes: 8}))
	result, err := readTools["read_file"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"large.txt"}`})
	if err != nil {
		t.Fatal(err)
	}
	var output ReadFileResult
	decodeToolData(t, result, &output)
	want := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
	if !output.Truncated || output.SHA256 != want {
		t.Fatalf("truncated read = %#v, want full hash %s", output, want)
	}
}

func TestWorkspaceReadToolsRejectBinaryFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "model.bin"), []byte{0, 1, 2, 3}, 0o600); err != nil {
		t.Fatal(err)
	}
	readTools := toolMap(NewWorkspaceReadTools(WorkspacePolicy{Root: root}))
	_, err := readTools["read_file"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"model.bin"}`})
	if err == nil || !strings.Contains(err.Error(), "二进制") {
		t.Fatalf("read binary error = %v, want binary-file error", err)
	}
}

func TestWorkspaceReadToolsSearchesLiteralAndRegex(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "a.go", "func load() {}\nfunc save() {}\n")
	writeTestWorkspaceFile(t, root, "b.txt", "load is documented\n")

	readTools := toolMap(NewWorkspaceReadTools(WorkspacePolicy{Root: root}))
	for _, input := range []string{
		`{"query":"load","path":".","glob":"*.go","regex":false}`,
		`{"query":"func [a-z]+\\(\\)","path":".","glob":"*.go","regex":true}`,
	} {
		result, err := readTools["search_files"].Invoke(context.Background(), tool.CallInput{RawArgs: input})
		if err != nil {
			t.Fatal(err)
		}
		var output SearchFilesResult
		decodeToolData(t, result, &output)
		if len(output.Matches) == 0 || output.Matches[0].Path != "a.go" {
			t.Fatalf("search result = %#v, want a.go match", output)
		}
	}
}

func TestWorkspaceReadToolsGitDiffIsScopedToWorkspace(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "README.md", "before\n")
	runGit(t, root, "init")
	runGit(t, root, "config", "user.email", "test@example.com")
	runGit(t, root, "config", "user.name", "AutoResearch Test")
	runGit(t, root, "add", "README.md")
	runGit(t, root, "commit", "-m", "initial")
	writeTestWorkspaceFile(t, root, "README.md", "after\n")

	readTools := toolMap(NewWorkspaceReadTools(WorkspacePolicy{Root: root}))
	result, err := readTools["git_diff"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"README.md"}`})
	if err != nil {
		t.Fatal(err)
	}
	var output GitDiffResult
	decodeToolData(t, result, &output)
	if !strings.Contains(output.Patch, "+after") || strings.Contains(output.Patch, "../") {
		t.Fatalf("git diff = %q", output.Patch)
	}
	if _, err := readTools["git_diff"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"../outside"}`}); err == nil {
		t.Fatal("git_diff accepted a path outside workspace")
	}
}

func TestWorkspaceReadToolsExposeOnlyExpectedDefinitions(t *testing.T) {
	readTools := NewWorkspaceReadTools(WorkspacePolicy{Root: t.TempDir()})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: readTools})
	if err != nil {
		t.Fatal(err)
	}
	if len(node.Definitions()) != 4 {
		t.Fatalf("tool definitions = %d, want 4", len(node.Definitions()))
	}
}

func toolMap(tools []tool.Tool) map[string]tool.Tool {
	result := make(map[string]tool.Tool, len(tools))
	for _, value := range tools {
		result[value.Spec().Name] = value
	}
	return result
}

func decodeToolData(t *testing.T, result tool.Result, target any) {
	t.Helper()
	if result.IsError {
		t.Fatalf("tool result is error: %#v", result)
	}
	if result.Data == nil {
		t.Fatal("tool result has no data")
	}
	data, err := json.Marshal(result.Data)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatal(err)
	}
}

func writeTestWorkspaceFile(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}

var _ = errors.Is
