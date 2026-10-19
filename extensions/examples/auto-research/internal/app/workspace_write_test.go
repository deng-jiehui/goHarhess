package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/tool"
)

func TestWorkspaceWriteToolsRejectStaleFileHash(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "main.go", "old\n")
	writeTools := toolMap(NewWorkspaceWriteTools(WorkspacePolicy{Root: root}, func(context.Context, PendingWorkspaceChange) (bool, error) {
		return true, nil
	}))
	_, err := writeTools["write_file"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"main.go","content":"new\n","expected_sha256":"stale"}`})
	if err == nil || !strings.Contains(err.Error(), "文件已变化") {
		t.Fatalf("stale hash error = %v", err)
	}
	content, readErr := os.ReadFile(filepath.Join(root, "main.go"))
	if readErr != nil || string(content) != "old\n" {
		t.Fatalf("file after stale write = %q, err=%v", content, readErr)
	}
}

func TestWorkspaceWriteToolsRequiresConfirmationBeforeWrite(t *testing.T) {
	root := t.TempDir()
	writeTools := toolMap(NewWorkspaceWriteTools(WorkspacePolicy{Root: root}, func(_ context.Context, change PendingWorkspaceChange) (bool, error) {
		if change.Tool != "write_file" || len(change.Paths) != 1 {
			t.Fatalf("confirmation change = %#v", change)
		}
		return false, nil
	}))
	result, err := writeTools["write_file"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"new.txt","content":"hello\n","create":true}`})
	if err != nil {
		t.Fatal(err)
	}
	var output WriteFileResult
	decodeToolData(t, result, &output)
	if !output.ConfirmationRequired || output.Applied {
		t.Fatalf("confirmation result = %#v", output)
	}
	if _, err := os.Stat(filepath.Join(root, "new.txt")); !os.IsNotExist(err) {
		t.Fatalf("file exists after rejected confirmation, err=%v", err)
	}
}

func TestWorkspaceWriteToolsWriteFileAfterConfirmation(t *testing.T) {
	root := t.TempDir()
	writeTools := toolMap(NewWorkspaceWriteTools(WorkspacePolicy{Root: root}, func(_ context.Context, change PendingWorkspaceChange) (bool, error) {
		return change.Tool == "write_file", nil
	}))
	result, err := writeTools["write_file"].Invoke(context.Background(), tool.CallInput{RawArgs: `{"path":"new.txt","content":"hello\n","create":true}`})
	if err != nil {
		t.Fatal(err)
	}
	var output WriteFileResult
	decodeToolData(t, result, &output)
	if !output.Applied || output.ConfirmationRequired || output.SHA256 == "" {
		t.Fatalf("write result = %#v", output)
	}
	content, err := os.ReadFile(filepath.Join(root, "new.txt"))
	if err != nil || string(content) != "hello\n" {
		t.Fatalf("written content = %q, err=%v", content, err)
	}
}

func TestWorkspaceWriteToolsApplyPatchAfterValidationAndConfirmation(t *testing.T) {
	root := t.TempDir()
	writeTestWorkspaceFile(t, root, "main.go", "package main\n")
	patch := "*** Begin Patch\n*** Update File: main.go\n@@\n-package main\n+package main\n\n+// changed\n*** End Patch\n"
	// The tool accepts standard unified patches; this malformed patch must be rejected.
	writeTools := toolMap(NewWorkspaceWriteTools(WorkspacePolicy{Root: root}, func(context.Context, PendingWorkspaceChange) (bool, error) {
		t.Fatal("confirmation must not run for an invalid patch")
		return false, nil
	}))
	if _, err := writeTools["apply_patch"].Invoke(context.Background(), tool.CallInput{RawArgs: mustJSON(t, ApplyPatchArgs{Patch: patch})}); err == nil {
		t.Fatal("invalid patch was accepted")
	}

	validPatch := "diff --git a/main.go b/main.go\n--- a/main.go\n+++ b/main.go\n@@ -1 +1,3 @@\n package main\n+\n+// changed\n"
	confirmed := false
	writeTools = toolMap(NewWorkspaceWriteTools(WorkspacePolicy{Root: root}, func(_ context.Context, change PendingWorkspaceChange) (bool, error) {
		confirmed = change.Tool == "apply_patch" && strings.Contains(change.Diff, "changed")
		return confirmed, nil
	}))
	result, err := writeTools["apply_patch"].Invoke(context.Background(), tool.CallInput{RawArgs: mustJSON(t, ApplyPatchArgs{Patch: validPatch})})
	if err != nil {
		t.Fatal(err)
	}
	var output ApplyPatchResult
	decodeToolData(t, result, &output)
	if !confirmed || !output.Applied || output.ConfirmationRequired {
		t.Fatalf("patch result = %#v", output)
	}
	content, err := os.ReadFile(filepath.Join(root, "main.go"))
	if err != nil || !strings.Contains(string(content), "changed") {
		t.Fatalf("patched content = %q, err=%v", content, err)
	}
}

func TestWorkspaceWriteToolsExposeExpectedDefinitions(t *testing.T) {
	writeTools := NewWorkspaceWriteTools(WorkspacePolicy{Root: t.TempDir()}, nil)
	if len(writeTools) != 2 {
		t.Fatalf("write tool count = %d, want 2", len(writeTools))
	}
	if writeTools[0].Spec().IsReadOnly || writeTools[1].Spec().IsReadOnly {
		t.Fatal("write tools must not be marked read-only")
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func fileSHA256(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}
