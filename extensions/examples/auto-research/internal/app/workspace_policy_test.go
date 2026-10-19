package app

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestWorkspacePolicyResolvePathAcceptsNestedFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "src", "main.go")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	resolved, err := (WorkspacePolicy{Root: root}).ResolvePath("src/main.go")
	if err != nil {
		t.Fatalf("ResolvePath() error = %v", err)
	}
	if resolved != path {
		t.Fatalf("ResolvePath() = %q, want %q", resolved, path)
	}
}

func TestWorkspacePolicyResolvePathRejectsEscape(t *testing.T) {
	root := t.TempDir()
	policy := WorkspacePolicy{Root: root}

	for _, value := range []string{"../outside.txt", filepath.Join(root, "outside.txt"), "src/../../outside.txt"} {
		if _, err := policy.ResolvePath(value); err == nil {
			t.Errorf("ResolvePath(%q) succeeded, want workspace escape error", value)
		}
	}
}

func TestWorkspacePolicyResolvePathRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "linked")
	if err := os.Symlink(outside, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symbolic links unavailable: %v", err)
		}
		t.Fatal(err)
	}
	if _, err := (WorkspacePolicy{Root: root}).ResolvePath("linked/secret.txt"); err == nil {
		t.Fatal("ResolvePath() accepted a path through an escaping symlink")
	}
}

func TestWorkspacePolicyRejectsInvalidRoot(t *testing.T) {
	if _, err := (WorkspacePolicy{}).ResolvePath("file.txt"); err == nil {
		t.Fatal("ResolvePath() accepted an empty workspace root")
	}
	if _, err := (WorkspacePolicy{Root: filepath.Join(t.TempDir(), "missing")}).ResolvePath("file.txt"); err == nil {
		t.Fatal("ResolvePath() accepted a missing workspace root")
	}
}

func TestWorkspacePolicyValidatesShellCommandAndTimeout(t *testing.T) {
	policy := WorkspacePolicy{Root: t.TempDir(), MaxCommandDuration: 5 * time.Second}
	for _, command := range []string{"go test ./...", "go build ./...", "npm test", "git diff --stat", "rg -n graph ."} {
		if err := policy.ValidateCommand(command, "."); err != nil {
			t.Errorf("ValidateCommand(%q) error = %v", command, err)
		}
	}
	if err := policy.ValidateTimeout(6000); err == nil {
		t.Fatal("ValidateTimeout() accepted a timeout above the policy limit")
	}
}

func TestWorkspacePolicyRejectsDangerousShellCommand(t *testing.T) {
	policy := WorkspacePolicy{Root: t.TempDir(), MaxCommandDuration: time.Minute}
	for _, command := range []string{"rm -rf .", "Remove-Item -Recurse .", "curl https://example.com", "npm install"} {
		err := policy.ValidateCommand(command, ".")
		if err == nil {
			t.Errorf("ValidateCommand(%q) succeeded, want rejection", command)
			continue
		}
		if errors.Is(err, ErrWorkspacePath) {
			t.Errorf("ValidateCommand(%q) returned path error: %v", command, err)
		}
	}
}

func TestWorkspacePolicyClassifiesMutationAndConfirmation(t *testing.T) {
	policy := WorkspacePolicy{Root: t.TempDir(), MaxCommandDuration: time.Minute}
	if !policy.IsMutation("git apply change.patch") {
		t.Fatal("IsMutation(git apply) = false, want true")
	}
	if policy.IsMutation("go test ./...") {
		t.Fatal("IsMutation(go test) = true, want false")
	}
	if !strings.Contains(policy.ValidateCommand("python script.py", ".").Error(), "确认") {
		t.Fatal("unknown command error does not explain confirmation requirement")
	}
}
