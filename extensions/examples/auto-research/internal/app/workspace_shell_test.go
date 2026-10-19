package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/tool"
)

func TestWorkspaceShellToolRunsAllowedCommandInsideWorkspace(t *testing.T) {
	root := t.TempDir()
	policy := WorkspacePolicy{Root: root, Shell: "pwsh", MaxOutputBytes: 4096, MaxCommandDuration: time.Second}
	command := NewWorkspaceShellTool(policy, nil)
	result, err := command.Invoke(context.Background(), tool.CallInput{RawArgs: `{"command":"go version","cwd":"."}`})
	if err != nil {
		t.Fatal(err)
	}
	var output RunShellResult
	decodeToolData(t, result, &output)
	if output.ExitCode != 0 || !strings.Contains(output.Stdout, "go version") {
		t.Fatalf("shell result = %#v", output)
	}
	if output.ConfirmationRequired || output.Truncated {
		t.Fatalf("shell result flags = %#v", output)
	}
}

func TestWorkspaceShellToolDoesNotExecuteMutationCommand(t *testing.T) {
	root := t.TempDir()
	command := NewWorkspaceShellTool(WorkspacePolicy{Root: root, Shell: "pwsh"}, nil)
	result, err := command.Invoke(context.Background(), tool.CallInput{RawArgs: `{"command":"git apply change.patch","cwd":"."}`})
	if err != nil {
		t.Fatal(err)
	}
	var output RunShellResult
	decodeToolData(t, result, &output)
	if !output.ConfirmationRequired || output.ExitCode != -1 {
		t.Fatalf("mutation result = %#v", output)
	}
}

func TestWorkspaceShellToolRejectsBlockedCommandAndOutsideCWD(t *testing.T) {
	root := t.TempDir()
	command := NewWorkspaceShellTool(WorkspacePolicy{Root: root, Shell: "pwsh"}, nil)
	if _, err := command.Invoke(context.Background(), tool.CallInput{RawArgs: `{"command":"curl https://example.com","cwd":"."}`}); err == nil {
		t.Fatal("blocked command was accepted")
	}
	if _, err := command.Invoke(context.Background(), tool.CallInput{RawArgs: `{"command":"go version","cwd":"../outside"}`}); err == nil {
		t.Fatal("outside cwd was accepted")
	}
}

func TestWorkspaceShellToolClampsOutputAndPublishesProgress(t *testing.T) {
	root := t.TempDir()
	var progress []ResearchProgress
	command := NewWorkspaceShellTool(WorkspacePolicy{Root: root, Shell: "pwsh", MaxOutputBytes: 8}, func(_ context.Context, value ResearchProgress) error {
		progress = append(progress, value)
		return nil
	})
	result, err := command.Invoke(context.Background(), tool.CallInput{RawArgs: `{"command":"go version","cwd":"."}`})
	if err != nil {
		t.Fatal(err)
	}
	var output RunShellResult
	decodeToolData(t, result, &output)
	if !output.Truncated || len(output.Stdout) > 8 {
		t.Fatalf("output = %#v", output)
	}
	if len(progress) < 2 || progress[0].Status != "running" || progress[len(progress)-1].Status != "completed" {
		t.Fatalf("progress = %#v", progress)
	}
}

func TestWorkspaceShellToolReportsTimeout(t *testing.T) {
	root := t.TempDir()
	command := NewWorkspaceShellTool(WorkspacePolicy{Root: root, Shell: "pwsh", MaxCommandDuration: 10 * time.Millisecond}, nil)
	result, err := command.Invoke(context.Background(), tool.CallInput{RawArgs: `{"command":"go test ./...","cwd":".","timeout_ms":10}`})
	if err != nil {
		t.Fatal(err)
	}
	var output RunShellResult
	decodeToolData(t, result, &output)
	if !output.TimedOut || output.ExitCode == 0 {
		t.Fatalf("timeout result = %#v", output)
	}
}
