package app

import (
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
)

func TestWorkspaceToolEventsSummarizeFunctionResults(t *testing.T) {
	message := schema.NewMessage(schema.RoleTool,
		schema.NewFunctionToolResultBlock("call-1", "read_file", false, schema.NewTextBlock("README content")),
		schema.NewFunctionToolResultBlock("call-2", "run_shell", true, schema.NewTextBlock("command failed")),
	)
	events := workspaceToolEvents(message)
	if len(events) != 2 {
		t.Fatalf("工具事件数量=%d，期望 2", len(events))
	}
	if events[0].Tool != "read_file" || events[0].Status != "completed" || events[0].Preview != "README content" {
		t.Fatalf("成功工具事件=%+v", events[0])
	}
	if events[1].Tool != "run_shell" || events[1].Status != "failed" || !events[1].IsError || !strings.Contains(events[1].Preview, "command failed") {
		t.Fatalf("失败工具事件=%+v", events[1])
	}
}
