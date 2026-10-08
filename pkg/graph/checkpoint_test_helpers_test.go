package graph_test

import (
	"encoding/json"
	"testing"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/state"
)

// checkpointProgress 仅用于断言检查点中的调度数据，不是框架的公开恢复接口。
type checkpointProgress struct {
	Schedule      *graph.StepSchedule        `json:"schedule,omitempty"`
	Interrupts    []control.InterruptRecord  `json:"interrupts,omitempty"`
	GraphName     string                     `json:"graph_name"`
	GraphRevision string                     `json:"graph_revision"`
	ResumeNode    string                     `json:"resume_node"`
	Step          int                        `json:"step"`
	Visits        map[string]int             `json:"visits,omitempty"`
	State         graph.GraphStateCheckpoint `json:"state"`
	CursorStack   graph.CursorStack          `json:"cursor_stack,omitempty"`
	Point         graph.InterruptPoint       `json:"point"`
	Reason        string                     `json:"reason,omitempty"`
	Metadata      map[string]any             `json:"metadata,omitempty"`
	// Tasks 只投影恢复判定需要的字段，不是执行树的完整镜像。
	Tasks []struct {
		Node   string `json:"node"`
		Status string `json:"status"`
	} `json:"tasks,omitempty"`
}

func decodeProgress(data []byte) (checkpointProgress, error) {
	var p checkpointProgress
	err := json.Unmarshal(data, &p)
	return p, err
}
func inspectCheckpoint(t *testing.T, c state.Checkpoint) checkpointProgress {
	t.Helper()
	p, err := decodeProgress(c.Execution)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (p checkpointProgress) JSON() ([]byte, error) { return json.Marshal(p) }
