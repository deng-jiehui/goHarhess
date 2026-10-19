package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/schema"
)

// ResearchState 是 AutoResearch 图的业务状态。它只保存可恢复的业务数据，
// 数据库连接、模型和工具由图构造器闭包持有。
type ResearchState struct {
	History       []schema.Message  `json:"history"`
	UserMessage   schema.Message    `json:"user_message"`
	InputText     string            `json:"input_text"`
	Project       domain.Project    `json:"project"`
	Notes         []domain.Note     `json:"notes"`
	Route         string            `json:"route,omitempty"`
	Scope         string            `json:"scope,omitempty"`
	Answer        string            `json:"answer,omitempty"`
	Question      string            `json:"question,omitempty"`
	GoalError     string            `json:"goal_error,omitempty"`
	GoalRepairs   int               `json:"goal_repairs"`
	DraftMessages []schema.Message  `json:"draft_messages,omitempty"`
	Interaction   InteractionState  `json:"interaction"`
	SaveRequested bool              `json:"save_requested"`
	Draft         string            `json:"draft,omitempty"`
	NoteCompacted bool              `json:"note_compacted"`
	Note          domain.Note       `json:"note"`
	NoteSaved     bool              `json:"note_saved"`
	Output        []schema.Message  `json:"output,omitempty"`
	Workspace     WorkspaceRunState `json:"workspace,omitempty"`
}

// WorkspaceRunState 是工作区子图的可恢复状态。模型可以选择工具，
// 但业务图仍负责循环次数、确认和结束条件。
type WorkspaceRunState struct {
	Messages  []schema.Message        `json:"messages,omitempty"`
	Rounds    int                     `json:"rounds"`
	Calls     int                     `json:"calls"`
	Pending   *PendingWorkspaceChange `json:"pending,omitempty"`
	Completed bool                    `json:"completed"`
}

type GoalDecision struct {
	Route string `json:"route"`
	Scope string `json:"scope"`
	Save  bool   `json:"save"`
}

type BusinessStepResult struct{}

func newResearchState(_ context.Context, input []schema.Message) (*ResearchState, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("研究输入不能为空")
	}
	cloned := schema.CloneMessages(input)
	text := schema.LastUserText(cloned)
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("研究目标不能为空")
	}
	return &ResearchState{
		History:     cloned[:max(0, len(cloned)-1)],
		UserMessage: cloned[len(cloned)-1],
		InputText:   text,
		Notes:       []domain.Note{},
	}, nil
}

func newResearchStateCodec() (graph.StateCodec, error) {
	return graph.NewStateCodec[*ResearchState]("autoresearch:research-state", 2,
		func(_ context.Context, state *ResearchState) ([]byte, error) {
			return json.Marshal(state)
		},
		func(_ context.Context, data []byte) (*ResearchState, error) {
			var state ResearchState
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&state); err != nil {
				return nil, err
			}
			if err := decoder.Decode(&struct{}{}); err != io.EOF {
				if err == nil {
					return nil, fmt.Errorf("状态包含多个 JSON 值")
				}
				return nil, err
			}
			return &state, nil
		},
	)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
