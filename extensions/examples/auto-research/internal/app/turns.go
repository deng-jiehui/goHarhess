package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/stream"
)

const researchDefinitionVersion = "autoresearch-phase2-graph-v2"

type SendInput struct {
	Text           string `json:"text"`
	IdempotencyKey string `json:"idempotency_key"`
}

type SendResult struct {
	Turn     domain.Turn `json:"turn"`
	Replayed bool        `json:"replayed"`
}

type AnswerInput struct {
	InterruptID string `json:"interrupt_id"`
	Text        string `json:"text"`
}

type ResumeInput struct {
	Revision       int64         `json:"revision"`
	IdempotencyKey string        `json:"idempotency_key"`
	Answers        []AnswerInput `json:"answers"`
}

type TurnActionInput struct {
	Revision       int64  `json:"revision"`
	IdempotencyKey string `json:"idempotency_key"`
}

type TurnActionResult struct {
	Turn     domain.Turn `json:"turn"`
	ActionID string      `json:"action_id,omitempty"`
	Replayed bool        `json:"replayed"`
}

type turnContext struct {
	Project     domain.Project   `json:"project"`
	Notes       []domain.Note    `json:"notes"`
	History     []schema.Message `json:"history"`
	UserMessage schema.Message   `json:"user_message"`
}

type TurnService struct {
	runtime           domain.RuntimeRepository
	projects          domain.ProjectRepository
	sessions          domain.SessionRepository
	checkpoints       state.Store
	checkpointFactory func(domain.Scope, string, string) state.Store
	model             llm.ChatModel
	modelSettings     ModelSettings
	workspacePolicy   WorkspacePolicy
	runTimeout        time.Duration
	mu                sync.Mutex
	answers           map[string][]AnswerInput
	wake              chan struct{}
	stop              chan struct{}
	done              chan struct{}
}

func NewTurnService(runtime domain.RuntimeRepository, projects domain.ProjectRepository, sessions domain.SessionRepository, models ...llm.ChatModel) *TurnService {
	var model llm.ChatModel
	if len(models) > 0 {
		model = models[0]
	}
	return &TurnService{
		runtime: runtime, projects: projects, sessions: sessions,
		model: model, modelSettings: ModelSettings{Mode: "fixture", Configured: true}, runTimeout: 5 * time.Minute,
		checkpoints: state.NewMemoryStore(), checkpointFactory: func(domain.Scope, string, string) state.Store { return nil }, answers: make(map[string][]AnswerInput), wake: make(chan struct{}, 1),
	}
}

// ModelSettings 返回当前模型的非敏感状态。
func (s *TurnService) ModelSettings() ModelSettings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.modelSettings
}

// ConfigureModel 在本地进程内切换模型。API Key 不会进入持久化层。
func (s *TurnService) ConfigureModel(input ModelConfigInput) (ModelSettings, error) {
	model, settings, err := buildChatModel(input)
	if err != nil {
		return ModelSettings{}, err
	}
	s.mu.Lock()
	s.model = model
	s.modelSettings = settings
	s.mu.Unlock()
	s.signalWake()
	return settings, nil
}

// SetWorkspacePolicy 配置 Agent 可访问的本地工作区。策略只保存在进程内，
// 不进入项目、会话或模型上下文持久化数据。
func (s *TurnService) SetWorkspacePolicy(policy WorkspacePolicy) {
	s.mu.Lock()
	s.workspacePolicy = policy
	s.mu.Unlock()
}

func (s *TurnService) signalWake() {
	if s.wake != nil {
		signal(s.wake)
	}
}

// SetCheckpointFactory 将检查点存储交给基础设施层注入；未注入时使用本地
// 内存存储，便于离线演示和单元测试。
func (s *TurnService) SetCheckpointFactory(factory func(domain.Scope, string, string) state.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if factory != nil {
		s.checkpointFactory = factory
	}
}

func (s *TurnService) Send(ctx context.Context, scope domain.Scope, sessionID string, in SendInput) (SendResult, error) {
	if strings.TrimSpace(in.Text) == "" || len([]rune(in.Text)) > 8000 || strings.TrimSpace(in.IdempotencyKey) == "" || len(in.IdempotencyKey) > 128 {
		return SendResult{}, fmt.Errorf("%w：消息和幂等键不合法", domain.ErrInvalid)
	}
	project, err := s.projects.GetProject(ctx, scope)
	if err != nil {
		return SendResult{}, err
	}
	if s.sessions != nil {
		if _, err = s.sessions.GetSession(ctx, scope, sessionID); err != nil {
			return SendResult{}, err
		}
	}
	notes, err := s.runtime.ListNotes(ctx, scope, 20)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return SendResult{}, err
	}
	historyRecords, err := s.runtime.ListMessages(ctx, scope, sessionID, 100)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return SendResult{}, err
	}
	history := decodeCommittedMessages(historyRecords)
	user := schema.UserMessage(in.Text)
	encoded, err := json.Marshal(turnContext{Project: project, Notes: notes, History: history, UserMessage: user})
	if err != nil {
		return SendResult{}, err
	}
	admitted, err := s.runtime.AdmitTurn(ctx, scope, sessionID, domain.TurnDraft{InputText: in.Text, IdempotencyKey: in.IdempotencyKey, DefinitionVersion: researchDefinitionVersion, ContextJSON: encoded})
	if err != nil {
		return SendResult{}, err
	}
	if !admitted.Replayed {
		content, _ := json.Marshal(user)
		if err = s.runtime.SaveMessages(ctx, scope, sessionID, 0, []domain.MessageRecord{{SessionID: sessionID, TurnID: admitted.Turn.ID, ContentJSON: content, Committed: true}}); err != nil {
			return SendResult{}, err
		}
		_, _ = s.runtime.AppendEvent(ctx, scope, domain.SessionEvent{SessionID: sessionID, TurnID: admitted.Turn.ID, Kind: "turn.accepted", PayloadJSON: encoded})
		signal(s.wake)
	}
	return SendResult{Turn: admitted.Turn, Replayed: admitted.Replayed}, nil
}

func (s *TurnService) Get(ctx context.Context, scope domain.Scope, sessionID, turnID string) (domain.Turn, error) {
	return s.runtime.GetTurn(ctx, scope, sessionID, turnID)
}

func (s *TurnService) Snapshot(ctx context.Context, scope domain.Scope, sessionID string, limit int) (domain.TurnSnapshot, error) {
	return s.runtime.Snapshot(ctx, scope, sessionID, limit)
}

func (s *TurnService) Events(ctx context.Context, scope domain.Scope, sessionID string, after int64, limit int) ([]domain.SessionEvent, error) {
	return s.runtime.ListEvents(ctx, scope, sessionID, after, limit)
}

func (s *TurnService) Notes(ctx context.Context, scope domain.Scope, limit int) ([]domain.Note, error) {
	return s.runtime.ListNotes(ctx, scope, limit)
}

func (s *TurnService) DeleteNote(ctx context.Context, scope domain.Scope, noteID string) error {
	if strings.TrimSpace(noteID) == "" {
		return domain.ErrInvalid
	}
	return s.runtime.DeleteNote(ctx, scope, noteID)
}

func (s *TurnService) Resume(ctx context.Context, scope domain.Scope, sessionID, turnID string, in ResumeInput) (TurnActionResult, error) {
	if len(in.Answers) == 0 || strings.TrimSpace(in.IdempotencyKey) == "" || in.Revision < 1 {
		return TurnActionResult{}, fmt.Errorf("%w：回答和版本不能为空", domain.ErrInvalid)
	}
	payload, _ := json.Marshal(in.Answers)
	result, err := s.runtime.ApplyAction(ctx, scope, sessionID, turnID, domain.ActionDraft{Kind: "resume", IdempotencyKey: in.IdempotencyKey, RequestHash: hash(payload), PayloadJSON: payload, Revision: in.Revision})
	if err != nil {
		return TurnActionResult{}, err
	}
	if !result.Replayed {
		s.mu.Lock()
		s.answers[turnID] = append([]AnswerInput(nil), in.Answers...)
		s.mu.Unlock()
		signal(s.wake)
	}
	return TurnActionResult{Turn: result.Turn, ActionID: result.Action.ID, Replayed: result.Replayed}, nil
}

func (s *TurnService) Retry(ctx context.Context, scope domain.Scope, sessionID, turnID string, in TurnActionInput) (TurnActionResult, error) {
	if in.Revision < 1 || strings.TrimSpace(in.IdempotencyKey) == "" {
		return TurnActionResult{}, domain.ErrInvalid
	}
	result, err := s.runtime.ApplyAction(ctx, scope, sessionID, turnID, domain.ActionDraft{Kind: "retry", IdempotencyKey: in.IdempotencyKey, RequestHash: hash([]byte("retry")), PayloadJSON: []byte(`{}`), Revision: in.Revision})
	if err != nil {
		return TurnActionResult{}, err
	}
	signal(s.wake)
	return TurnActionResult{Turn: result.Turn, ActionID: result.Action.ID, Replayed: result.Replayed}, nil
}

func (s *TurnService) Cancel(ctx context.Context, scope domain.Scope, sessionID, turnID string, in TurnActionInput) (TurnActionResult, error) {
	if in.Revision < 1 || strings.TrimSpace(in.IdempotencyKey) == "" {
		return TurnActionResult{}, domain.ErrInvalid
	}
	result, err := s.runtime.ApplyAction(ctx, scope, sessionID, turnID, domain.ActionDraft{Kind: "cancel", IdempotencyKey: in.IdempotencyKey, RequestHash: hash([]byte("cancel")), PayloadJSON: []byte(`{}`), Revision: in.Revision})
	if err != nil {
		return TurnActionResult{}, err
	}
	return TurnActionResult{Turn: result.Turn, ActionID: result.Action.ID, Replayed: result.Replayed}, nil
}

// RunQueued 执行一个排队 Turn，便于服务 worker 和集成测试共用同一条路径。
func (s *TurnService) RunQueued(ctx context.Context) (bool, error) {
	turn, found, err := s.runtime.ClaimQueued(ctx)
	if err != nil || !found {
		return found, err
	}
	return true, s.runTurn(ctx, turn)
}

func (s *TurnService) Start(ctx context.Context) {
	s.mu.Lock()
	if s.done != nil {
		s.mu.Unlock()
		return
	}
	s.stop, s.done = make(chan struct{}), make(chan struct{})
	stop, done := s.stop, s.done
	s.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			_, _ = s.RunQueued(ctx)
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
			case <-s.wake:
			}
		}
	}()
}

func (s *TurnService) Stop() {
	s.mu.Lock()
	stop, done := s.stop, s.done
	s.stop, s.done = nil, nil
	s.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
}

func (s *TurnService) runTurn(ctx context.Context, turn domain.Turn) error {
	scope := domain.Scope{UserID: turn.OwnerID, ProjectID: turn.ProjectID}
	if turn.DefinitionVersion != researchDefinitionVersion {
		return s.failTurn(ctx, scope, turn, "recovery_required", "本轮使用旧版研究流程，无法套用新版中断协议；请结束旧轮次后重新发送。项目资料和历史消息仍保留。")
	}
	if err := s.runtime.SetTurnState(ctx, scope, turn.ID, domain.TurnRunning, "", ""); err != nil {
		return err
	}
	var tc turnContext
	if err := json.Unmarshal(turn.ContextJSON, &tc); err != nil {
		_ = s.runtime.SetTurnState(ctx, scope, turn.ID, domain.TurnFailed, "invalid_context", err.Error())
		return err
	}
	s.mu.Lock()
	model := s.model
	workspacePolicy := s.workspacePolicy
	s.mu.Unlock()
	deps := ResearchGraphDeps{Scope: scope, SessionID: turn.SessionID, TurnID: turn.ID, Project: tc.Project, Notes: tc.Notes, Model: model, WorkspacePolicy: workspacePolicy,
		SaveNote: func(saveCtx context.Context, note domain.Note) (domain.Note, error) {
			return s.runtime.SaveNote(saveCtx, scope, note)
		}}
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		_ = s.runtime.SetTurnState(ctx, scope, turn.ID, domain.TurnFailed, "graph_build_failed", err.Error())
		return err
	}
	graphKey := "autoresearch:" + turn.ID
	s.mu.Lock()
	checkpointStore := s.checkpoints
	if s.checkpointFactory != nil {
		if injected := s.checkpointFactory(scope, turn.SessionID, turn.ID); injected != nil {
			checkpointStore = injected
		}
	}
	s.mu.Unlock()
	options := []runnable.Option{graph.WithCheckpoint(checkpointStore, graphKey), graph.WithDurability(graph.DurabilitySync)}
	s.mu.Lock()
	answers := append([]AnswerInput(nil), s.answers[turn.ID]...)
	delete(s.answers, turn.ID)
	s.mu.Unlock()
	if len(answers) == 0 {
		if actions, actionErr := s.runtime.ListActions(ctx, scope, turn.SessionID, turn.ID); actionErr == nil {
			for index := len(actions) - 1; index >= 0; index-- {
				if actions[index].Kind != "resume" {
					continue
				}
				_ = json.Unmarshal(actions[index].PayloadJSON, &answers)
				break
			}
		}
	}
	var output []schema.Message
	s.mu.Lock()
	runTimeout := s.runTimeout
	s.mu.Unlock()
	if runTimeout <= 0 {
		runTimeout = 5 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	eventObserver := newRuntimeEventObserver(s.runtime, scope, turn.SessionID, turn.ID, turn.Attempt)
	defer eventObserver.Close()
	runCtx = observe.With(runCtx, eventObserver)
	streamConfig := stream.Config{Buffer: 128, Lifecycle: true}
	if len(answers) > 0 {
		pending, pendingErr := compiled.PendingInterrupts(ctx, options...)
		if pendingErr != nil || len(pending) == 0 {
			return s.failTurn(ctx, scope, turn, "recovery_required", "找不到待回答问题")
		}
		resumeOptions := append([]runnable.Option{}, options...)
		for _, answer := range answers {
			resumeOptions = append(resumeOptions, graph.WithResume(answer.InterruptID, answer.Text))
		}
		var reader *stream.Reader[[]schema.Message]
		reader, err = compiled.ResumeStreamEvents(runCtx, streamConfig, resumeOptions...)
		if err == nil {
			output, err = s.consumePublicStream(ctx, scope, turn, reader)
		}
	} else {
		if _, abandonErr := state.AbandonRun(runCtx, checkpointStore, graphKey); abandonErr != nil && !errors.Is(abandonErr, state.ErrNoUnfinishedRun) {
			payload, _ := json.Marshal(map[string]string{"error": abandonErr.Error()})
			_, _ = s.runtime.AppendEvent(ctx, scope, domain.SessionEvent{SessionID: turn.SessionID, TurnID: turn.ID, Attempt: turn.Attempt, Kind: "checkpoint_recovery_failed", PayloadJSON: payload})
			return s.failTurn(ctx, scope, turn, "checkpoint_recovery_failed", abandonErr.Error())
		}
		input := append(schema.CloneMessages(tc.History), tc.UserMessage)
		var reader *stream.Reader[[]schema.Message]
		reader, err = compiled.StreamEvents(runCtx, input, streamConfig, options...)
		if err == nil {
			output, err = s.consumePublicStream(ctx, scope, turn, reader)
		}
	}
	eventObserver.Close()
	cancel()
	if err != nil {
		var interrupted *graph.InterruptedError
		if errors.As(err, &interrupted) {
			pending := interrupted.PendingInterrupts()
			data, _ := json.Marshal(pending)
			if setErr := s.runtime.SetPendingQuestions(ctx, scope, turn.ID, data); setErr != nil {
				return setErr
			}
			return s.runtime.SetTurnState(ctx, scope, turn.ID, domain.TurnAwaitingInput, "", "")
		}
		return s.failTurn(ctx, scope, turn, "execution_failed", err.Error())
	}
	for _, message := range output {
		content, _ := json.Marshal(message)
		if err = s.runtime.SaveMessages(ctx, scope, turn.SessionID, turn.Attempt, []domain.MessageRecord{{SessionID: turn.SessionID, TurnID: turn.ID, ContentJSON: content, Committed: true}}); err != nil {
			return s.failTurn(ctx, scope, turn, "message_persist_failed", err.Error())
		}
	}
	resultPayload, _ := json.Marshal(output)
	_, _ = s.runtime.AppendEvent(ctx, scope, domain.SessionEvent{SessionID: turn.SessionID, TurnID: turn.ID, Attempt: turn.Attempt, Kind: "turn.completed", PayloadJSON: resultPayload})
	return s.runtime.SetTurnState(ctx, scope, turn.ID, domain.TurnCompleted, "", "")
}

// consumePublicStream 持续排空 Graph 公共流，只把业务允许的中间内容投影到会话事件。
// 理解目标和草稿节点的原始模型消息留在内部，避免提示词、JSON 和内部修复过程泄漏到前端。
func (s *TurnService) consumePublicStream(ctx context.Context, scope domain.Scope, turn domain.Turn, reader *stream.Reader[[]schema.Message]) ([]schema.Message, error) {
	if reader == nil {
		return nil, errors.New("graph returned a nil public stream")
	}
	defer reader.Close()
	var output []schema.Message
	for {
		event, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			return output, nil
		}
		if err != nil {
			return output, err
		}
		if event.Message != nil {
			if event.Message.Snapshot != nil {
				if progress, ok := parseProgressMessage(*event.Message.Snapshot); ok {
					payload, marshalErr := json.Marshal(progress)
					if marshalErr == nil {
						_, _ = s.runtime.AppendEvent(ctx, scope, domain.SessionEvent{
							SessionID: turn.SessionID, TurnID: turn.ID, Attempt: turn.Attempt,
							Kind: "research.progress", PayloadJSON: payload,
							StreamRunID: event.Meta.RunID, Sequence: event.Meta.Sequence,
						})
					}
				}
				if event.Meta.NodeID == "workspace_tools" {
					for _, toolEvent := range workspaceToolEvents(*event.Message.Snapshot) {
						payload, marshalErr := json.Marshal(toolEvent)
						if marshalErr == nil {
							_, _ = s.runtime.AppendEvent(ctx, scope, domain.SessionEvent{
								SessionID: turn.SessionID, TurnID: turn.ID, Attempt: turn.Attempt,
								Kind: "workspace.tool", PayloadJSON: payload,
								StreamRunID: event.Meta.RunID, Sequence: event.Meta.Sequence,
							})
						}
					}
				}
			}
			if event.Meta.NodeID == "compose_reply" {
				payload, marshalErr := json.Marshal(event.Message)
				if marshalErr == nil {
					_, _ = s.runtime.AppendEvent(ctx, scope, domain.SessionEvent{
						SessionID: turn.SessionID, TurnID: turn.ID, Attempt: turn.Attempt,
						Kind: "assistant.delta", PayloadJSON: payload,
						StreamRunID: event.Meta.RunID, Sequence: event.Meta.Sequence,
					})
				}
			}
		}
		if event.Result != nil {
			output = schema.CloneMessages(*event.Result)
		}
	}
}

func (s *TurnService) failTurn(ctx context.Context, scope domain.Scope, turn domain.Turn, code, message string) error {
	_ = s.runtime.SetTurnState(ctx, scope, turn.ID, domain.TurnFailed, code, message)
	return fmt.Errorf("%s: %s", code, message)
}

func decodeCommittedMessages(records []domain.MessageRecord) []schema.Message {
	result := make([]schema.Message, 0, len(records))
	for _, record := range records {
		if !record.Committed || record.Superseded {
			continue
		}
		var message schema.Message
		if json.Unmarshal(record.ContentJSON, &message) == nil {
			result = append(result, message)
		}
	}
	return result
}

func hash(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func newID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
