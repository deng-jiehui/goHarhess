package decision_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/extensions/decision"
	extotel "github.com/goharness/goharness/extensions/otel"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// ---------- Request.Validate ----------

func TestRequestValidate(t *testing.T) {
	choice := decision.Choice("pick", decision.Option{ID: "a"}, decision.Option{ID: "b"})
	valid := decision.Request{Questions: map[string]decision.Question{"q": choice}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	cases := map[string]decision.Request{
		"非法字符":   {Questions: map[string]decision.Question{"bad id!": choice}},
		"空键":     {Questions: map[string]decision.Question{"": choice}},
		"超长键":    {Questions: map[string]decision.Question{strings.Repeat("x", 101): choice}},
		"选项不足":   {Questions: map[string]decision.Question{"q": decision.Choice("pick", decision.Option{ID: "a"})}},
		"选项ID重复": {Questions: map[string]decision.Question{"q": decision.Choice("pick", decision.Option{ID: "a"}, decision.Option{ID: "a"})}},
		"选项ID空":  {Questions: map[string]decision.Question{"q": decision.Choice("pick", decision.Option{ID: ""}, decision.Option{ID: "b"})}},
		"级别不足":   {Questions: map[string]decision.Question{"q": decision.Score("rate", "low")}},
		"级别过多":   {Questions: map[string]decision.Question{"q": decision.Score("rate", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11")}},
		"未知类型":   {Questions: map[string]decision.Question{"q": {Type: "bogus"}}},
	}
	for name, req := range cases {
		if err := req.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	// 问题数上限 32
	many := decision.Request{Questions: map[string]decision.Question{}}
	for i := 0; i < 33; i++ {
		many.Questions[strings.Repeat("q", 1)+string(rune('a'+i))+string(rune('0'+i/26))] = choice
	}
	if err := many.Validate(); err == nil {
		t.Error("33 questions: expected error")
	}
	// Noul 无需选项即合法
	if err := (decision.Request{Questions: map[string]decision.Question{"ok": decision.Noul("is it?")}}).Validate(); err != nil {
		t.Errorf("noul without options rejected: %v", err)
	}
	// 边界：恰好 100 字符的键、32 个问题
	exact := decision.Request{Questions: map[string]decision.Question{}}
	for i := 0; i < 32; i++ {
		key := string(rune('a'+i%26)) + string(rune('0'+i/26)) + strings.Repeat("k", 98)
		exact.Questions[key] = choice
	}
	if err := exact.Validate(); err != nil {
		t.Errorf("32 questions with 100-char keys rejected: %v", err)
	}
}

// ---------- Route / Yes ----------

func TestRoute(t *testing.T) {
	targets := map[string]string{"billing": "billing_node", "support": "support_node"}
	answer := decision.Answer{Type: decision.QuestionTypeChoice, Choice: "billing", Confidence: 0.9}

	if got := decision.Route(answer, 0.5, targets, "fallback"); got != "billing_node" {
		t.Errorf("high confidence: got %q", got)
	}
	if got := decision.Route(answer, 0.95, targets, "fallback"); got != "fallback" {
		t.Errorf("low confidence: got %q", got)
	}
	unknown := decision.Answer{Type: decision.QuestionTypeChoice, Choice: "sales", Confidence: 0.9}
	if got := decision.Route(unknown, 0.5, targets, "fallback"); got != "fallback" {
		t.Errorf("unlisted choice: got %q", got)
	}
	// 置信度恰等于阈值 → 目标
	edge := decision.Answer{Type: decision.QuestionTypeChoice, Choice: "billing", Confidence: 0.5}
	if got := decision.Route(edge, 0.5, targets, "fallback"); got != "billing_node" {
		t.Errorf("confidence == threshold: got %q", got)
	}
}

func TestYes(t *testing.T) {
	if !decision.Yes(decision.Answer{PTrue: 0.5}, 0.5) {
		t.Error("PTrue == threshold should be true")
	}
	if decision.Yes(decision.Answer{PTrue: 0.49}, 0.5) {
		t.Error("PTrue < threshold should be false")
	}
}

// ---------- Node / ScriptedModel ----------

type decisionState struct {
	Input  string
	Answer decision.Answer
	Visits []string
}

func TestNode(t *testing.T) {
	var order []string
	model := decision.NewScriptedModel(decision.Response{Answers: map[string]decision.Answer{
		"route": {Type: decision.QuestionTypeChoice, Choice: "billing", Confidence: 0.9},
	}})
	node := decision.Node[decisionState](model,
		func(_ context.Context, s *decisionState) (decision.Request, error) {
			order = append(order, "build")
			return decision.Request{State: s.Input, Questions: map[string]decision.Question{
				"route": decision.Choice("pick", decision.Option{ID: "billing"}, decision.Option{ID: "support"}),
			}}, nil
		},
		func(_ context.Context, s *decisionState, r decision.Response) error {
			order = append(order, "apply")
			s.Answer = r.Answers["route"]
			return nil
		})
	s := &decisionState{Input: "ticket"}
	if err := node(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if s.Answer.Choice != "billing" {
		t.Fatalf("answer not applied: %+v", s.Answer)
	}
	if len(order) != 2 || order[0] != "build" || order[1] != "apply" {
		t.Fatalf("order=%v", order)
	}
	if reqs := model.Requests(); len(reqs) != 1 || reqs[0].State != "ticket" {
		t.Fatalf("requests=%v", reqs)
	}
	// 用尽后报错
	if err := node(context.Background(), s); !errors.Is(err, decision.ErrScriptedExhausted) {
		t.Fatalf("exhausted: %v", err)
	}
}

func TestNodeModelErrorPropagates(t *testing.T) {
	sentinel := errors.New("model down")
	model := decision.NewFuncModel(func(context.Context, decision.Request) (decision.Response, error) {
		return decision.Response{}, sentinel
	})
	node := decision.Node[decisionState](model,
		func(context.Context, *decisionState) (decision.Request, error) {
			return decision.Request{Questions: map[string]decision.Question{"q": decision.Noul("ok?")}}, nil
		},
		func(context.Context, *decisionState, decision.Response) error {
			t.Error("apply must not run on model error")
			return nil
		})
	if err := node(context.Background(), &decisionState{}); !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
}

// ---------- 回答范围校验 ----------

func TestValidateAnswer(t *testing.T) {
	choice := decision.Choice("pick", decision.Option{ID: "a"}, decision.Option{ID: "b"})
	score := decision.Score("rate", "lo", "mid", "hi")
	noul := decision.Noul("ok?")

	valid := []struct {
		q decision.Question
		a decision.Answer
	}{
		{choice, decision.Answer{Type: decision.QuestionTypeChoice, Choice: "a", Probabilities: map[string]float64{"a": 0.7, "b": 0.3}, Confidence: 0.9}},
		{choice, decision.Answer{Choice: "b", Confidence: 0.5}}, // 空 Type 视为同型
		{score, decision.Answer{Type: decision.QuestionTypeScore, Score: 1.5, Confidence: 0.8}},
		{noul, decision.Answer{PTrue: 1, Confidence: 0}},
	}
	for i, tc := range valid {
		if err := tc.q.ValidateAnswer(tc.a); err != nil {
			t.Errorf("valid[%d]: %v", i, err)
		}
	}

	nan := math.NaN()
	invalid := []struct {
		name string
		q    decision.Question
		a    decision.Answer
	}{
		{"类型不符", noul, decision.Answer{Type: decision.QuestionTypeChoice, Choice: "a", Confidence: 0.5}},
		{"置信度越界", noul, decision.Answer{PTrue: 0.5, Confidence: 1.1}},
		{"置信度NaN", noul, decision.Answer{PTrue: 0.5, Confidence: nan}},
		{"未知选项", choice, decision.Answer{Choice: "ghost", Confidence: 0.9}},
		{"概率键非选项", choice, decision.Answer{Choice: "a", Probabilities: map[string]float64{"a": 0.5, "ghost": 0.5}, Confidence: 0.9}},
		{"概率越界", choice, decision.Answer{Choice: "a", Probabilities: map[string]float64{"a": 1.2}, Confidence: 0.9}},
		{"概率NaN", choice, decision.Answer{Choice: "a", Probabilities: map[string]float64{"a": nan}, Confidence: 0.9}},
		{"分数越界", score, decision.Answer{Score: 3, Confidence: 0.5}},
		{"分数NaN", score, decision.Answer{Score: nan, Confidence: 0.5}},
		{"PTrue越界", noul, decision.Answer{PTrue: 5, Confidence: 0.9}},
		{"PTrue为NaN", noul, decision.Answer{PTrue: nan, Confidence: 0.5}},
	}
	for _, tc := range invalid {
		if err := tc.q.ValidateAnswer(tc.a); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
}

func TestNodeRejectsInvalidResponse(t *testing.T) {
	model := decision.NewScriptedModel(decision.Response{Answers: map[string]decision.Answer{
		"q": {Type: decision.QuestionTypeNoul, PTrue: 5, Confidence: 0.9},
	}})
	applied := false
	node := decision.Node[decisionState](model,
		func(context.Context, *decisionState) (decision.Request, error) {
			return decision.Request{Questions: map[string]decision.Question{"q": decision.Noul("ok?")}}, nil
		},
		func(context.Context, *decisionState, decision.Response) error {
			applied = true
			return nil
		})
	err := node(context.Background(), &decisionState{})
	if !errors.Is(err, decision.ErrInvalidResponse) {
		t.Fatalf("expected ErrInvalidResponse, got %v", err)
	}
	if applied {
		t.Fatal("apply must not run on invalid response")
	}
}

// ---------- 图集成 ----------

func triageGraph(t *testing.T, model decision.Model, extraTargets ...string) (*graph.CompiledGraph[decisionState, *decisionState], error) {
	t.Helper()
	targets := map[string]string{"billing": "billing", "support": "support", "sales": "sales"}
	return graph.NewState[decisionState]("triage").
		Node("triage", decision.Node[decisionState](model,
			func(_ context.Context, s *decisionState) (decision.Request, error) {
				return decision.Request{State: s.Input, Questions: map[string]decision.Question{
					"route": decision.Choice("route the ticket",
						decision.Option{ID: "billing"}, decision.Option{ID: "support"}, decision.Option{ID: "sales"}),
				}}, nil
			},
			func(_ context.Context, s *decisionState, r decision.Response) error {
				s.Answer = r.Answers["route"]
				return nil
			})).
		Node("billing", func(_ context.Context, s *decisionState) error { s.Visits = append(s.Visits, "billing"); return nil }).
		Node("support", func(_ context.Context, s *decisionState) error { s.Visits = append(s.Visits, "support"); return nil }).
		Node("sales", func(_ context.Context, s *decisionState) error { s.Visits = append(s.Visits, "sales"); return nil }).
		Node("fallback", func(_ context.Context, s *decisionState) error { s.Visits = append(s.Visits, "fallback"); return nil }).
		Start("triage").
		Branch("triage", func(_ context.Context, s *decisionState) (string, error) {
			return decision.Route(s.Answer, 0.5, targets, "fallback"), nil
		}, append([]string{"billing", "support", "sales", "fallback"}, extraTargets...)...).
		Edge("billing", graph.End).Edge("support", graph.End).Edge("sales", graph.End).Edge("fallback", graph.End).
		Compile()
}

func choiceResponse(choice string, confidence float64) decision.Response {
	return decision.Response{Answers: map[string]decision.Answer{
		"route": {Type: decision.QuestionTypeChoice, Choice: choice, Confidence: confidence},
	}}
}

func TestTriageGraphRouting(t *testing.T) {
	cases := []struct {
		name     string
		response decision.Response
		want     string
	}{
		{"高置信度路由到选中项", choiceResponse("billing", 0.9), "billing"},
		{"低置信度走兜底", choiceResponse("support", 0.2), "fallback"},
		{"置信度恰等于阈值仍路由", choiceResponse("sales", 0.5), "sales"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app, err := triageGraph(t, decision.NewScriptedModel(tc.response))
			if err != nil {
				t.Fatal(err)
			}
			out, err := app.Invoke(context.Background(), decisionState{Input: "invoice issue"})
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Visits) != 1 || out.Visits[0] != tc.want {
				t.Fatalf("visits=%v want %q", out.Visits, tc.want)
			}
		})
	}
}

// 回归断言：Branch 声明的目标指向不存在的节点时 Compile 失败（现有图行为）。
func TestTriageGraphUndeclaredBranchTarget(t *testing.T) {
	// 为触发"声明了不存在节点"，直接从 Branch targets 加入不存在的节点名。
	_, err := graph.NewState[decisionState]("bad-triage").
		Node("triage", decision.Node[decisionState](decision.NewScriptedModel(choiceResponse("billing", 0.9)),
			func(context.Context, *decisionState) (decision.Request, error) {
				return decision.Request{Questions: map[string]decision.Question{"route": decision.Noul("x?")}}, nil
			},
			func(context.Context, *decisionState, decision.Response) error { return nil })).
		Start("triage").
		Branch("triage", func(context.Context, *decisionState) (string, error) { return "ghost", nil }, "ghost").
		Compile()
	if err == nil {
		t.Fatal("expected Compile to fail for undeclared branch target")
	}
}

// ---------- ToolGuard ----------

func newGuardAgent(t *testing.T, guard tool.Middleware, tracked *atomic.Int32, extraTools ...tool.Tool) agent.Agent {
	t.Helper()
	tracked_ := tool.NewFunctionTool(tool.Spec{Name: "refund"},
		func(context.Context, tool.CallInput) (tool.Result, error) {
			tracked.Add(1)
			return tool.TextResult("refund issued"), nil
		})
	tools := append([]tool.Tool{tracked_}, extraTools...)
	value, err := agent.New(llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("c1", "refund", `{"amount": 10}`))},
		[]schema.Message{schema.AssistantMessage("settled")}),
		agent.WithName("guard-test"), agent.WithTools(tools...), agent.WithMiddleware(guard))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func guardConfig(model decision.Model) decision.GuardConfig {
	return decision.GuardConfig{Model: model, ModelName: "test-decision", MinPTrue: 0.5}
}

func noulResponse(ptrue float64) decision.Response {
	return decision.Response{Answers: map[string]decision.Answer{
		decision.GuardQuestionID: {Type: decision.QuestionTypeNoul, PTrue: ptrue, Confidence: 0.9},
	}}
}

func TestToolGuardSkipsReadOnly(t *testing.T) {
	model := decision.NewScriptedModel() // 零响应：被调用即 ErrScriptedExhausted
	guard, err := decision.ToolGuard(guardConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	readonly := tool.NewFunctionTool(tool.Spec{Name: "lookup", IsReadOnly: true},
		func(context.Context, tool.CallInput) (tool.Result, error) {
			ran.Add(1)
			return tool.TextResult("found"), nil
		})
	value, err := agent.New(llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("c1", "lookup", `{}`))},
		[]schema.Message{schema.AssistantMessage("done")}),
		agent.WithTools(readonly), agent.WithMiddleware(guard))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 || len(model.Requests()) != 0 {
		t.Fatalf("ran=%d modelCalls=%d", ran.Load(), len(model.Requests()))
	}
}

func TestToolGuardAllowsAboveThreshold(t *testing.T) {
	var ran atomic.Int32
	model := decision.NewScriptedModel(noulResponse(0.9))
	guard, err := decision.ToolGuard(guardConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	value := newGuardAgent(t, guard, &ran)
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 {
		t.Fatalf("tool ran %d times", ran.Load())
	}
}

func guardInterrupt(t *testing.T, value agent.Agent, option runnable.Option) *graph.InterruptedError {
	t.Helper()
	_, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("expected InterruptedError, got %v", err)
	}
	if pending := suspended.PendingInterrupts(); len(pending) != 1 {
		t.Fatalf("pending=%d", len(pending))
	}
	return suspended
}

func TestToolGuardInterruptsBelowThreshold(t *testing.T) {
	var ran atomic.Int32
	// 恢复后中间件重放会再问一次 → 预置两条相同回答。
	model := decision.NewScriptedModel(noulResponse(0.1), noulResponse(0.1))
	guard, err := decision.ToolGuard(guardConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	value := newGuardAgent(t, guard, &ran)
	option := agent.WithCheckpoint(state.NewMemoryStore(), "guard-low")

	suspended := guardInterrupt(t, value, option)
	if ran.Load() != 0 {
		t.Fatalf("tool ran %d times before approval", ran.Load())
	}
	var payload approvalPayloadView
	for _, p := range suspended.PendingInterrupts() {
		decodePayload(t, p.Payload, &payload)
	}
	if payload.Tool != "refund" || payload.Threshold != 0.5 || payload.PTrue != 0.1 {
		t.Fatalf("payload=%+v", payload)
	}

	pending := suspended.PendingInterrupts()
	out, err := agent.Resume(context.Background(), value, option, agent.WithResume(pending[0].ID, true))
	if err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 {
		t.Fatalf("tool ran %d times after approval", ran.Load())
	}
	if last := out[len(out)-1]; last.JoinedText("") != "settled" {
		t.Fatalf("output=%q", last.JoinedText(""))
	}
	// 两次决策调用：首次 + 恢复重放。
	if got := len(model.Requests()); got != 2 {
		t.Fatalf("model calls=%d", got)
	}
}

type approvalPayloadView struct {
	Tool      string  `json:"tool"`
	PTrue     float64 `json:"ptrue"`
	Threshold float64 `json:"threshold"`
	Reason    string  `json:"reason"`
}

func decodePayload(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatal(err)
	}
}

func TestToolGuardRejects(t *testing.T) {
	var ran atomic.Int32
	model := decision.NewScriptedModel(noulResponse(0.1), noulResponse(0.1))
	guard, err := decision.ToolGuard(guardConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	value := newGuardAgent(t, guard, &ran)
	option := agent.WithCheckpoint(state.NewMemoryStore(), "guard-reject")

	suspended := guardInterrupt(t, value, option)
	pending := suspended.PendingInterrupts()
	out, err := agent.Resume(context.Background(), value, option, agent.WithResume(pending[0].ID, false))
	if err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 0 {
		t.Fatalf("tool ran %d times", ran.Load())
	}
	// 拒绝结果须对模型可见：ReAct 配方把 isError 结果渲染成
	// "An error occurred when calling tool ...: <text>" 观察消息。
	if !hasToolResultText(out, "refund", "rejected by reviewer") {
		t.Fatalf("no rejection tool result in output: %v", out)
	}
}

func TestToolGuardRejectsUnrecognizedAnswer(t *testing.T) {
	var ran atomic.Int32
	model := decision.NewScriptedModel(noulResponse(0.1), noulResponse(0.1))
	guard, err := decision.ToolGuard(guardConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	value := newGuardAgent(t, guard, &ran)
	option := agent.WithCheckpoint(state.NewMemoryStore(), "guard-weird")

	suspended := guardInterrupt(t, value, option)
	pending := suspended.PendingInterrupts()
	out, err := agent.Resume(context.Background(), value, option, agent.WithResume(pending[0].ID, "maybe"))
	if err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 0 {
		t.Fatalf("tool ran %d times", ran.Load())
	}
	if !hasToolResultText(out, "refund", "not recognized") {
		t.Fatalf("no unrecognized-answer result in output: %v", out)
	}
}

func TestToolGuardModelErrorInterrupts(t *testing.T) {
	var ran atomic.Int32
	sentinel := errors.New("decision backend down")
	model := decision.NewFuncModel(func(context.Context, decision.Request) (decision.Response, error) {
		return decision.Response{}, sentinel
	})
	guard, err := decision.ToolGuard(guardConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	value := newGuardAgent(t, guard, &ran)
	option := agent.WithCheckpoint(state.NewMemoryStore(), "guard-down")

	suspended := guardInterrupt(t, value, option)
	if ran.Load() != 0 {
		t.Fatalf("tool ran %d times", ran.Load())
	}
	var payload approvalPayloadView
	decodePayload(t, suspended.PendingInterrupts()[0].Payload, &payload)
	if payload.Reason != "decision model unavailable" {
		t.Fatalf("reason=%q", payload.Reason)
	}
}

// 取消/排空时决策模型出错必须原样上抛，不产生人工审批中断。
func TestToolGuardCancelPropagates(t *testing.T) {
	sentinel := errors.New("drain requested")
	guard, err := decision.ToolGuard(guardConfig(decision.NewFuncModel(
		func(ctx context.Context, _ decision.Request) (decision.Response, error) {
			<-ctx.Done()
			return decision.Response{}, context.Cause(ctx)
		})))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := guard.Handle(ctx, tool.Spec{Name: "refund"},
			tool.CallInput{RawArgs: `{}`},
			func(context.Context, tool.CallInput) (tool.Result, error) {
				return tool.TextResult("ran"), nil
			})
		done <- err
	}()
	cancel(sentinel)
	err = <-done
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected cancellation cause to propagate, got %v", err)
	}
	// 若误走审批路径，返回的会是 ctx.Err() 或中断错误而非 sentinel；
	// errors.Is(err, sentinel) 命中即证明没有经过 Interrupt。
}

// 决策模型返回越界回答按"模型不可用"走保守审批，而不是放行。
func TestToolGuardInvalidAnswerInterrupts(t *testing.T) {
	var ran atomic.Int32
	model := decision.NewScriptedModel(
		decision.Response{Answers: map[string]decision.Answer{
			decision.GuardQuestionID: {Type: decision.QuestionTypeNoul, PTrue: 5, Confidence: 0.9},
		}})
	guard, err := decision.ToolGuard(guardConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	value := newGuardAgent(t, guard, &ran)
	option := agent.WithCheckpoint(state.NewMemoryStore(), "guard-invalid")

	suspended := guardInterrupt(t, value, option)
	if ran.Load() != 0 {
		t.Fatalf("tool ran %d times", ran.Load())
	}
	var payload approvalPayloadView
	decodePayload(t, suspended.PendingInterrupts()[0].Payload, &payload)
	if payload.Reason != "decision model returned an invalid answer" {
		t.Fatalf("reason=%q", payload.Reason)
	}
}

func TestToolGuardConfigError(t *testing.T) {
	model := decision.NewScriptedModel()
	for _, min := range []float64{0, -0.5, 1.5} {
		if _, err := decision.ToolGuard(decision.GuardConfig{Model: model, MinPTrue: min}); err == nil {
			t.Errorf("MinPTrue=%v: expected config error", min)
		}
	}
	if _, err := decision.ToolGuard(decision.GuardConfig{MinPTrue: 0.5}); err == nil {
		t.Error("nil model: expected config error")
	}
}

func hasToolResultText(messages []schema.Message, toolName, text string) bool {
	for _, m := range messages {
		for _, b := range m.ContentBlocks {
			if b.FunctionToolResult == nil || b.FunctionToolResult.Name != toolName {
				continue
			}
			for _, inner := range b.FunctionToolResult.ContentBlocks {
				if inner.Text != nil && strings.Contains(inner.Text.Text, text) {
					return true
				}
			}
		}
	}
	return false
}

// ---------- Observe ----------

type memObserver struct {
	mu     sync.Mutex
	events []observe.Event
}

func (o *memObserver) OnEvent(_ context.Context, e observe.Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func TestObserveEmitsPerAnswer(t *testing.T) {
	model := decision.Observe(decision.NewScriptedModel(decision.Response{Answers: map[string]decision.Answer{
		"route": {Type: decision.QuestionTypeChoice, Choice: "billing", Confidence: 0.9},
		"risk":  {Type: decision.QuestionTypeNoul, PTrue: 0.3, Confidence: 0.7},
		"level": {Type: decision.QuestionTypeScore, Score: 1.5, Confidence: 0.8},
	}}))
	obs := &memObserver{}
	ctx := observe.With(context.Background(), obs)
	_, err := model.Invoke(ctx, decision.Request{Questions: map[string]decision.Question{
		"route": decision.Choice("pick", decision.Option{ID: "a"}, decision.Option{ID: "b"}),
		"risk":  decision.Noul("ok?"),
		"level": decision.Score("rate", "lo", "hi"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(obs.events) != 3 {
		t.Fatalf("events=%d", len(obs.events))
	}
	byQuestion := map[string]observe.Event{}
	for _, e := range obs.events {
		if e.Kind != decision.KindDecision {
			t.Fatalf("kind=%q", e.Kind)
		}
		byQuestion[e.Attrs["question_id"].(string)] = e
	}
	if byQuestion["route"].Attrs["selected"] != "billing" || byQuestion["route"].Attrs["question_type"] != "choice" {
		t.Fatalf("choice event attrs=%v", byQuestion["route"].Attrs)
	}
	if byQuestion["risk"].Attrs["p_true"] != 0.3 {
		t.Fatalf("noul event attrs=%v", byQuestion["risk"].Attrs)
	}
	if byQuestion["level"].Attrs["score"] != 1.5 {
		t.Fatalf("score event attrs=%v", byQuestion["level"].Attrs)
	}
	for _, e := range obs.events {
		if e.Attrs["confidence"] == nil {
			t.Fatalf("missing confidence: %v", e.Attrs)
		}
	}
}

// 现有观察者对未知 Kind 的实际表现：不 panic 是硬性要求。
func TestObserveWithSlogAndOtel(t *testing.T) {
	model := decision.Observe(decision.NewScriptedModel(decision.Response{Answers: map[string]decision.Answer{
		"route": {Type: decision.QuestionTypeChoice, Choice: "billing", Confidence: 0.9},
	}}))
	req := decision.Request{Questions: map[string]decision.Question{
		"route": decision.Choice("pick", decision.Option{ID: "a"}, decision.Option{ID: "b"}),
	}}

	var buf bytes.Buffer
	slogObserver := observe.Slog(slog.New(slog.NewTextHandler(&buf, nil)))
	ctx := observe.With(context.Background(), slogObserver)
	if _, err := model.Invoke(ctx, req); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "decision") {
		t.Fatalf("slog output missing decision event: %q", buf.String())
	}

	otelObserver := extotel.Observer(nil)
	ctx = observe.With(context.Background(), otelObserver)
	model2 := decision.Observe(decision.NewScriptedModel(decision.Response{Answers: map[string]decision.Answer{
		"route": {Type: decision.QuestionTypeChoice, Choice: "billing", Confidence: 0.9},
	}}))
	if _, err := model2.Invoke(ctx, req); err != nil {
		t.Fatal(err)
	}
}

// ---------- 并发 ----------

func TestScriptedModelConcurrent(t *testing.T) {
	model := decision.NewScriptedModel(decision.Response{Answers: map[string]decision.Answer{}})
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := model.Invoke(context.Background(), decision.Request{Questions: map[string]decision.Question{
				"q": decision.Noul("ok?"),
			}})
			if err != nil && !errors.Is(err, decision.ErrScriptedExhausted) {
				errs.Add(1)
			}
		}()
	}
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("unexpected errors: %d", errs.Load())
	}
	if got := len(model.Requests()); got != 32 {
		t.Fatalf("requests=%d", got)
	}
}
