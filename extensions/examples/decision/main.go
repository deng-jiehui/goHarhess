// 决策模型集成（W4-9 实验）端到端示例，全程脚本化假模型，离线可跑：
//
//  1. 分诊路由：decision.Node 在图里问 choice 问题，Route 辅助函数把
//     低置信度回答落到预声明的兜底目标（编译期校验的静态分支）。
//  2. 工具筛查：ToolGuard 中间件对有副作用的工具先问 noul 问题，
//     低于阈值经 graph.Interrupt 挂起，批准后经 agent.Resume 继续。
//
// 红线见 extensions/decision/doc.go：决策是建议不是授权。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/goharness/goharness/extensions/decision"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// ---- 第一部分：分诊路由 ----------------------------------------------------

type triageState struct {
	Text   string
	Answer decision.Answer
	Queue  string
}

func demoRouting(ctx context.Context) {
	// 假决策模型：按状态内容回答（退款→billing 高置信，其余→tech
	// 低置信走兜底），演示 Route 的置信度阈值与 fallback。
	dm := decision.NewFuncModel(func(_ context.Context, req decision.Request) (decision.Response, error) {
		text, _ := req.State.(string)
		answer := decision.Answer{Type: decision.QuestionTypeChoice, Choice: "tech", Confidence: 0.4}
		if strings.Contains(text, "退款") {
			answer = decision.Answer{Type: decision.QuestionTypeChoice, Choice: "billing", Confidence: 0.9}
		}
		return decision.Response{Answers: map[string]decision.Answer{"route": answer}}, nil
	})

	app, err := graph.NewState[triageState]("triage").
		Node("decide", decision.Node(dm,
			func(_ context.Context, s *triageState) (decision.Request, error) {
				return decision.Request{State: s.Text, Questions: map[string]decision.Question{
					"route": decision.Choice("这条工单属于哪个队列？",
						decision.Option{ID: "billing", Description: "账单与退款"},
						decision.Option{ID: "tech", Description: "技术问题"}),
				}}, nil
			},
			func(_ context.Context, s *triageState, r decision.Response) error {
				s.Answer = r.Answers["route"]
				return nil
			})).
		Node("billing", func(_ context.Context, s *triageState) error {
			s.Queue = "billing"
			return nil
		}).
		Node("tech", func(_ context.Context, s *triageState) error {
			s.Queue = "tech"
			return nil
		}).
		Node("manual", func(_ context.Context, s *triageState) error {
			s.Queue = "manual"
			return nil
		}).
		Start("decide").
		// 路由目标全部预声明：低置信度或未识别的选项都落 manual。
		Branch("decide", func(_ context.Context, s *triageState) (string, error) {
			return decision.Route(s.Answer, 0.6,
				map[string]string{"billing": "billing", "tech": "tech"}, "manual"), nil
		}, "billing", "tech", "manual").
		Edge("billing", graph.End).
		Edge("tech", graph.End).
		Edge("manual", graph.End).
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	for _, text := range []string{"我要申请退款", "服务连不上了"} {
		out, err := app.Invoke(ctx, triageState{Text: text})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("工单 %q → 队列 %s（choice=%s confidence=%.2f）\n",
			text, out.Queue, out.Answer.Choice, out.Answer.Confidence)
	}
}

// ---- 第二部分：工具筛查 ----------------------------------------------------

func demoGuard(ctx context.Context) {
	// 决策模型恒定回答 PTrue=0.2，低于阈值 → 转人工审批。
	// 恢复重放时会再问一次同一问题，恒定答案保证中断载荷一致。
	dm := decision.NewFuncModel(func(context.Context, decision.Request) (decision.Response, error) {
		return decision.Response{Answers: map[string]decision.Answer{
			decision.GuardQuestionID: {Type: decision.QuestionTypeNoul, PTrue: 0.2, Confidence: 0.7},
		}}, nil
	})
	guard, err := decision.ToolGuard(decision.GuardConfig{Model: dm, MinPTrue: 0.8})
	if err != nil {
		log.Fatal(err)
	}

	var executed bool
	deploy := tool.NewFunctionTool(tool.Spec{Name: "deploy"},
		func(context.Context, tool.CallInput) (tool.Result, error) {
			executed = true
			return tool.TextResult("deployed"), nil
		})

	assistant, err := agent.New(
		llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("c1", "deploy", `{}`))},
			[]schema.Message{schema.AssistantMessage("部署完成")}),
		agent.WithName("ops"), agent.WithTools(deploy), agent.WithMiddleware(guard))
	if err != nil {
		log.Fatal(err)
	}
	checkpoint := agent.WithCheckpoint(state.NewMemoryStore(), "ops-demo")

	_, err = assistant.Invoke(ctx, []schema.Message{schema.UserMessage("部署新版本")}, checkpoint)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		log.Fatalf("应挂起等待审批，实际: %v", err)
	}
	pending := suspended.PendingInterrupts()
	fmt.Printf("deploy 被筛查拦截（PTrue=0.2 < 0.8），等待审批问题 %s；工具已执行=%v\n",
		pending[0].ID, executed)

	// 审批批准 → 恢复运行 → 工具执行一次。
	out, err := agent.Resume(ctx, assistant, checkpoint, agent.WithResume(pending[0].ID, true))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("批准后恢复：工具执行=%v，最终答复 %q\n", executed, out[len(out)-1].JoinedText(""))
}

func main() {
	ctx := context.Background()
	fmt.Println("== 分诊路由 ==")
	demoRouting(ctx)
	fmt.Println("\n== 工具筛查 ==")
	demoGuard(ctx)
}
