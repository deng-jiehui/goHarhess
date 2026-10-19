package app

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

func interactionRouteRequest(request llm.Request) bool {
	for _, message := range request.Messages {
		if strings.Contains(message.JoinedText(""), "严格 JSON") {
			return true
		}
	}
	return false
}

func TestResearchInteractionDoesNotRequireHardcodedScope(t *testing.T) {
	deps := testGraphDeps(nil)
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if interactionRouteRequest(request) {
			return []schema.Message{schema.AssistantMessage(`{"route":"focus","scope":"","save":false}`)}, nil
		}
		return []schema.Message{schema.AssistantMessage("可以先比较模型压缩、推理效率和检索增强三个方向。")}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("先给我几个研究方向，不用追问")})
	if err != nil || len(output) == 0 {
		t.Fatalf("模型决定直接给方向时不应被固定范围拦截: output=%v err=%v", output, err)
	}
}

func TestResearchInteractionDynamicQuestionResumesOriginalTaskAfterRebuild(t *testing.T) {
	var notes []domain.Note
	routeCalls, questionCalls := 0, 0
	deps := testGraphDeps(&notes)
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if interactionRouteRequest(request) {
			routeCalls++
			return []schema.Message{schema.AssistantMessage(`{"route":"focus","scope":"检索系统","save":true}`)}, nil
		}
		for _, message := range request.Messages {
			if message.Role == schema.RoleTool {
				data, _ := json.Marshal(request.Messages)
				if !strings.Contains(string(data), "单张显卡，预算两小时") || !strings.Contains(string(data), "检索系统") {
					t.Errorf("恢复时丢失用户答案或原始研究范围: %s", data)
				}
				return []schema.Message{schema.AssistantMessage("在单卡两小时预算内比较检索基线。")}, nil
			}
		}
		if strings.HasPrefix(request.Messages[0].JoinedText(""), "__draft__") {
			questionCalls++
			return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(
				"ask-compute-budget", "ask_user", `{"question":"实验可使用哪些计算资源？","choices":["本地 CPU","实验室 GPU"]}`,
			))}, nil
		}
		return []schema.Message{schema.AssistantMessage("已按确认的计算预算整理并保存。")}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewMemoryStore()
	options := graph.WithCheckpoint(store, "dynamic-question")
	_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("整理检索系统实验方向，并保存为项目笔记")}, options, graph.WithDurability(graph.DurabilitySync))
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("模型提出的问题应暂停执行: %v", err)
	}
	if len(notes) != 0 {
		t.Fatal("回答前不应保存笔记")
	}
	pending, err := compiled.PendingInterrupts(context.Background(), options)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	payload, _ := json.Marshal(pending[0])
	if !strings.Contains(string(payload), "实验可使用哪些计算资源") || !strings.Contains(string(payload), "实验室 GPU") || strings.Contains(string(payload), "单文件缺陷") {
		t.Fatalf("中断问题必须来自模型: %s", payload)
	}
	// 重建编译图，验证恢复使用已保存的调用，不重新生成问题或业务路线。
	compiled, err = BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Resume(context.Background(), options, graph.WithResume(pending[0].ID, "单张显卡，预算两小时"))
	if err != nil || len(output) == 0 || len(notes) != 1 {
		t.Fatalf("恢复原任务失败: output=%v notes=%v err=%v", output, notes, err)
	}
	if routeCalls != 1 || questionCalls != 1 {
		t.Fatalf("恢复不应重跑路由或重新提问: route=%d question=%d", routeCalls, questionCalls)
	}
	data, _ := json.Marshal(output)
	if !strings.Contains(string(data), "单张显卡，预算两小时") {
		t.Fatal("已确认的用户信息应进入最终消息元数据，供后续对话使用")
	}
}

func TestResearchInteractionRoutingRepairsJSONWithoutToolDefinitions(t *testing.T) {
	deps := testGraphDeps(nil)
	calls := 0
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if interactionRouteRequest(request) {
			if len(request.Functions) != 0 {
				t.Error("结构化路由不应再提供 select_research_route 工具")
			}
			calls++
			if calls == 1 {
				return []schema.Message{schema.AssistantMessage("# 研究方向\n这不是路由 JSON")}, nil
			}
			return []schema.Message{schema.AssistantMessage(`{"route":"answer","scope":"","save":false}`)}, nil
		}
		return []schema.Message{schema.AssistantMessage("先做一份小规模基线。")}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("下一步怎么做？")})
	if err != nil || len(output) == 0 || calls != 2 {
		t.Fatalf("格式错误应修复一次而非关键词回退: output=%v calls=%d err=%v", output, calls, err)
	}
}

func TestResearchInteractionRejectsUnknownRouteWithoutKeywordFallback(t *testing.T) {
	var notes []domain.Note
	deps := testGraphDeps(&notes)
	deps.Model = llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		return []schema.Message{schema.AssistantMessage(`{"route":"invented_node","scope":"仓库级缺陷","save":true}`)}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("整理仓库研究并保存笔记")})
	if err == nil || !strings.Contains(err.Error(), "路由") || len(notes) != 0 {
		t.Fatalf("非法路线不得回退并写入笔记: notes=%v err=%v", notes, err)
	}
}

func TestResearchInteractionWorkspaceRejectsMixedQuestionAndWriteBeforeExecution(t *testing.T) {
	root := t.TempDir()
	deps := testGraphDeps(nil)
	deps.WorkspacePolicy = WorkspacePolicy{Root: root}
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if interactionRouteRequest(request) {
			return []schema.Message{schema.AssistantMessage(`{"route":"workspace","scope":"","save":false}`)}, nil
		}
		return []schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("mixed-write", "write_file", `{"path":"should-not-exist.txt","content":"unexpected","create":true}`),
			schema.NewFunctionToolCallBlock("mixed-ask", "ask_user", `{"question":"选择训练资源","choices":["CPU","GPU"]}`),
		)}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("检查工作区")}, graph.WithCheckpoint(state.NewMemoryStore(), "mixed"))
	if err == nil || !strings.Contains(err.Error(), "独占") || errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("混合批次应在执行前拒绝，不能进入写入审批: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "should-not-exist.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("被拒绝批次不应写入文件: %v", err)
	}
}

func TestResearchInteractionMultipleQuestionsReturnToEachBusinessFlow(t *testing.T) {
	for _, route := range []string{"answer", "notes", "workspace"} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			deps := testGraphDeps(nil)
			root := t.TempDir()
			writeTestWorkspaceFile(t, root, "README.md", "research evidence")
			deps.WorkspacePolicy = WorkspacePolicy{Root: root}
			routeCalls, businessCalls := 0, 0
			deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
				if interactionRouteRequest(request) {
					routeCalls++
					return []schema.Message{schema.AssistantMessage(`{"route":"` + route + `","scope":"检索系统","save":false}`)}, nil
				}
				if len(request.Functions) == 0 {
					return []schema.Message{schema.AssistantMessage("已完成当前任务。")}, nil
				}
				businessCalls++
				var answers []string
				read := false
				for _, message := range request.Messages {
					if answer := interactionResultText(message); answer != "" {
						answers = append(answers, answer)
					}
					for _, result := range message.FunctionToolResults() {
						read = read || result.Name == "read_file"
					}
				}
				switch len(answers) {
				case 0:
					return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("ask-resources", "ask_user", `{"question":"可用的计算资源是什么？"}`))}, nil
				case 1:
					if answers[0] != "仅 CPU" {
						t.Errorf("第一项真实答案丢失: %v", answers)
					}
					return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("ask-duration", "ask_user", `{"question":"希望何时完成第一轮？","choices":["本周","本月"]}`))}, nil
				}
				if strings.Join(answers, "/") != "仅 CPU/本周完成基线" {
					t.Errorf("连续追问没有保留全部答案: %v", answers)
				}
				if route == "workspace" && !read {
					return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("read-after-answers", "read_file", `{"path":"README.md"}`))}, nil
				}
				return []schema.Message{schema.AssistantMessage("根据 CPU 和一周时间预算完成研究建议。")}, nil
			})
			compiled, err := BuildResearchGraph(deps)
			if err != nil {
				t.Fatal(err)
			}
			options := graph.WithCheckpoint(state.NewMemoryStore(), "multi-"+route)
			_, err = compiled.Invoke(ctx, []schema.Message{schema.UserMessage("请继续检索系统的研究任务")}, options, graph.WithDurability(graph.DurabilitySync))
			var output []schema.Message
			for _, answer := range []string{"仅 CPU", "本周完成基线"} {
				if !errors.Is(err, graph.ErrInterrupted) {
					t.Fatalf("应等待下一项动态补充: %v", err)
				}
				pending, pendingErr := compiled.PendingInterrupts(ctx, options)
				if pendingErr != nil || len(pending) != 1 {
					t.Fatalf("pending=%v err=%v", pending, pendingErr)
				}
				compiled, err = BuildResearchGraph(deps)
				if err != nil {
					t.Fatal(err)
				}
				output, err = compiled.Resume(ctx, options, graph.WithResume(pending[0].ID, answer))
			}
			wantCalls := 3
			if route == "workspace" {
				wantCalls++
			}
			if err != nil || len(output) != 1 || routeCalls != 1 || businessCalls != wantCalls {
				t.Fatalf("连续恢复没有继续原流程: route=%d business=%d output=%v err=%v", routeCalls, businessCalls, output, err)
			}
			data, _ := json.Marshal(output)
			if !strings.Contains(string(data), "仅 CPU") || !strings.Contains(string(data), "本周完成基线") {
				t.Fatalf("最终消息缺少确认答案: %s", data)
			}
		})
	}
}

func TestResearchInteractionRejectsInvalidQuestionsBeforeInterrupt(t *testing.T) {
	for _, args := range []string{
		`{"question":""}`,
		`{"question":"计算资源？","choices":["CPU","CPU"]}`,
		`{"question":"计算资源？","choices":["CPU"]}`,
		`{"question":"计算资源？","approved":true}`,
		`{"question":"计算资源？"} {"question":"额外内容"}`,
	} {
		t.Run(args, func(t *testing.T) {
			deps := testGraphDeps(nil)
			deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
				if interactionRouteRequest(request) {
					return []schema.Message{schema.AssistantMessage(`{"route":"focus","scope":"","save":false}`)}, nil
				}
				return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("invalid-question", "ask_user", args))}, nil
			})
			compiled, err := BuildResearchGraph(deps)
			if err != nil {
				t.Fatal(err)
			}
			_, err = compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("比较研究方向")}, graph.WithCheckpoint(state.NewMemoryStore(), "invalid"))
			if err == nil || errors.Is(err, graph.ErrInterrupted) || !strings.Contains(err.Error(), "ask_user") {
				t.Fatalf("非法提问应在生成 pending 前拒绝: %v", err)
			}
		})
	}
}

func TestResearchInteractionStopsRepeatedQuestionsAtBudget(t *testing.T) {
	ctx := context.Background()
	deps := testGraphDeps(nil)
	questions := 0
	deps.Model = llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if interactionRouteRequest(request) {
			return []schema.Message{schema.AssistantMessage(`{"route":"answer","scope":"","save":false}`)}, nil
		}
		questions++
		return []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("ask-budget", "ask_user", `{"question":"还有什么约束？"}`))}, nil
	})
	compiled, err := BuildResearchGraph(deps)
	if err != nil {
		t.Fatal(err)
	}
	options := graph.WithCheckpoint(state.NewMemoryStore(), "question-budget")
	_, err = compiled.Invoke(ctx, []schema.Message{schema.UserMessage("继续讨论研究方案")}, options, graph.WithDurability(graph.DurabilitySync))
	for count := 0; count < 3; count++ {
		if !errors.Is(err, graph.ErrInterrupted) {
			t.Fatalf("第 %d 次提问没有正确暂停: %v", count+1, err)
		}
		pending, pendingErr := compiled.PendingInterrupts(ctx, options)
		if pendingErr != nil || len(pending) != 1 {
			t.Fatalf("pending=%v err=%v", pending, pendingErr)
		}
		_, err = compiled.Resume(ctx, options, graph.WithResume(pending[0].ID, "约束已经补充"))
	}
	if err == nil || errors.Is(err, graph.ErrInterrupted) || !strings.Contains(err.Error(), "追问次数已达上限") || questions != 4 {
		t.Fatalf("重复追问必须在预算内停止: questions=%d err=%v", questions, err)
	}
}
