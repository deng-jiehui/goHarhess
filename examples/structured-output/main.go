// examples/structured-output 演示结构化输出的两种接入方式：
//  1. 单次结构化回答：模型调用携带类型化 ResponseFormat，回答经同一份
//     Schema 校验后解码为 Go 结构体。
//  2. 工具 Agent 之后挂「整理模型 → 解码节点」：业务 Agent 完成任务，
//     整理模型只生成结构化答案（不带 Functions），解码节点写入 State。
//
// 两个模型都指向本地 HTTP fixture，不需要真实 API Key；fixture 同时验证
// 请求体中确实携带了 response_format。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

type Decision struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason"`
}

const decisionJSON = `{"approved":false,"reason":"缺少审批材料"}`

// fixture 按路径返回 OpenAI-compatible 响应，并记录是否收到 response_format。
func newModelFixture(t *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if payload["response_format"] != nil {
			t.Add(1)
		}
		// 业务请求（无 response_format）返回普通文本；结构化请求返回 JSON。
		text := "approval requires more material"
		if payload["response_format"] != nil {
			text = decisionJSON
		}
		if payload["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":%s}}]}\n\n", mustQuote(text))
			fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\",\"delta\":{}}]}\n\n")
			fmt.Fprintf(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":%s},"finish_reason":"stop"}]}`,
			mustQuote(text))
	}))
}

func mustQuote(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func main() {
	var formatRequests atomic.Int32
	server := newModelFixture(&formatRequests)
	defer server.Close()

	model, err := llm.New(llm.Config{
		ModelType:    "openai-compatible",
		Model:        "fixture-model",
		ModelServer:  server.URL,
		HTTPClient:   server.Client(),
		Capabilities: &llm.Capabilities{Streaming: true, ToolCalls: true, JSONSchema: true},
	})
	if err != nil {
		log.Fatal(err)
	}
	output, err := llm.NewStructuredOutput[Decision](
		llm.WithOutputName("decision"), llm.WithOutputDescription("审批结论"))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	// 1. 单次结构化回答。
	response, err := model.Invoke(ctx,
		llm.Request{Messages: []schema.Message{schema.UserMessage("审批单 A-1 是否通过？")}},
		llm.WithResponseFormat(output.Format()))
	if err != nil {
		log.Fatal(err)
	}
	decision, err := output.Decode(response.Messages)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("one-shot: approved=%v reason=%s\n", decision.Approved, decision.Reason)

	// 2. 业务 Agent → 整理模型 → decode 的图接线。
	lookup := tool.New("lookup", "查询审批材料", func(_ context.Context, a struct {
		ID string `json:"id"`
	}) (string, error) {
		return a.ID + ": 材料缺失", nil
	}, tool.ReadOnly())
	// 业务 Agent 用脚本模型：先调用工具，再给出文字结论。
	worker, err := agent.New(llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("1", "lookup", `{"id":"A-1"}`))},
		[]schema.Message{schema.AssistantMessage("A-1 缺少审批材料")},
	), agent.WithTools(lookup))
	if err != nil {
		log.Fatal(err)
	}
	organizer, err := llm.NewChatModelNode(model)
	if err != nil {
		log.Fatal(err)
	}

	type flowState struct {
		Task      []schema.Message
		AgentOut  []schema.Message
		Organizer []schema.Message
		Decision  *Decision
	}
	g := graph.NewGraph[[]schema.Message, Decision]("approval")
	if err := g.SetStateFactory(func(_ context.Context, task []schema.Message) (*flowState, error) {
		return &flowState{Task: schema.CloneMessages(task)}, nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := g.AddNode("work", worker,
		graph.WithPreHandler(func(_ context.Context, s *flowState) ([]schema.Message, error) {
			return schema.CloneMessages(s.Task), nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *flowState, out []schema.Message) error {
			s.AgentOut = schema.CloneMessages(out)
			return nil
		}),
	); err != nil {
		log.Fatal(err)
	}
	if err := g.AddNode("organize", organizer,
		// 整理模型只读结论生成结构化答案，不给 Functions（不再有工具循环）。
		graph.WithPreHandler(func(_ context.Context, s *flowState) (llm.Request, error) {
			messages := append(schema.CloneMessages(s.AgentOut),
				schema.UserMessage("把上面的结论整理成 JSON 输出。"))
			return llm.Request{Messages: messages}, nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *flowState, response llm.Response) error {
			s.Organizer = schema.CloneMessages(response.Messages)
			return nil
		}),
	); err != nil {
		log.Fatal(err)
	}
	if err := g.AddNode("decode", output,
		graph.WithPreHandler(func(_ context.Context, s *flowState) ([]schema.Message, error) {
			return schema.CloneMessages(s.Organizer), nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *flowState, value Decision) error {
			s.Decision = &value
			return nil
		}),
	); err != nil {
		log.Fatal(err)
	}
	for _, edge := range [][2]string{
		{graph.Start, "work"}, {"work", "organize"}, {"organize", "decode"}, {"decode", graph.End},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			log.Fatal(err)
		}
	}
	compiled, err := g.Compile()
	if err != nil {
		log.Fatal(err)
	}
	result, err := compiled.Invoke(ctx,
		[]schema.Message{schema.UserMessage("审批单 A-1 是否通过？")},
		// 格式选项只定向给整理模型节点，不进入业务 Agent 或 decode。
		runnable.ForNode("organize", llm.WithResponseFormat(output.Format())))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("pipeline: approved=%v reason=%s\n", result.Approved, result.Reason)
	if formatRequests.Load() != 2 {
		log.Fatalf("fixture saw %d structured requests, want 2", formatRequests.Load())
	}
	fmt.Printf("fixture verified: %d requests carried response_format\n", formatRequests.Load())
	if !strings.HasPrefix(decision.Reason, "缺少") {
		log.Fatal("unexpected decision")
	}
}
