// 声明式（YAML）Agent 示例（W5-8）：把 YAML 映射到 agent.New 的
// Option 装配，agents: 引用经 subagent.AsTool 挂成专家工具，
// version 写入 WithDefinitionVersion（定义变更后旧检查点拒绝恢复）。
//
// 框架不内置 YAML 加载器——加载与校验属于应用层；本例演示一种映射。
// 加载不可信 YAML 时的护栏清单见文件末尾的 untrustedInputRules。
//
//	go run .   （内置示例 YAML 与脚本化模型，离线可跑）
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/goharness/goharness/extensions/subagent"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// spec 是一份 YAML Agent 声明。
type spec struct {
	Name         string   `yaml:"name"`
	Version      string   `yaml:"version"`
	Instructions string   `yaml:"instructions"`
	Model        string   `yaml:"model"`
	MaxLLMCalls  int      `yaml:"max_llm_calls"`
	MaxTokens    int      `yaml:"max_tokens"`
	Timeout      string   `yaml:"timeout"`
	Tools        []string `yaml:"tools"`
	Agents       map[string]struct {
		Model       string   `yaml:"model"`
		Tools       []string `yaml:"tools"`
		Description string   `yaml:"description"`
	} `yaml:"agents"`
}

// registry 是租户放行的组件注册表：YAML 只能引用其中的名字，
// 未知名字报错而不是静默忽略。
type registry struct {
	models map[string]llm.ChatModel
	tools  map[string]tool.Tool
}

const (
	maxInstructionsRunes = 4096 // 指令长度上限：声明式文件不该携带超长提示词
	maxNameRunes         = 64
)

// build 把 spec 装配成 Agent；所有数值上限向下收紧到注册表侧可接受的范围。
func build(s spec, reg registry) (agent.Agent, error) {
	if len(s.Name) > maxNameRunes || len(s.Instructions) > maxInstructionsRunes {
		return nil, fmt.Errorf("yamlagent: name/instructions exceed limits")
	}
	model, ok := reg.models[s.Model]
	if !ok {
		return nil, fmt.Errorf("yamlagent: unknown model %q (allowed: %s)", s.Model, keys(reg.models))
	}
	opts := []agent.Option{
		agent.WithName(s.Name), agent.WithSystemMessage(s.Instructions),
		agent.WithDefinitionVersion(s.Version), // 定义版本写进执行记录
	}
	// 参数封顶：只在声明时追加，且向下收紧到服务可承受值。
	if s.MaxLLMCalls > 0 {
		opts = append(opts, agent.WithMaxLLMCalls(min(s.MaxLLMCalls, 16)))
	}
	if s.MaxTokens > 0 {
		opts = append(opts, agent.WithMaxTokens(min(s.MaxTokens, 8192)))
	}
	if d, err := time.ParseDuration(s.Timeout); err == nil && s.Timeout != "" {
		opts = append(opts, agent.WithTimeout(min(d, 5*time.Minute)))
	}
	for _, name := range s.Tools {
		t, ok := reg.tools[name]
		if !ok {
			return nil, fmt.Errorf("yamlagent: unknown tool %q (allowed: %s)", name, keys(reg.tools))
		}
		opts = append(opts, agent.WithTool(t))
	}
	// agents: 引用映射为 AsTool 专家——同一注册表递归装配。
	for name, sub := range s.Agents {
		expert, err := build(spec{
			Name: name, Instructions: sub.Description, Model: sub.Model,
			Tools: sub.Tools, MaxLLMCalls: s.MaxLLMCalls,
		}, reg)
		if err != nil {
			return nil, fmt.Errorf("agent %q: %w", name, err)
		}
		opts = append(opts, agent.WithTool(subagent.AsTool(expert, name, sub.Description)))
	}
	return agent.New(model, opts...)
}

func keys[V any](m map[string]V) string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	return strings.Join(names, ", ")
}

// ---- 演示：注册表 + 内置 YAML + 脚本化模型 ---------------------------------

const demoYAML = `
name: helpdesk
version: "1.0"
instructions: 你是客服助手，数据库问题交给专家。
model: main
max_llm_calls: 4
tools: [echo]
agents:
  db_expert:
    model: main
    tools: [echo]
    description: 数据库问题专家
`

func main() {
	reg := registry{
		// 父代理与专家共用同一脚本化模型，脚本按调用顺序消费：
		// ① 父→db_expert ② 专家→echo ③ 专家→答复 ④ 父→汇总。
		models: map[string]llm.ChatModel{"main": llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("c1", "db_expert", `{"task":"解释慢查询"}`))},
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("c2", "echo", `{"text":"expert answer"}`))},
			[]schema.Message{schema.AssistantMessage("加索引可解决")},
			[]schema.Message{schema.AssistantMessage("专家答复已汇总：加索引可解决")})},
		tools: map[string]tool.Tool{"echo": tool.NewFunctionTool(
			tool.Spec{Name: "echo", IsReadOnly: true},
			func(_ context.Context, in tool.CallInput) (tool.Result, error) {
				return tool.TextResult("echo: " + in.RawArgs), nil
			})},
	}
	var s spec
	if err := yaml.Unmarshal([]byte(demoYAML), &s); err != nil {
		log.Fatal(err)
	}
	a, err := build(s, reg)
	if err != nil {
		log.Fatal(err)
	}
	out, err := a.Invoke(context.Background(), []schema.Message{schema.UserMessage("帮我看数据库性能")})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out[len(out)-1].JoinedText(""))

	// 未知组件名被拒绝（不可信输入护栏演示）：
	bad := spec{Name: "x", Model: "gpt-9000", Instructions: "y"}
	if _, err := build(bad, reg); err != nil {
		fmt.Println("未知模型被拒：", err)
	}
}

// untrustedInputRules 是加载不可信 YAML 时的最小护栏清单：
//
//  1. 工具与模型只能引用注册表内名字；超范围立即报错（上面 build 已做）。
//  2. 参数封顶：max_llm_calls/max_tokens/timeout 向下收紧到服务可承受值。
//  3. 指令长度上限：instructions/name 按 rune 数封顶，防提示词膨胀。
//  4. version 必填且写进 WithDefinitionVersion——声明变了旧挂起运行拒绝恢复。
//  5. YAML 文件本身不应来自终端用户输入；多人共享的场景按租户白名单
//     限定可用的 tools/models/roles 集合。
//  6. 混入 MCP 工具时启用严格模式（Policy）——远端只读注解默认不信任，
//     主机白名单与私网地址拦截打开，见 docs/extensions.md 的 MCP 一节。
var untrustedInputRules = 6
