package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"goharness/pkg/agent"
	"goharness/pkg/contextengine"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func TestAgentInitializationOptionsConfigureCommonFields(t *testing.T) {
	var gotMessages []schema.Message
	var gotOptions llm.ChatOptions
	model := llm.NewHandlerModel(func(ctx context.Context, messages []schema.Message, opts llm.ChatOptions) ([]schema.Message, error) {
		gotMessages = messages
		gotOptions = opts
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	value, err := agent.NewBasicAgent(
		agent.BaseConfig{},
		agent.WithName("configured"),
		agent.WithDescription("configured by options"),
		agent.WithSystemMessage("system policy"),
		agent.WithModel(model),
		agent.WithMaxDepth(5),
		agent.WithTimeout(time.Second),
		agent.WithMaxConcurrency(2),
		agent.WithMaxRetries(1),
		agent.WithRetryBackoff(0),
		agent.WithMaxTokens(1234),
		agent.WithMaxLLMCalls(4),
		agent.WithCheckpointTimeout(time.Second),
		agent.WithExtraGenerateConfig(llm.GenerateConfig{"temperature": 0.2, "lang": "provider-specific"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if value.Name() != "configured" || value.Description() != "configured by options" {
		t.Fatalf("agent identity = %q / %q", value.Name(), value.Description())
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}); err != nil {
		t.Fatal(err)
	}
	if len(gotMessages) == 0 || gotMessages[0].Role != schema.RoleSystem || !strings.Contains(gotMessages[0].JoinedText(""), "system policy") {
		t.Fatalf("model messages = %#v", gotMessages)
	}
	if gotOptions.ExtraGenerateConfig["max_tokens"] != 1234 ||
		gotOptions.ExtraGenerateConfig["temperature"] != 0.2 ||
		gotOptions.ExtraGenerateConfig["lang"] != "provider-specific" {
		t.Fatalf("generate config = %#v", gotOptions.ExtraGenerateConfig)
	}
}

func TestAgentLanguageDoesNotBecomeProviderGenerateConfig(t *testing.T) {
	tests := []struct {
		name string
		text string
		opts []agent.RunOption
	}{
		{name: "auto detected", text: "hello"},
		{name: "explicit run language", text: "hello", opts: []agent.RunOption{agent.WithLang("zh")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var gotOptions llm.ChatOptions
			model := llm.NewHandlerModel(func(_ context.Context, _ []schema.Message, opts llm.ChatOptions) ([]schema.Message, error) {
				gotOptions = opts
				return []schema.Message{schema.AssistantMessage("ok")}, nil
			})
			value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model})
			if err != nil {
				t.Fatal(err)
			}
			options := make([]runnable.Option, len(test.opts))
			for index, option := range test.opts {
				options[index] = option
			}
			if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage(test.text)}, options...); err != nil {
				t.Fatal(err)
			}
			if value, exists := gotOptions.ExtraGenerateConfig["lang"]; exists {
				t.Fatalf("provider generate config contains implicit lang: %#v", value)
			}
		})
	}
}

func TestWithContextBuilderLambdaReplacesDefaultContextNode(t *testing.T) {
	called := false
	model := llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
		if len(messages) != 1 || messages[0].JoinedText("") != "custom context" {
			return nil, fmt.Errorf("model messages = %#v", messages)
		}
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	builder := runnable.Func[contextengine.Request, contextengine.Result](func(_ context.Context, request contextengine.Request, _ ...runnable.Option) (contextengine.Result, error) {
		called = true
		return contextengine.Result{Messages: []schema.Message{schema.UserMessage("custom context")}}, nil
	})
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model}, agent.WithContextBuilder(builder))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("original")}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("custom context builder was not invoked")
	}
}

func TestDefaultContextBuilderIsAStandardRunnable(t *testing.T) {
	builder, err := contextengine.NewContextBuilder(contextengine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	var _ runnable.Runnable[contextengine.Request, contextengine.Result] = builder
}

func TestAgentInitializationRejectsUnsafeNumericOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  agent.Option
	}{
		{name: "depth", opt: agent.WithMaxDepth(0)},
		{name: "timeout", opt: agent.WithTimeout(-time.Second)},
		{name: "concurrency", opt: agent.WithMaxConcurrency(1000)},
		{name: "retries", opt: agent.WithMaxRetries(-1)},
		{name: "backoff", opt: agent.WithRetryBackoff(-time.Second)},
		{name: "tokens", opt: agent.WithMaxTokens(0)},
		{name: "LLM calls", opt: agent.WithMaxLLMCalls(0)},
		{name: "checkpoint timeout", opt: agent.WithCheckpointTimeout(-time.Second)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := agent.NewBasicAgent(agent.BaseConfig{}, test.opt)
			if err == nil || !strings.Contains(err.Error(), "configure agent") {
				t.Fatalf("NewBasicAgent error = %v", err)
			}
		})
	}
}
