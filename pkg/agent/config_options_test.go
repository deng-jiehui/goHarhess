package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

func TestAgentInitializationOptionsConfigureCommonFields(t *testing.T) {
	var gotMessages []schema.Message
	var gotOptions llm.Options
	model := llm.NewHandlerModel(func(ctx context.Context, request llm.Request, opts llm.Options) ([]schema.Message, error) {
		gotMessages = request.Messages
		gotOptions = opts
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	value, err := agent.New(model, agent.WithName("configured"),
		agent.WithDescription("configured by options"),
		agent.WithSystemMessage("system policy"),
		agent.WithMaxDepth(5),
		agent.WithTimeout(time.Second),
		agent.WithMaxConcurrency(2),
		agent.WithToolRetries(1),
		agent.WithToolRetryBackoff(0),
		agent.WithMaxTokens(1234),
		agent.WithMaxLLMCalls(4),
		agent.WithCheckpointTimeout(time.Second),
		agent.WithExtraGenerateConfig(llm.GenerateConfig{"temperature": 0.2, "lang": "provider-specific"}))
	if err != nil {
		t.Fatal(err)
	}
	named, ok := value.(runnable.Named)
	if !ok || named.Name() != "configured" || named.Description() != "configured by options" {
		t.Fatalf("agent identity = %#v", value)
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
	var gotOptions llm.Options
	model := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, opts llm.Options) ([]schema.Message, error) {
		gotOptions = opts
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	value, err := agent.New(model)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")}); err != nil {
		t.Fatal(err)
	}
	if value, exists := gotOptions.ExtraGenerateConfig["lang"]; exists {
		t.Fatalf("provider generate config contains implicit lang: %#v", value)
	}
}

func TestWithContextBuilderLambdaReplacesDefaultContextNode(t *testing.T) {
	called := false
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if len(request.Messages) != 1 || request.Messages[0].JoinedText("") != "custom context" {
			return nil, fmt.Errorf("model messages = %#v", request.Messages)
		}
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	builder := runnable.Func[contextengine.Request, contextengine.Result](func(_ context.Context, request contextengine.Request, _ ...runnable.Option) (contextengine.Result, error) {
		called = true
		return contextengine.Result{Messages: []schema.Message{schema.UserMessage("custom context")}}, nil
	})
	value, err := agent.New(model, agent.WithContextBuilder(builder))
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

func TestAgentHistoryCompressorAppliesToDefaultContextBuilder(t *testing.T) {
	store := state.NewMemoryStore()
	var compressedHistory []schema.Message
	modelCalls := 0
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		modelCalls++
		texts := make([]string, 0, len(request.Messages))
		for _, message := range request.Messages {
			texts = append(texts, message.JoinedText(""))
		}
		if len(texts) > 0 && texts[len(texts)-1] == "current" {
			if strings.Contains(strings.Join(texts, "|"), "old history") || strings.Contains(strings.Join(texts, "|"), "old answer") {
				return nil, fmt.Errorf("uncompressed history was sent: %#v", request.Messages)
			}
			if !strings.Contains(strings.Join(texts, "|"), "compressed history") {
				return nil, fmt.Errorf("compressed history was not sent: %#v", request.Messages)
			}
		}
		if modelCalls == 1 {
			return []schema.Message{schema.AssistantMessage("old answer")}, nil
		}
		return []schema.Message{schema.AssistantMessage("new answer")}, nil
	})
	compressor := contextengine.HistoryCompressorFunc(func(_ context.Context, history []schema.Message) ([]schema.Message, error) {
		if len(history) == 0 {
			return history, nil
		}
		compressedHistory = schema.CloneMessages(history)
		return []schema.Message{schema.UserMessage("compressed history")}, nil
	})
	value, err := agent.New(model, agent.WithHistoryCompressor(compressor), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("old history")}, agent.WithConversationID("compressor")); err != nil {
		t.Fatal(err)
	}
	if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("current")}, agent.WithConversationID("compressor")); err != nil {
		t.Fatal(err)
	}
	if len(compressedHistory) != 2 || compressedHistory[0].JoinedText("") != "old history" || compressedHistory[1].JoinedText("") != "old answer" {
		t.Fatalf("compressed history = %#v", compressedHistory)
	}
	checkpoint, found, err := store.Load(context.Background(), "compressor")
	if err != nil || !found || len(checkpoint.Messages) != 4 {
		t.Fatalf("checkpoint found=%t err=%v messages=%#v", found, err, checkpoint.Messages)
	}
	if checkpoint.Messages[0].JoinedText("") != "old history" || checkpoint.Messages[1].JoinedText("") != "old answer" || checkpoint.Messages[2].JoinedText("") != "current" {
		t.Fatalf("checkpoint history was compressed: %#v", checkpoint.Messages)
	}
}

func TestAgentInitializationRejectsUnsafeNumericOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  agent.Option
	}{
		{name: "depth", opt: agent.WithMaxDepth(0)},
		{name: "timeout", opt: agent.WithTimeout(-time.Second)},
		{name: "concurrency", opt: agent.WithMaxConcurrency(1000)},
		{name: "retries", opt: agent.WithToolRetries(-1)},
		{name: "workers negative", opt: agent.WithToolWorkers(-1)},
		{name: "workers over cap", opt: agent.WithToolWorkers(tool.MaxRuntimeWorkers + 1)},
		{name: "backoff", opt: agent.WithToolRetryBackoff(-time.Second)},
		{name: "tokens", opt: agent.WithMaxTokens(0)},
		{name: "LLM calls", opt: agent.WithMaxLLMCalls(0)},
		{name: "checkpoint timeout", opt: agent.WithCheckpointTimeout(-time.Second)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := agent.New(nil, test.opt)
			if err == nil || !strings.Contains(err.Error(), "configure agent") {
				t.Fatalf("agent.New error = %v", err)
			}
		})
	}
}
