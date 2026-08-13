package framework_test

import (
	"context"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/framework"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
)

func TestAppRunsDefaultAgent(t *testing.T) {
	model := llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")})
	a, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "assistant",
		Model: model,
	})
	if err != nil {
		t.Fatal(err)
	}

	app, err := framework.NewApp("test-app", a)
	if err != nil {
		t.Fatal(err)
	}

	last, err := app.Invoke(context.Background(), []schema.Message{schema.UserMessage("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if len(last) != 1 || last[0].Text() != "ok" {
		t.Fatalf("last = %#v, want assistant ok", last)
	}
}

func TestAppRejectsTypedNilRunnable(t *testing.T) {
	var typedNil *agent.BasicAgent
	if _, err := framework.NewApp("typed-nil", typedNil); err == nil {
		t.Fatal("NewApp accepted a typed nil runnable")
	}
}
