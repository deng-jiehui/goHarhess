package app

import (
	"testing"
)

func TestTurnServiceModelSettingsCanSwitchBetweenFixtureAndRemote(t *testing.T) {
	service := NewTurnService(nil, nil, nil)
	if got := service.ModelSettings(); got.Mode != "fixture" || !got.Configured {
		t.Fatalf("默认模型状态=%+v", got)
	}

	remote, err := service.ConfigureModel(ModelConfigInput{
		Mode:     "remote",
		Endpoint: "http://127.0.0.1:19090/v1",
		Model:    "test-model",
		APIKey:   "test-key",
	})
	if err != nil {
		t.Fatalf("配置远程模型失败: %v", err)
	}
	if remote.Mode != "remote" || remote.Endpoint == "" || remote.Model != "test-model" || !remote.Configured {
		t.Fatalf("远程模型状态=%+v", remote)
	}

	fixture, err := service.ConfigureModel(ModelConfigInput{Mode: "fixture"})
	if err != nil {
		t.Fatalf("切回 fixture 失败: %v", err)
	}
	if fixture.Mode != "fixture" || !fixture.Configured || fixture.Endpoint != "" {
		t.Fatalf("fixture 模型状态=%+v", fixture)
	}
}

func TestTurnServiceRejectsIncompleteRemoteModelSettings(t *testing.T) {
	service := NewTurnService(nil, nil, nil)
	for name, input := range map[string]ModelConfigInput{
		"缺少端点": {Mode: "remote", Model: "test-model", APIKey: "key"},
		"缺少模型": {Mode: "remote", Endpoint: "http://127.0.0.1:19090/v1", APIKey: "key"},
		"缺少密钥": {Mode: "remote", Endpoint: "http://127.0.0.1:19090/v1", Model: "test-model"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := service.ConfigureModel(input); err == nil {
				t.Fatal("不完整配置被接受")
			}
		})
	}
}
