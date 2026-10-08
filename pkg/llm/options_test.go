package llm_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

type extModelConfig struct {
	Tag string
}

// extAwareModel 声明并解析自己的扩展配置类型，模拟自定义 Provider。
type extAwareModel struct {
	failures int32
	tags     []string
}

func (m *extAwareModel) OptionTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeFor[extModelConfig]()}
}

func (m *extAwareModel) Invoke(_ context.Context, _ llm.Request, options ...runnable.Option) (llm.Response, error) {
	resolved, remaining, err := runnable.ResolveComponentOptions(extModelConfig{}, options...)
	if err != nil {
		return llm.Response{}, err
	}
	if err := runnable.CheckNoRemainingOptions(remaining...); err != nil {
		return llm.Response{}, err
	}
	m.tags = append(m.tags, resolved.Tag)
	if atomic.AddInt32(&m.failures, 0) > 0 {
		atomic.AddInt32(&m.failures, -1)
		return llm.Response{}, errors.New("temporary")
	}
	return llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("ok:"+resolved.Tag))}}, nil
}
func (m *extAwareModel) Stream(ctx context.Context, request llm.Request, options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.StreamFromInvoke(ctx, request, m.Invoke, options...)
}
func (m *extAwareModel) Collect(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (llm.Response, error) {
	return runnable.CollectFromInvoke(ctx, input, m.Invoke, options...)
}
func (m *extAwareModel) Transform(ctx context.Context, input *runnable.StreamReader[llm.Request], options ...runnable.Option) (*runnable.StreamReader[llm.Response], error) {
	return runnable.TransformFromInvoke(ctx, input, m.Invoke, options...)
}
func (*extAwareModel) Capabilities() llm.Capabilities { return llm.Capabilities{} }
func (*extAwareModel) ModelInfo() llm.ModelInfo       { return llm.ModelInfo{Name: "ext-aware"} }

func withExtTag(tag string) runnable.Option {
	return runnable.WrapOption(func(c *extModelConfig) { c.Tag = tag })
}

// ChatModelNode 把下游模型声明的扩展类型透传给分发层。
func TestChatModelNodeRelaysDownstreamOptionTypes(t *testing.T) {
	node, err := llm.NewChatModelNode(&extAwareModel{})
	if err != nil {
		t.Fatal(err)
	}
	types := node.OptionTypes()
	want := reflect.TypeFor[extModelConfig]()
	found := false
	for _, item := range types {
		if item == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("OptionTypes = %v, missing %v", types, want)
	}
	if _, err := node.Invoke(context.Background(), llm.Request{}, withExtTag("deep")); err != nil {
		t.Fatal(err)
	}
}

// 叶子模型完成全部类型解析后仍检查剩余项：不认识的配置类型在入口报错。
func TestLeafModelRejectsUnknownOptionTypes(t *testing.T) {
	model := llm.NewScriptedModel([]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("ok"))})
	if _, err := model.Invoke(context.Background(), llm.Request{}, withExtTag("x")); err == nil || !strings.Contains(err.Error(), "unrecognized option") {
		t.Fatalf("Invoke error = %v", err)
	}
}

// 重试包装器在边界冻结整份有效配置：设置函数只在冻结时执行一次，
// 之后每次重试应用的是冻结快照，不再重放设置函数。
func TestRetryingModelFreezesExtensionOptions(t *testing.T) {
	var applies int32
	counted := runnable.WrapOption(func(c *extModelConfig) {
		atomic.AddInt32(&applies, 1)
		c.Tag = "frozen"
	})
	base := &extAwareModel{failures: 1}
	model, err := llm.NewRetryingModel(base, llm.RetryConfig{MaxAttempts: 2, Retryable: func(error) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	response, err := model.Invoke(context.Background(), llm.Request{}, counted)
	if err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&applies); got != 1 {
		t.Fatalf("ext option applied %d times, want 1", got)
	}
	if response.Messages[0].JoinedText("") != "ok:frozen" {
		t.Fatalf("response = %#v", response)
	}
	if len(base.tags) != 2 || base.tags[0] != "frozen" || base.tags[1] != "frozen" {
		t.Fatalf("attempt tags = %v", base.tags)
	}
}
