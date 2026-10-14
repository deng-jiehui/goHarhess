package decision

import (
	"context"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
)

// KindDecision 是决策事件的观测类别；自定义 Kind 不归入核心枚举。
const KindDecision = observe.Kind("decision")

// Observe 包装决策模型：每次 Invoke 成功后为回答里的每个问题发射一个
// kind=decision 的瞬时事件，Attrs 含 question_id、question_type、confidence，
// 并按问题类型附 selected（choice）、score（score）、p_true（noul）。
func Observe(model Model) Model {
	return observed{inner: model}
}

type observed struct{ inner Model }

func (o observed) Invoke(ctx context.Context, request Request, options ...runnable.Option) (Response, error) {
	response, err := o.inner.Invoke(ctx, request, options...)
	if err != nil {
		return Response{}, err
	}
	for id, answer := range response.Answers {
		emitAnswer(ctx, id, answer)
	}
	return response, nil
}

func emitAnswer(ctx context.Context, id string, answer Answer) {
	if !observe.Active(ctx) {
		return
	}
	attrs := map[string]any{
		"question_id":   id,
		"question_type": string(answer.Type),
		"confidence":    answer.Confidence,
	}
	switch answer.Type {
	case QuestionTypeChoice:
		attrs["selected"] = answer.Choice
	case QuestionTypeScore:
		attrs["score"] = answer.Score
	case QuestionTypeNoul:
		attrs["p_true"] = answer.PTrue
	}
	observe.Emit(ctx, observe.Event{
		Kind:  KindDecision,
		Phase: observe.PhaseEvent,
		Name:  "decision",
		Attrs: attrs,
	})
}

func (o observed) Stream(ctx context.Context, input Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return o.inner.Stream(ctx, input, options...)
}

func (o observed) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	return o.inner.Collect(ctx, input, options...)
}

func (o observed) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return o.inner.Transform(ctx, input, options...)
}
