package decision

import (
	"errors"
	"fmt"
	"math"
	"regexp"

	"github.com/goharness/goharness/pkg/runnable"
)

// QuestionType 是决策模型支持的问题类型。
type QuestionType string

const (
	// QuestionTypeChoice 让模型在若干选项上给出概率分布。
	QuestionTypeChoice QuestionType = "choice"
	// QuestionTypeScore 让模型在有序级别上给出概率分布。
	QuestionTypeScore QuestionType = "score"
	// QuestionTypeNoul 让模型对命题成立与否给出概率。
	QuestionTypeNoul QuestionType = "noul"
)

// Option 是 choice 问题的一个候选项。
type Option struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
}

// Question 是向决策模型提出的一个带类型问题。
type Question struct {
	Type         QuestionType
	Instructions string
	// Options 是 choice 问题的候选项：2–255 个，ID 非空且唯一。
	Options []Option
	// Levels 是 score 问题的有序级别：2–10 级。
	Levels []string
}

// Choice 构造一个 choice 问题。
func Choice(instructions string, options ...Option) Question {
	return Question{Type: QuestionTypeChoice, Instructions: instructions, Options: options}
}

// Score 构造一个 score 问题，levels 按从低到高有序排列。
func Score(instructions string, levels ...string) Question {
	return Question{Type: QuestionTypeScore, Instructions: instructions, Levels: levels}
}

// Noul 构造一个 noul 问题（命题成立与否）。
func Noul(instructions string) Question {
	return Question{Type: QuestionTypeNoul, Instructions: instructions}
}

// Request 是一次决策模型调用：可携带任意状态与一组问题。
type Request struct {
	Model string
	// State 原样透传给决策模型（字符串或可序列化结构），本包不负责序列化。
	State any
	// Questions 的键为问题 ID：1–100 字符，仅 [A-Za-z0-9_.-]；最多 32 个。
	Questions map[string]Question
}

var questionIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)

// Validate 校验请求字段；决策模型实现在处理前必须先调用。
func (r Request) Validate() error {
	if len(r.Questions) > 32 {
		return fmt.Errorf("decision: at most 32 questions, got %d", len(r.Questions))
	}
	for id, q := range r.Questions {
		if !questionIDPattern.MatchString(id) {
			return fmt.Errorf("decision: invalid question id %q", id)
		}
		if err := q.Validate(); err != nil {
			return fmt.Errorf("decision: question %q: %w", id, err)
		}
	}
	return nil
}

// Validate 校验单个问题的类型相关约束。
func (q Question) Validate() error {
	switch q.Type {
	case QuestionTypeChoice:
		if len(q.Options) < 2 || len(q.Options) > 255 {
			return fmt.Errorf("choice requires 2-255 options, got %d", len(q.Options))
		}
		seen := make(map[string]bool, len(q.Options))
		for _, option := range q.Options {
			if option.ID == "" {
				return errors.New("choice option id cannot be empty")
			}
			if seen[option.ID] {
				return fmt.Errorf("duplicate choice option id %q", option.ID)
			}
			seen[option.ID] = true
		}
	case QuestionTypeScore:
		if len(q.Levels) < 2 || len(q.Levels) > 10 {
			return fmt.Errorf("score requires 2-10 levels, got %d", len(q.Levels))
		}
	case QuestionTypeNoul:
	default:
		return fmt.Errorf("unknown question type %q", q.Type)
	}
	return nil
}

// Answer 是决策模型对一个问题的回答；按问题类型填相关字段。
type Answer struct {
	Type QuestionType
	// Choice 是概率最高的选项 ID（choice）。
	Choice string
	// Probabilities 是各选项的概率分布（choice）。
	Probabilities map[string]float64
	// Score 是概率加权后的级别位置：0 起算的级别索引，可为小数（score）。
	Score float64
	// PTrue 是命题成立的概率（noul）。
	PTrue float64
	// Confidence 是模型对本次回答的置信度，0..1。
	Confidence float64
}

// Response 是一次决策模型调用的全部回答，键与 Request.Questions 对应。
type Response struct {
	Answers map[string]Answer
}

// ErrInvalidResponse 标记决策模型返回了不合规的回答（缺失、越界或类型不符）。
var ErrInvalidResponse = errors.New("decision: invalid response")

// ValidateAnswer 按问题类型校验单个回答；a.Type 为空视为与 q.Type 相同，
// 非空必须相等。
func (q Question) ValidateAnswer(a Answer) error {
	if a.Type != "" && a.Type != q.Type {
		return fmt.Errorf("answer type %q does not match question type %q", a.Type, q.Type)
	}
	if math.IsNaN(a.Confidence) || a.Confidence < 0 || a.Confidence > 1 {
		return fmt.Errorf("confidence %v out of [0,1]", a.Confidence)
	}
	switch q.Type {
	case QuestionTypeChoice:
		valid := false
		for _, option := range q.Options {
			if option.ID == a.Choice {
				valid = true
			}
		}
		if !valid {
			return fmt.Errorf("choice %q is not an option", a.Choice)
		}
		for id, p := range a.Probabilities {
			if !seen(q.Options, id) {
				return fmt.Errorf("probability key %q is not an option", id)
			}
			if math.IsNaN(p) || p < 0 || p > 1 {
				return fmt.Errorf("probability for %q out of [0,1]: %v", id, p)
			}
		}
	case QuestionTypeScore:
		if math.IsNaN(a.Score) || a.Score < 0 || a.Score > float64(len(q.Levels)-1) {
			return fmt.Errorf("score %v out of [0,%d]", a.Score, len(q.Levels)-1)
		}
	case QuestionTypeNoul:
		if math.IsNaN(a.PTrue) || a.PTrue < 0 || a.PTrue > 1 {
			return fmt.Errorf("p_true %v out of [0,1]", a.PTrue)
		}
	}
	return nil
}

func seen(options []Option, id string) bool {
	for _, option := range options {
		if option.ID == id {
			return true
		}
	}
	return false
}

// ValidateResponse 校验每个被问的问题都必须有回答，且逐个通过
// ValidateAnswer；错误统一以 ErrInvalidResponse 包装并带上问题 ID。
func (r Request) ValidateResponse(resp Response) error {
	for id, q := range r.Questions {
		answer, ok := resp.Answers[id]
		if !ok {
			return fmt.Errorf("%w: missing answer for question %q", ErrInvalidResponse, id)
		}
		if err := q.ValidateAnswer(answer); err != nil {
			return fmt.Errorf("%w: question %q: %v", ErrInvalidResponse, id, err)
		}
	}
	return nil
}

// Model 是决策模型的统一执行契约。
type Model = runnable.Runnable[Request, Response]
