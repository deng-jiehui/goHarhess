package decision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/goharness/goharness/pkg/httpclient"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
)

const providerName = "decision"

// maxHTTPResponseBody 限制决策响应读取上限。
const maxHTTPResponseBody = 16 << 20

// Wire 是 HTTP 信封的字段名映射：各决策服务的顶层字段名可能不同
// （model/state/questions/answers），问题与答案内部字段沿用本包
// 的厂商中立 schema。设为空使用默认值。
type Wire struct {
	ModelField     string // 默认 "model"
	StateField     string // 默认 "state"
	QuestionsField string // 默认 "questions"
	AnswersField   string // 默认 "answers"
}

func (w Wire) withDefaults() Wire {
	if w.ModelField == "" {
		w.ModelField = "model"
	}
	if w.StateField == "" {
		w.StateField = "state"
	}
	if w.QuestionsField == "" {
		w.QuestionsField = "questions"
	}
	if w.AnswersField == "" {
		w.AnswersField = "answers"
	}
	return w
}

// HTTPConfig 是决策模型 HTTP 适配器的配置；Endpoint 为完整 URL。
//
// 信封顶层字段名可经 Wire 配置，但问题与回答的内部字段
// （options/levels/p_true/confidence…）是本包自己的 JSON 形状——
// 没有已知的厂商服务直接接受这套形状；对接具体厂商需要在 Model 外
// 再包一层转换（自己实现 Model，把 Request 转成厂商格式再把回答转回来），
// 或者自建网关。
type HTTPConfig struct {
	// Endpoint 必须是 https URL；仅当主机是回环地址
	// （localhost、127.0.0.0/8、::1）或设置 AllowInsecureHTTP 时允许 http。
	Endpoint string
	APIKey   string
	// Model 是默认模型名；请求里的 Request.Model 优先。
	Model string
	Wire  Wire
	// AllowInsecureHTTP 允许非回环主机的 http Endpoint；仅在明确知道
	// 链路可信时使用（例如自建的同网段网关）。
	AllowInsecureHTTP bool
	Pool              *httpclient.Pool
	Timeout           time.Duration
	Retry             llm.RetryConfig
}

type httpClient struct {
	cfg  HTTPConfig
	wire Wire
	http *http.Client
}

// NewHTTP 构造决策模型 HTTP 客户端：POST JSON 信封，复用 llm 的
// 等量抖动重试与 Retry-After 语义。信封顶层字段名可配（Wire），但
// 内部字段是本包自己的形状——见 HTTPConfig 注释；对接具体厂商需要
// 在 Model 外再包一层转换，或自建网关。
func NewHTTP(cfg HTTPConfig) (Model, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("decision: http adapter requires an endpoint")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("decision: cannot parse endpoint %q", cfg.Endpoint)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !cfg.AllowInsecureHTTP && !isLoopbackHost(u.Hostname()) {
			return nil, fmt.Errorf("decision: http endpoint %q requires a loopback host or AllowInsecureHTTP", cfg.Endpoint)
		}
	default:
		return nil, fmt.Errorf("decision: endpoint scheme %q must be http or https", u.Scheme)
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("decision: Timeout cannot be negative")
	}
	return &httpClient{
		cfg:  cfg,
		wire: cfg.Wire.withDefaults(),
		http: httpclient.NewClient(httpclient.ClientConfig{Pool: cfg.Pool, Timeout: cfg.Timeout}),
	}, nil
}

func (c *httpClient) Invoke(ctx context.Context, request Request, _ ...runnable.Option) (Response, error) {
	if ctx == nil {
		return Response{}, errors.New("decision: invoke context cannot be nil")
	}
	if err := request.Validate(); err != nil {
		return Response{}, err
	}
	model := request.Model
	if model == "" {
		model = c.cfg.Model
	}
	var response Response
	err := llm.CallWithRetry(ctx, c.cfg.Retry, nil, func() error {
		var callErr error
		response, callErr = c.call(ctx, model, request)
		return callErr
	})
	return response, err
}

func (c *httpClient) Stream(ctx context.Context, input Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return runnable.StreamFromInvoke(ctx, input, c.Invoke, options...)
}
func (c *httpClient) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	return runnable.CollectFromInvoke(ctx, input, c.Invoke, options...)
}
func (c *httpClient) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return runnable.TransformFromInvoke(ctx, input, c.Invoke, options...)
}
func (*httpClient) NodeKind() runnable.NodeKind { return runnable.NodeKindRunnable }

// wireQuestion 是发送端的问题编码；字段名厂商中立。
type wireQuestion struct {
	Type         QuestionType `json:"type"`
	Instructions string       `json:"instructions,omitempty"`
	Options      []Option     `json:"options,omitempty"`
	Levels       []string     `json:"levels,omitempty"`
}

// wireAnswer 是接收端的答案编码。
type wireAnswer struct {
	Type          QuestionType       `json:"type,omitempty"`
	Choice        string             `json:"choice,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Score         float64            `json:"score,omitempty"`
	PTrue         float64            `json:"p_true,omitempty"`
	Confidence    float64            `json:"confidence,omitempty"`
}

func (c *httpClient) call(ctx context.Context, model string, request Request) (Response, error) {
	questions := make(map[string]wireQuestion, len(request.Questions))
	for id, q := range request.Questions {
		questions[id] = wireQuestion{Type: q.Type, Instructions: q.Instructions, Options: q.Options, Levels: q.Levels}
	}
	body := map[string]any{
		c.wire.ModelField:     model,
		c.wire.QuestionsField: questions,
	}
	if request.State != nil {
		body[c.wire.StateField] = request.State
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return Response{}, fmt.Errorf("decision: marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return Response{}, fmt.Errorf("decision: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return Response{}, fmt.Errorf("decision: request: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPResponseBody+1))
	if err != nil {
		return Response{}, fmt.Errorf("decision: read response: %w", err)
	}
	if int64(len(data)) > maxHTTPResponseBody {
		return Response{}, fmt.Errorf("decision: response exceeds %d bytes", maxHTTPResponseBody)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return Response{}, httpStatusError(resp, data)
	}
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return Response{}, fmt.Errorf("decision: decode response: %w", err)
	}
	rawAnswers, ok := parsed[c.wire.AnswersField]
	if !ok {
		return Response{}, fmt.Errorf("decision: response missing %q field", c.wire.AnswersField)
	}
	encoded, err := json.Marshal(rawAnswers)
	if err != nil {
		return Response{}, fmt.Errorf("decision: re-encode answers: %w", err)
	}
	var answers map[string]wireAnswer
	if err := json.Unmarshal(encoded, &answers); err != nil {
		return Response{}, fmt.Errorf("decision: decode answers: %w", err)
	}
	response := Response{Answers: make(map[string]Answer, len(answers))}
	for id, a := range answers {
		response.Answers[id] = Answer{
			Type: a.Type, Choice: a.Choice, Probabilities: a.Probabilities,
			Score: a.Score, PTrue: a.PTrue, Confidence: a.Confidence,
		}
	}
	// 空 Type 先按问题类型补齐，再整体校验：缺失或越界即服务违约，
	// 返回 ErrInvalidResponse，不进入重试。
	for id, q := range request.Questions {
		if answer, ok := response.Answers[id]; ok && answer.Type == "" {
			answer.Type = q.Type
			response.Answers[id] = answer
		}
	}
	if err := request.ValidateResponse(response); err != nil {
		return Response{}, err
	}
	return response, nil
}

// isLoopbackHost 报告主机是否为回环地址（localhost、127.0.0.0/8、::1）。
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func httpStatusError(resp *http.Response, body []byte) error {
	message := strings.TrimSpace(string(body))
	if len(message) > 1024 {
		message = message[:1024]
	}
	return llm.NewHTTPStatusError(providerName, resp.StatusCode, resp.Status,
		message, llm.ParseRetryAfter(resp.Header.Get("Retry-After")))
}

var _ Model = (*httpClient)(nil)
