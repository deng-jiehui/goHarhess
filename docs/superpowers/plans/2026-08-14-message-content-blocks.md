# Message Content Blocks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed-field message model with one ordered content-block model that preserves text, multimodal content, reasoning, function calls, function results, opaque provider data and response metadata through runtime and persistence boundaries.

**Architecture:** `schema.Message.ContentBlocks` becomes the only content source. Provider adapters decode and encode ordered blocks, ToolsNode consumes typed function blocks, Context Engine transforms blocks without flattening them, and checkpoints/caches persist the same schema. The migration is intentionally breaking and removes the old `Content`, `ContentItem`, `ReasoningContent`, top-level `FunctionCall`, `Name`, `FunctionID` and `Text()` APIs.

**Tech Stack:** Go, standard-library `encoding/json`, existing generic Runnable/StreamReader APIs, existing Go test suite.

## Global Constraints

- The final `schema.Message` fields are exactly `Role`, `ContentBlocks`, `ResponseMeta` and `Extra`.
- `ContentBlocks` is the only writable content representation after Task 7.
- Preserve block order through JSON, cloning, streaming, tools, context assembly and checkpoints.
- Use `RoleTool`; remove `RoleFunction` after all callers migrate.
- Keep function arguments as strings and opaque provider payloads as `json.RawMessage`.
- Do not add prompt-cache policy, replay guarantees, transcript/branch/fingerprint abstractions or new providers.
- Do not preserve legacy message JSON or old accessor behavior.
- Do not silently drop unknown or unsupported blocks.
- Preserve unrelated user changes in the dirty worktree; stage only files owned by the current task.
- Use TDD for every task and run the task-specific test gate before committing.
- The repository-wide suite is required only after the breaking migration is complete in Task 7.

---

## File Structure

- `pkg/schema/block.go`: block types, payloads, constructors, validation and defensive payload accessors.
- `pkg/schema/response_meta.go`: persisted response metadata and provider-neutral token usage.
- `pkg/schema/message.go`: message roles, message construction, JSON, validation and deep cloning.
- `pkg/llm/openai_compatible.go`: OpenAI-compatible block wire conversion and usage decoding.
- `pkg/llm/delta.go`: position-preserving block delta accumulation.
- `pkg/tool/tool.go`: block-based tool results.
- `pkg/tool/node.go`: block-based call extraction and result message construction.
- `pkg/contextengine/token.go`: block token accounting and text-block truncation.
- `pkg/contextengine/engine.go`: block-aware normalization, merging and trimming.
- `pkg/contextengine/builder.go`: block-first conversation construction.
- `pkg/agent/*`: Agent recipes and routing migrated to explicit block queries.
- `extensions/*`, `pkg/state/*`, `pkg/graph/*`: persistence, extension and graph callers migrated to the new schema.
- `docs/*`, `README.md`: public examples and architecture descriptions migrated after code passes.

---

### Task 1: Add the Ordered Block Schema

**Files:**
- Create: `pkg/schema/block.go`
- Create: `pkg/schema/block_test.go`
- Create: `pkg/schema/response_meta.go`
- Modify: `pkg/schema/message.go`
- Modify: `pkg/schema/message_test.go`

**Interfaces:**
- Produces: `ContentBlockType`, `ContentBlock`, `Text`, `MediaContent`, `Reasoning`, `FunctionToolCall`, `FunctionToolResult`, `OpaqueContent`, `ResponseMeta`, `TokenUsage`.
- Produces: `NewMessage(Role, ...ContentBlock) Message` and one constructor per built-in block.
- Produces: `TextBlocks`, `JoinedText`, `ReasoningBlocks`, `FunctionToolCalls`, and `FunctionToolResults` defensive accessors.
- Produces: block-aware `HasChinese`, `LastUserText`, and `ExtractFiles` helpers used by Agent and ToolsNode.
- Transitional constraint: retain old message fields and `Content` only until Task 7 so unmigrated struct literals still compile. Change `NewMessage` and the text-message constructors to blocks in this task; package tests outside the active migration gate may fail until their owning task migrates them.

- [ ] **Step 1: Write failing block round-trip and ordering tests**

Add tests that construct interleaved blocks, marshal the message, unmarshal it, and assert order and payload fidelity:

```go
func TestMessageContentBlocksRoundTripInOrder(t *testing.T) {
	raw := json.RawMessage(`{"provider_type":"server_tool_use","value":7}`)
	message := schema.NewMessage(schema.RoleAssistant,
		schema.NewReasoningBlock("plan", "sig-1"),
		schema.NewTextBlock("checking"),
		schema.NewFunctionToolCallBlock("call-1", "weather", `{"city":"Shanghai"}`),
		schema.NewOpaqueBlock("provider.server_tool_use", raw),
	)
	message.ResponseMeta = &schema.ResponseMeta{
		ID: "resp-1", Model: "model-1", FinishReason: "tool_calls",
		Usage: &schema.TokenUsage{InputTokens: 10, OutputTokens: 4, CacheReadTokens: 8},
	}

	data, err := json.Marshal(message)
	if err != nil { t.Fatal(err) }
	var got schema.Message
	if err := json.Unmarshal(data, &got); err != nil { t.Fatal(err) }
	if err := got.Validate(); err != nil { t.Fatal(err) }
	if len(got.ContentBlocks) != 4 { t.Fatalf("blocks=%d", len(got.ContentBlocks)) }
	if got.ContentBlocks[0].Type != schema.ContentBlockTypeReasoning ||
		got.ContentBlocks[1].Type != schema.ContentBlockTypeText ||
		got.ContentBlocks[2].Type != schema.ContentBlockTypeFunctionToolCall ||
		got.ContentBlocks[3].Type != schema.ContentBlockTypeOpaque {
		t.Fatalf("order=%#v", got.ContentBlocks)
	}
	if string(got.ContentBlocks[3].Opaque.Raw) != string(raw) { t.Fatalf("raw=%s", got.ContentBlocks[3].Opaque.Raw) }
	if got.ResponseMeta.Usage.CacheReadTokens != 8 { t.Fatalf("meta=%#v", got.ResponseMeta) }
}
```

- [ ] **Step 2: Run the schema test to verify it fails**

Run: `go test -count=1 ./pkg/schema -run TestMessageContentBlocksRoundTripInOrder`

Expected: FAIL because block and response metadata types do not exist.

- [ ] **Step 3: Implement block and response metadata types**

Implement these exact exported types and constants:

```go
type ContentBlockType string

const (
	ContentBlockTypeText               ContentBlockType = "text"
	ContentBlockTypeImage              ContentBlockType = "image"
	ContentBlockTypeFile               ContentBlockType = "file"
	ContentBlockTypeAudio              ContentBlockType = "audio"
	ContentBlockTypeVideo              ContentBlockType = "video"
	ContentBlockTypeReasoning          ContentBlockType = "reasoning"
	ContentBlockTypeFunctionToolCall   ContentBlockType = "function_tool_call"
	ContentBlockTypeFunctionToolResult ContentBlockType = "function_tool_result"
	ContentBlockTypeOpaque             ContentBlockType = "opaque"
)

type ContentBlock struct {
	Type ContentBlockType `json:"type"`
	Text *Text `json:"text,omitempty"`
	Image *MediaContent `json:"image,omitempty"`
	File *MediaContent `json:"file,omitempty"`
	Audio *MediaContent `json:"audio,omitempty"`
	Video *MediaContent `json:"video,omitempty"`
	Reasoning *Reasoning `json:"reasoning,omitempty"`
	FunctionToolCall *FunctionToolCall `json:"function_tool_call,omitempty"`
	FunctionToolResult *FunctionToolResult `json:"function_tool_result,omitempty"`
	Opaque *OpaqueContent `json:"opaque,omitempty"`
}
```

Add the payloads and constructors specified by the design. Use this constructor signature for tool results:

```go
type Text struct {
	Text  string         `json:"text"`
	Extra map[string]any `json:"extra,omitempty"`
}

type MediaContent struct {
	URI      string          `json:"uri,omitempty"`
	MIMEType string          `json:"mime_type,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
	Extra    map[string]any  `json:"extra,omitempty"`
}

type Reasoning struct {
	Text      string         `json:"text,omitempty"`
	Signature string         `json:"signature,omitempty"`
	Extra     map[string]any `json:"extra,omitempty"`
}

type FunctionToolCall struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments string         `json:"arguments"`
	Extra     map[string]any `json:"extra,omitempty"`
}

type FunctionToolResult struct {
	CallID        string         `json:"call_id"`
	Name          string         `json:"name"`
	ContentBlocks []ContentBlock `json:"content_blocks,omitempty"`
	IsError       bool           `json:"is_error,omitempty"`
	Extra         map[string]any `json:"extra,omitempty"`
}

type OpaqueContent struct {
	Kind string          `json:"kind"`
	Raw  json.RawMessage `json:"raw"`
}

func NewTextBlock(text string) ContentBlock
func NewImageBlock(uri, mimeType string) ContentBlock
func NewFileBlock(uri, mimeType string) ContentBlock
func NewAudioBlock(uri, mimeType string) ContentBlock
func NewVideoBlock(uri, mimeType string) ContentBlock
func NewReasoningBlock(text, signature string) ContentBlock
func NewFunctionToolCallBlock(id, name, arguments string) ContentBlock
func NewFunctionToolResultBlock(callID, name string, isError bool, blocks ...ContentBlock) ContentBlock
func NewOpaqueBlock(kind string, raw json.RawMessage) ContentBlock
```

Add response metadata with these exact fields:

```go
type ResponseMeta struct {
	ID           string         `json:"id,omitempty"`
	Model        string         `json:"model,omitempty"`
	FinishReason string         `json:"finish_reason,omitempty"`
	Usage        *TokenUsage    `json:"usage,omitempty"`
	Extra        map[string]any `json:"extra,omitempty"`
}

type TokenUsage struct {
	InputTokens      int            `json:"input_tokens,omitempty"`
	OutputTokens     int            `json:"output_tokens,omitempty"`
	TotalTokens      int            `json:"total_tokens,omitempty"`
	ReasoningTokens  int            `json:"reasoning_tokens,omitempty"`
	CacheReadTokens  int            `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int            `json:"cache_write_tokens,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
}
```

Change `NewMessage` immediately to `func NewMessage(role Role, blocks ...ContentBlock) Message`. Reimplement `TextMessage`, `SystemMessage`, `UserMessage`, and `AssistantMessage` with `NewTextBlock`. Keep the legacy fields and `FunctionMessage` only as temporary migration scaffolding; do not make `NewMessage` accept `any` or both content models.

- [ ] **Step 4: Add failing validation and clone-isolation tests**

Cover mismatched tagged-union payloads, two payloads in one block, invalid opaque JSON, missing call IDs/names, finalized function arguments that are not a JSON object, media blocks with zero or two sources, nested result paths, negative usage counts, and clone isolation:

```go
func TestCloneMessagesIsolatesNestedBlocksAndMetadata(t *testing.T) {
	source := schema.NewMessage(schema.RoleTool,
		schema.NewFunctionToolResultBlock("call-1", "weather", false,
			schema.NewTextBlock("sunny"),
		),
	)
	source.ResponseMeta = &schema.ResponseMeta{Extra: map[string]any{"trace": map[string]any{"id": "a"}}}
	cloned, err := schema.CloneMessagesStrict([]schema.Message{source})
	if err != nil { t.Fatal(err) }
	cloned[0].ContentBlocks[0].FunctionToolResult.ContentBlocks[0].Text.Text = "rain"
	cloned[0].ResponseMeta.Extra["trace"].(map[string]any)["id"] = "b"
	if source.ContentBlocks[0].FunctionToolResult.ContentBlocks[0].Text.Text != "sunny" { t.Fatal("nested block shares state") }
	if source.ResponseMeta.Extra["trace"].(map[string]any)["id"] != "a" { t.Fatal("metadata shares state") }
}
```

- [ ] **Step 5: Implement validation, cloning and accessors**

Add:

```go
func (b ContentBlock) Validate() error
func (m Message) TextBlocks() []Text
func (m Message) JoinedText(separator string) string
func (m Message) ReasoningBlocks() []Reasoning
func (m Message) FunctionToolCalls() []FunctionToolCall
func (m Message) FunctionToolResults() []FunctionToolResult
```

Make accessors top-level only and deeply clone returned `Extra`, raw bytes and nested blocks. Extend `CloneMessages` and `CloneMessagesStrict` to copy new fields while legacy fields still exist transiently.

Rewrite `HasChinese` to inspect top-level text blocks, `LastUserText` to return `JoinedText("")` for the last user message, and `ExtractFiles` to collect URI values from file blocks plus image blocks when `includeImages` is true. Add focused tests proving media URIs are not included in joined text.

- [ ] **Step 6: Run the schema package gate**

Run: `go test -count=1 ./pkg/schema`

Expected: PASS.

- [ ] **Step 7: Commit the schema foundation**

```bash
git add pkg/schema/block.go pkg/schema/block_test.go pkg/schema/response_meta.go pkg/schema/message.go pkg/schema/message_test.go
git commit -m "feat(schema): add ordered message blocks"
```

---

### Task 2: Migrate LLM Responses, Streaming and OpenAI Conversion

**Files:**
- Modify: `pkg/llm/model.go`
- Modify: `pkg/llm/delta.go`
- Modify: `pkg/llm/delta_test.go`
- Modify: `pkg/llm/openai_compatible.go`
- Create: `pkg/llm/openai_compatible_internal_test.go`
- Modify: `pkg/llm/openai_compatible_test.go`
- Modify: `pkg/llm/stream_standardizer.go`
- Modify: `pkg/llm/response_stream.go`
- Modify: `pkg/llm/response_stream_test.go`
- Modify: `pkg/llm/mock.go`
- Modify: `pkg/llm/callback_test.go`
- Modify: `pkg/llm/chat_model_node_test.go`
- Modify: `pkg/llm/retry_test.go`

**Interfaces:**
- Consumes: ordered schema blocks and `schema.TokenUsage` from Task 1.
- Produces: `type Usage = schema.TokenUsage`.
- Produces: OpenAI decoding that returns one assistant message with ordered text, reasoning and function-call blocks.
- Produces: `encodeOpenAIMessages([]schema.Message, bool) ([]map[string]any, error)`.
- Produces: block-position delta accumulation and final `ResponseMeta` attachment.

- [ ] **Step 1: Replace OpenAI tests with block-native expectations**

Create `openai_compatible_internal_test.go` with `package llm`, then add a non-streaming response fixture with two tool calls and cache usage:

```go
func TestOpenAIResponseKeepsParallelCallsInOneMessage(t *testing.T) {
	data := []byte(`{
	  "id":"resp-1","model":"model-1",
	  "choices":[{"index":0,"finish_reason":"tool_calls","message":{
	    "role":"assistant","content":"checking","reasoning_content":"plan",
	    "tool_calls":[
	      {"id":"call-1","type":"function","function":{"name":"weather","arguments":"{\"city\":\"Shanghai\"}"}},
	      {"id":"call-2","type":"function","function":{"name":"air","arguments":"{\"city\":\"Shanghai\"}"}}
	    ]
	  }}],
	  "usage":{"prompt_tokens":20,"completion_tokens":8,"total_tokens":28,
	    "prompt_tokens_details":{"cached_tokens":16,"cache_write_tokens":4},
	    "completion_tokens_details":{"reasoning_tokens":3}}
	}`)
	messages, usage, finish, err := decodeOpenAIResponse(data, false)
	if err != nil { t.Fatal(err) }
	if len(messages) != 1 { t.Fatalf("messages=%d", len(messages)) }
	calls := messages[0].FunctionToolCalls()
	if len(calls) != 2 || calls[0].ID != "call-1" || calls[1].ID != "call-2" { t.Fatalf("calls=%#v", calls) }
	if usage.CacheReadTokens != 16 || usage.CacheWriteTokens != 4 || finish != "tool_calls" { t.Fatalf("usage=%#v finish=%q", usage, finish) }
}
```

Update request-encoding tests to build block messages and require multiple call blocks to encode as one `tool_calls` array. Add a case where an opaque block returns an error containing the message index, block index and type.

- [ ] **Step 2: Run focused OpenAI tests and verify failure**

Run: `go test -count=1 ./pkg/llm -run 'Test(OpenAIResponseKeepsParallelCallsInOneMessage|OpenAICompatible.*Block|OpenAICompatible.*Usage)'`

Expected: FAIL because decoding still splits calls and usage lacks cache counters.

- [ ] **Step 3: Migrate usage and OpenAI wire types**

Replace `llm.Usage` with:

```go
type Usage = schema.TokenUsage
```

Map OpenAI names as follows:

```text
prompt_tokens                              -> InputTokens
completion_tokens                          -> OutputTokens
total_tokens                               -> TotalTokens
completion_tokens_details.reasoning_tokens -> ReasoningTokens
prompt_tokens_details.cached_tokens        -> CacheReadTokens
prompt_tokens_details.cache_write_tokens   -> CacheWriteTokens
```

Extend `openAIResponse` with response `id` and `model`. Decode one choice into one message. Chat Completions exposes reasoning, content and tool calls as separate fields rather than one interleaved array, so normalize them in the deterministic order `reasoning`, `text`, then ordered `tool_calls`. Do not create one schema message per call.

- [ ] **Step 4: Implement block-native OpenAI encoding**

Change the encoder signature:

```go
func encodeOpenAIMessages(messages []schema.Message, includeReasoning bool) ([]map[string]any, error)
```

Encoding rules:

```text
RoleTool + one FunctionToolResult -> role=tool, tool_call_id, joined top-level text content
Text blocks                         -> content in block order
Reasoning blocks                    -> reasoning_content when enabled
FunctionToolCall blocks             -> one ordered tool_calls array
Opaque or unsupported media         -> typed error with message/block path
```

Propagate the new error from request construction before sending HTTP.

- [ ] **Step 5: Write failing block delta tests**

Replace message-slot tool-call delta tests with one assistant message containing sparse block positions:

```go
func TestDeltaAccumulatorMergesFunctionBlocksByPosition(t *testing.T) {
	acc := llm.NewDeltaAccumulator(true)
	first := llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("Hel"),
		schema.NewFunctionToolCallBlock("call-1", "wea", `{"city":`),
		schema.NewFunctionToolCallBlock("call-2", "a", `{"city":`),
	)}}
	second := llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("lo"),
		schema.NewFunctionToolCallBlock("", "ther", `"Shanghai"}`),
		schema.NewFunctionToolCallBlock("", "ir", `"Shanghai"}`),
	)}}
	if _, err := acc.Apply(first); err != nil { t.Fatal(err) }
	messages, err := acc.Apply(second)
	if err != nil { t.Fatal(err) }
	if messages[0].JoinedText("") != "Hello" { t.Fatalf("text=%q", messages[0].JoinedText("")) }
	calls := messages[0].FunctionToolCalls()
	if calls[0].Name != "weather" || calls[1].Name != "air" { t.Fatalf("calls=%#v", calls) }
}
```

- [ ] **Step 6: Implement block delta merging and response metadata attachment**

Merge blocks by slice position and matching `ContentBlockType`. Append text, reasoning text/signature, function name and argument fragments with the existing cumulative-delta guard. Reject a type change at an existing block position. Partial stream blocks may omit IDs, names or valid final JSON; only the reduced final message runs `Message.Validate`.

Carry `responseID` and `model` on private `wireEvent` values. At a completed response, call a private helper with this exact signature:

```go
func attachResponseMeta(messages []schema.Message, responseID, model, finishReason string, usage *Usage) []schema.Message
```

The helper clones the messages and attaches metadata to the sole generated message:

```go
message.ResponseMeta = &schema.ResponseMeta{
	ID: responseID,
	Model: model,
	FinishReason: response.FinishReason,
	Usage: cloneUsage(response.Usage),
}
```

Use the same helper in non-streaming decode and after the final streaming event. Keep the existing `decodeOpenAIResponse(data, delta) ([]schema.Message, *Usage, string, error)` signature; ID and model are written directly into message metadata. Keep invocation-level fields on `llm.Response` for callbacks and streaming. Update mocks and tests to use `NewMessage` and `JoinedText`.

- [ ] **Step 7: Run the LLM package gate**

Run: `go test -count=1 ./pkg/llm`

Expected: PASS.

- [ ] **Step 8: Commit the LLM migration**

```bash
git add pkg/llm
git commit -m "refactor(llm): carry ordered message blocks"
```

---

### Task 3: Migrate Tool Results and ToolsNode

**Files:**
- Modify: `pkg/tool/tool.go`
- Modify: `pkg/tool/tool_test.go`
- Modify: `pkg/tool/node.go`
- Modify: `pkg/tool/node_test.go`
- Modify: `pkg/tool/batch_hook.go`
- Modify: `pkg/tool/batch_hook_test.go`
- Modify: `pkg/tool/callback.go`
- Modify: `pkg/tool/callback_test.go`
- Modify: `pkg/tool/runtime_test.go`
- Modify: `pkg/tool/runtime_http_test.go`
- Modify: `pkg/tool/errors.go`

**Interfaces:**
- Consumes: `FunctionToolCall` and `FunctionToolResult` blocks.
- Produces: `tool.Result.ContentBlocks []schema.ContentBlock` and `ToContentBlocks() ([]schema.ContentBlock, error)`.
- Produces: `TailToolCalls` that extracts every call block from the assistant-call suffix in block order.
- Produces: one `RoleTool` result message per call with typed result correlation and error state.

- [ ] **Step 1: Write failing tool-result conversion tests**

Replace `Content` assertions with blocks and add structured data coverage:

```go
func TestResultToContentBlocksPreservesExplicitBlocks(t *testing.T) {
	result := tool.Result{ContentBlocks: []schema.ContentBlock{
		schema.NewTextBlock("first"),
		schema.NewImageBlock("https://example.test/a.png", "image/png"),
	}}
	blocks, err := result.ToContentBlocks()
	if err != nil { t.Fatal(err) }
	if len(blocks) != 2 || blocks[0].Text.Text != "first" { t.Fatalf("blocks=%#v", blocks) }
}
```

Also assert `DataResult(map[string]any{"ok": true})` produces one indented JSON text block.

- [ ] **Step 2: Run focused result tests and verify failure**

Run: `go test -count=1 ./pkg/tool -run 'TestResultToContentBlocks|TestResultData'`

Expected: FAIL because `Result` still exposes `schema.Content`.

- [ ] **Step 3: Implement the block-based Result API**

Use:

```go
type Result struct {
	ContentBlocks []schema.ContentBlock
	Data          any
	IsError       bool
}

func TextResult(text string) Result
func BlockResult(blocks ...schema.ContentBlock) Result
func DataResult(data any) Result
func ErrorResult(blocks ...schema.ContentBlock) Result
func (r Result) ToContentBlocks() ([]schema.ContentBlock, error)
```

Return defensive copies. Keep structured-data JSON formatting identical to the current two-space indentation.

- [ ] **Step 4: Write failing multi-call ToolsNode tests**

Construct one assistant message with text plus two calls and assert extraction and results preserve source order:

```go
func TestToolsNodeExecutesMultipleBlocksFromOneMessage(t *testing.T) {
	firstTool := tool.NewFunctionTool(tool.Spec{Name: "first"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("one"), nil
	})
	secondTool := tool.NewFunctionTool(tool.Spec{Name: "second"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("two"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{firstTool, secondTool}})
	if err != nil { t.Fatal(err) }
	input := []schema.Message{schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("run both"),
		schema.NewFunctionToolCallBlock("call-1", "first", `{}`),
		schema.NewFunctionToolCallBlock("call-2", "second", `{}`),
	)}
	output, err := node.Invoke(context.Background(), input)
	if err != nil { t.Fatal(err) }
	if len(output) != 2 { t.Fatalf("output=%d", len(output)) }
	first := output[0].FunctionToolResults()
	second := output[1].FunctionToolResults()
	if first[0].CallID != "call-1" || second[0].CallID != "call-2" { t.Fatalf("results=%#v %#v", first, second) }
}
```

- [ ] **Step 5: Implement block-native extraction and result messages**

`TailToolCalls` walks backward over contiguous assistant messages. For each message, collect all `FunctionToolCalls()` in block order. Stop at the first non-assistant message or assistant message without a call block. Remove `FunctionID` and `Name` lookups from this path.

Build results with:

```go
schema.NewMessage(schema.RoleTool,
	schema.NewFunctionToolResultBlock(call.FunctionID, call.Name, isError, blocks...),
)
```

Keep `tool.ToolCall.FunctionID` as the execution-layer correlation field for this migration; it is populated from `FunctionToolCall.ID`.

- [ ] **Step 6: Migrate hooks, callbacks and remaining tool tests**

Replace `Result.Content`, `schema.TextContent`, `Message.Text`, `Message.Name`, and `Message.FunctionID` assertions with typed block construction and access. Ensure `CallInput.Messages` still receives defensive message clones.

- [ ] **Step 7: Run the tool package gate**

Run: `go test -count=1 ./pkg/tool`

Expected: PASS.

- [ ] **Step 8: Commit the tool migration**

```bash
git add pkg/tool
git commit -m "refactor(tool): use typed message blocks"
```

---

### Task 4: Make Context Assembly Block-Aware

**Files:**
- Modify: `pkg/contextengine/builder.go`
- Modify: `pkg/contextengine/builder_test.go`
- Modify: `pkg/contextengine/token.go`
- Modify: `pkg/contextengine/engine.go`
- Modify: `pkg/contextengine/engine_test.go`

**Interfaces:**
- Consumes: ordered blocks and `RoleTool`.
- Produces: block-first `ConversationBuilder` methods.
- Produces: recursive block token counting and text-block-only truncation.
- Produces: role normalization and adjacent-role merging that preserve block boundaries and order.

- [ ] **Step 1: Write failing builder tests for interleaved blocks**

Use the new builder surface:

```go
func TestConversationBuilderKeepsAssistantBlockOrder(t *testing.T) {
	messages, err := contextengine.NewConversationBuilder().
		User("weather").
		AssistantBlocks(
			schema.NewReasoningBlock("plan", "sig"),
			schema.NewTextBlock("checking"),
			schema.NewFunctionToolCallBlock("call-1", "weather", `{}`),
		).
		ToolResult("call-1", "weather", false, schema.NewTextBlock("sunny")).
		Build()
	if err != nil { t.Fatal(err) }
	if len(messages[1].ContentBlocks) != 3 { t.Fatalf("blocks=%#v", messages[1].ContentBlocks) }
}
```

- [ ] **Step 2: Run builder tests and verify failure**

Run: `go test -count=1 ./pkg/contextengine -run TestConversationBuilderKeepsAssistantBlockOrder`

Expected: FAIL because block-first builder methods do not exist.

- [ ] **Step 3: Replace builder methods**

Keep `Message`, `System`, `User`, `Assistant`, `Metadata`, and `Build`, but make text helpers create text blocks. Replace content-specific methods with:

```go
func (b *ConversationBuilder) UserBlocks(blocks ...schema.ContentBlock) *ConversationBuilder
func (b *ConversationBuilder) AssistantBlocks(blocks ...schema.ContentBlock) *ConversationBuilder
func (b *ConversationBuilder) AssistantCall(id, name, arguments string) *ConversationBuilder
func (b *ConversationBuilder) ToolResult(id, name string, isError bool, blocks ...schema.ContentBlock) *ConversationBuilder
func (b *ConversationBuilder) Reasoning(text, signature string) *ConversationBuilder
```

`Reasoning` appends a reasoning block to the preceding assistant message; it never writes a parallel field.

- [ ] **Step 4: Write failing token and truncation tests**

Cover top-level text, reasoning, media fallback, tool arguments, nested result content and atomic opaque blocks. Add a truncation test where an interleaved image stays in its original position while only text changes:

```go
func TestContextBuilderTruncatesTextWithoutFlatteningBlocks(t *testing.T) {
	assembler, err := contextengine.NewContextBuilder(contextengine.Config{
		MaxContextTokens: 40, ReservedOutputTokens: 5,
	})
	if err != nil { t.Fatal(err) }
	message := schema.NewMessage(schema.RoleUser,
		schema.NewTextBlock(strings.Repeat("prefix ", 50)),
		schema.NewImageBlock("file:///diagram.png", "image/png"),
		schema.NewTextBlock("LATEST"),
	)
	result, err := assembler.Invoke(context.Background(), contextengine.Request{Input: []schema.Message{message}})
	if err != nil { t.Fatal(err) }
	blocks := result.Messages[0].ContentBlocks
	if len(blocks) != 3 || blocks[1].Type != schema.ContentBlockTypeImage || blocks[2].Text.Text != "LATEST" {
		t.Fatalf("blocks=%#v", blocks)
	}
}
```

- [ ] **Step 5: Implement recursive counting and block truncation**

Replace `countMessage` content access with a block switch. Count role plus:

```text
text               -> Text
reasoning          -> Text + Signature
media              -> URI + MIMEType + raw Data
function call      -> ID + Name + Arguments
function result    -> CallID + Name + recursively counted ContentBlocks
opaque             -> Kind + Raw
```

Replace `truncateContent` with a helper that clones `[]ContentBlock`, allocates the text budget across eligible top-level text blocks, preserves non-text blocks atomically, and retains block order.

- [ ] **Step 6: Migrate normalization and merging**

Use `RoleTool` and typed call/result IDs when validating call-result alignment. When `MergeAdjacentRoles` is enabled, append cloned block slices; do not insert a synthetic separator block and do not concatenate two text payloads. Remove reasoning by deleting reasoning blocks when trimming requires it.

Keep prompt-cache metadata behavior unchanged in this task; cache policy is outside the message migration.

- [ ] **Step 7: Run the Context Engine package gate**

Run: `go test -count=1 ./pkg/contextengine`

Expected: PASS.

- [ ] **Step 8: Commit the context migration**

```bash
git add pkg/contextengine
git commit -m "refactor(context): assemble ordered blocks"
```

---

### Task 5: Migrate Agent Recipes and Message Consumers

**Files:**
- Modify: `pkg/agent/base.go`
- Modify: `pkg/agent/basic.go`
- Modify: `pkg/agent/callback.go`
- Modify: `pkg/agent/config_options.go`
- Modify: `pkg/agent/hub.go`
- Modify: `pkg/agent/interrupt.go`
- Modify: `pkg/agent/new.go`
- Modify: `pkg/agent/node.go`
- Modify: `pkg/agent/options.go`
- Modify: `pkg/agent/react_agent.go`
- Modify: `pkg/agent/recipe_adapter.go`
- Modify: `pkg/agent/routing.go`
- Modify: `pkg/agent/runnable.go`
- Modify: `pkg/agent/state.go`
- Modify: `internal/agentrecipe/recipe.go`
- Modify: `internal/agentrecipe/routing.go`
- Modify: `internal/agentrecipe/group.go`
- Test: `pkg/agent/*_test.go`

**Interfaces:**
- Consumes: block-native LLM, ToolsNode and Context Engine APIs.
- Produces: Agent flows with no old message-field reads.
- Produces: explicit text extraction at each text-protocol boundary.
- Produces: agent attribution stored in `Message.Extra["agent_name"]`, separate from tool names.

- [ ] **Step 1: Add failing Agent integration coverage for one-message parallel calls**

Add a scripted model turn containing two function-call blocks, followed by a final text block. Assert the Agent performs two tools and appends typed results:

```go
func TestAgentExecutesParallelCallBlocksFromOneAssistantMessage(t *testing.T) {
	firstTool := tool.NewFunctionTool(tool.Spec{Name: "first"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("one"), nil
	})
	secondTool := tool.NewFunctionTool(tool.Spec{Name: "second"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("two"), nil
	})
	callMessage := schema.NewMessage(schema.RoleAssistant,
		schema.NewTextBlock("running"),
		schema.NewFunctionToolCallBlock("call-1", "first", `{}`),
		schema.NewFunctionToolCallBlock("call-2", "second", `{}`),
	)
	model := llm.NewScriptedModel([]schema.Message{callMessage}, []schema.Message{
		schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("done")),
	})
	value, err := agent.New(agent.BaseConfig{Model: model, Tools: []tool.Tool{firstTool, secondTool}})
	if err != nil { t.Fatal(err) }
	output, err := value.Invoke(context.Background(), []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewTextBlock("run")),
	})
	if err != nil { t.Fatal(err) }
	if len(output) != 4 { t.Fatalf("output=%d", len(output)) }
	if len(output[1].FunctionToolResults()) != 1 || len(output[2].FunctionToolResults()) != 1 { t.Fatalf("output=%#v", output) }
	if output[3].JoinedText("") != "done" { t.Fatalf("final=%q", output[3].JoinedText("")) }
}
```

- [ ] **Step 2: Run the focused Agent test and verify failure**

Run: `go test -count=1 ./pkg/agent -run TestAgentExecutesParallelCallBlocksFromOneAssistantMessage`

Expected: FAIL because Agent call detection still reads top-level `FunctionCall`.

- [ ] **Step 3: Migrate system prompt and call detection**

`withSystemMessage` prepends a new system text message or prepends a text block to the existing first system message without flattening its other blocks. Agent recipes detect pending calls with `message.FunctionToolCalls()` and consume typed results from `FunctionToolResults()`.

Replace all role checks from `RoleFunction` to `RoleTool`.

- [ ] **Step 4: Migrate explicit text-protocol boundaries**

Use `JoinedText("")` only where the consumer intentionally expects one text protocol:

```text
ReAct action parsing
ReAct observation/final-answer parsing
routing-agent textual fallbacks
language detection
mock echo models
human-readable tool errors
```

Use `TextBlocks()` or direct block traversal everywhere else. Do not include reasoning, media URIs or nested tool results in those strings.

- [ ] **Step 5: Migrate agent attribution**

Replace uses of `Message.Name` for agent ownership with:

```go
const agentNameMetadataKey = "agent_name"

func setMessageAgentName(message schema.Message, name string) schema.Message
func messageAgentName(message schema.Message) string
```

Keep these helpers private to `pkg/agent`; clone `Extra` before writing. Tool names remain inside function call/result payloads.

- [ ] **Step 6: Migrate all Agent tests**

Replace message literals and assertions with block constructors and accessors. Preserve each test's behavioral assertion: checkpoint contents, callback lifecycle, resume, routing, concurrency, tool errors and final output. Do not replace assertions with broad length-only checks.

- [ ] **Step 7: Run Agent and internal recipe gates**

Run: `go test -count=1 ./pkg/agent ./internal/agentrecipe`

Expected: PASS.

- [ ] **Step 8: Commit the Agent migration**

```bash
git add pkg/agent internal/agentrecipe
git commit -m "refactor(agent): consume typed message blocks"
```

---

### Task 6: Migrate Persistence, Extensions and Graph Callers

**Files:**
- Modify: `extensions/cache/llm/cache.go`
- Modify: `extensions/cache/llm/cache_test.go`
- Modify: `extensions/mcp/mcp.go`
- Modify: `extensions/mcp/mcp_test.go`
- Modify: `extensions/state/redis/redis_test.go`
- Modify: `pkg/state/memory_test.go`
- Modify: `pkg/graph/architecture_test.go`
- Modify: `pkg/graph/callback.go`
- Modify: `pkg/graph/graph_test.go`
- Modify: `pkg/graph/parallel.go`
- Modify: `pkg/graph/runtime_execute.go`
- Modify: `pkg/graph/runtime_graph.go`
- Modify: `pkg/graph/runtime_state.go`
- Modify: `pkg/graph/stream_execute.go`
- Modify: `pkg/callback/dispatcher.go`

**Interfaces:**
- Consumes: canonical message JSON and strict block cloning.
- Produces: checkpoint/cache round-trips that preserve nested blocks, opaque raw JSON and response metadata.
- Produces: MCP results converted directly to tool-result content blocks.

- [ ] **Step 1: Add failing persistence round-trip tests**

Store and load a message containing nested tool-result blocks, opaque data and response metadata in memory and Redis-backed stores:

```go
message := schema.NewMessage(schema.RoleTool,
	schema.NewFunctionToolResultBlock("call-1", "lookup", false,
		schema.NewTextBlock("value"),
		schema.NewOpaqueBlock("provider.citation", json.RawMessage(`{"page":3}`)),
	),
)
message.ResponseMeta = &schema.ResponseMeta{ID: "resp-1", Usage: &schema.TokenUsage{CacheReadTokens: 32}}
```

Assert the loaded result preserves call ID, opaque bytes and `CacheReadTokens`.

- [ ] **Step 2: Run persistence tests and verify failure**

Run: `go test -count=1 ./pkg/state ./extensions/state/redis ./extensions/cache/llm`

Expected: FAIL until fixtures and cache cloning use the new fields.

- [ ] **Step 3: Migrate LLM cache cloning and keys**

Use `Request.Clone` and `Response.Clone` with block-aware schema cloning. Keep canonical JSON hashing deterministic by hashing the marshaled new request representation. Add a regression test proving that changing block order changes the cache key and cloning a response does not share nested metadata.

- [ ] **Step 4: Migrate MCP result conversion**

Map MCP text and structured content to schema blocks and return `tool.BlockResult`. Preserve MCP `isError` through `FunctionToolResult.IsError`; do not write `tool_error` or call IDs to `Message.Extra`.

- [ ] **Step 5: Migrate graph, callback and state fixtures**

Replace remaining message literals, `Text()`, `Name`, `FunctionID`, `Content` and `RoleFunction` references in these packages. Graph code must continue treating messages as ordinary values and use `schema.CloneMessages` at ownership boundaries.

- [ ] **Step 6: Run extension and infrastructure gates**

Run: `go test -count=1 ./extensions/... ./pkg/state ./pkg/graph ./pkg/callback`

Expected: PASS.

- [ ] **Step 7: Commit persistence and extension migration**

```bash
git add extensions pkg/state pkg/graph pkg/callback
git commit -m "refactor(runtime): persist ordered message blocks"
```

---

### Task 7: Remove the Legacy Message Model

**Files:**
- Delete: `pkg/schema/content.go`
- Modify: `pkg/schema/message.go`
- Modify: `pkg/schema/message_test.go`
- Modify: all remaining Go files reported by the legacy-symbol scan

**Interfaces:**
- Consumes: every migrated caller from Tasks 1-6.
- Produces: final `Message` with only `Role`, `ContentBlocks`, `ResponseMeta`, and `Extra`.
- Removes: `Content`, `ContentItem`, `ContentType`, `TextContent`, `MultiContent`, `ReasoningContent`, `FunctionCall`, `Name`, `FunctionID`, `RoleFunction`, `Message.Text`, and legacy message JSON fallback.

- [ ] **Step 1: Run the legacy-symbol scan**

Run:

```powershell
rg -n --glob '*.go' 'schema\.Content\b|\bContentItem\b|\bContentType\b|TextContent\(|MultiContent\(|ReasoningContent|\bFunctionCall\b|RoleFunction|\.FunctionID\(|func \(m Message\) Text\(\)' pkg extensions internal
```

Expected: matches remain in the transitional schema and any callers missed by Tasks 1-6.

- [ ] **Step 2: Add a final architecture test**

Add a source-level test in `pkg/schema/message_test.go` that reads `message.go` and rejects removed field declarations and methods:

```go
func TestMessageHasOnlyBlockContentModel(t *testing.T) {
	data, err := os.ReadFile("message.go")
	if err != nil { t.Fatal(err) }
	source := string(data)
	for _, forbidden := range []string{
		"ReasoningContent", "FunctionCall *", "func (m Message) Text()", "func (m Message) FunctionID(",
	} {
		if strings.Contains(source, forbidden) { t.Fatalf("legacy message API remains: %s", forbidden) }
	}
}
```

- [ ] **Step 3: Delete legacy fields, JSON handling and content types**

Reduce the public message shape to:

```go
type Message struct {
	Role          Role           `json:"role"`
	ContentBlocks []ContentBlock `json:"content_blocks,omitempty"`
	ResponseMeta  *ResponseMeta  `json:"response_meta,omitempty"`
	Extra         map[string]any `json:"extra,omitempty"`
}
```

Delete `content.go`, remove the temporary `RoleFunction` alias, and remove all cloning/validation/JSON branches for legacy fields.

- [ ] **Step 4: Migrate every remaining compiler or scan match**

For each remaining match, choose one explicit replacement:

```text
plain message text      -> TextBlocks or JoinedText
multimodal content      -> ordered media/text blocks
reasoning               -> ReasoningBlocks
function request/result -> typed function block accessors
agent ownership         -> agent_name Extra helper
tool ownership          -> FunctionToolCall.Name or FunctionToolResult.Name
```

Do not add compatibility aliases to make the scan pass.

- [ ] **Step 5: Format and rerun the legacy-symbol scan**

Run: `gofmt -w (rg --files pkg extensions internal -g '*.go')`

Run the Step 1 `rg` command again.

Expected: no matches except intentional references inside the architecture test's forbidden-string table. Narrow the scan with `-g '!message_test.go'` to prove production code is clean.

- [ ] **Step 6: Run the complete code gate**

Run: `go test -count=1 ./...`

Expected: PASS.

- [ ] **Step 7: Commit the legacy removal**

```bash
git add pkg extensions internal
git commit -m "refactor(schema)!: remove fixed message fields" -m "BREAKING CHANGE: messages now use ordered ContentBlocks and RoleTool. Legacy Content, Text, FunctionCall and FunctionID APIs are removed."
```

---

### Task 8: Update Documentation and Run Final Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/使用手册.md`
- Modify: `docs/context-management.md`
- Modify: `docs/llm-providers.md`
- Modify: `docs/execution-model.md`
- Modify: `docs/extensions.md`
- Modify: `docs/mcp-tools.md`
- Modify: `docs/tool-runtime.md`
- Modify: `docs/tool-http.md`
- Modify: other current documentation returned by the legacy-symbol scan

**Interfaces:**
- Consumes: final public API from Task 7.
- Produces: examples that compile conceptually against the block-only message model.
- Produces: final repository verification evidence.

- [ ] **Step 1: Find every legacy documentation reference**

Run:

```powershell
rg -n 'TextContent|MultiContent|ContentItem|ReasoningContent|FunctionCall|FunctionMessage|RoleFunction|\.Text\(\)|FunctionID|Message\{' README.md docs -g '*.md'
```

Expected: list all examples and prose that still describe the old model.

- [ ] **Step 2: Rewrite the public message examples**

Use block-first examples consistently:

```go
input := []schema.Message{
	schema.NewMessage(schema.RoleUser,
		schema.NewTextBlock("Summarize this image"),
		schema.NewImageBlock("file:///tmp/image.png", "image/png"),
	),
}
```

Document `ResponseMeta`, top-level-only filtered accessors, direct traversal for full order, `RoleTool`, opaque blocks and the absence of implicit `Text()` flattening. Do not add prompt-cache policy to the Message documentation.

- [ ] **Step 3: Verify documentation contains no legacy API**

Run the Step 1 `rg` command again.

Expected: no current README/manual/provider/runtime documentation matches. Historical specs and plans may retain old names when clearly describing removed APIs.

- [ ] **Step 4: Run formatting and static checks**

Run: `gofmt -w (rg --files pkg extensions internal -g '*.go')`

Run: `go vet ./...`

Expected: both commands complete successfully.

- [ ] **Step 5: Run the final test suite**

Run: `go test -count=1 ./...`

Expected: PASS with no package failures.

- [ ] **Step 6: Review the final diff for accidental compatibility code**

Run:

```powershell
git diff --check
git diff --stat
rg -n --glob '*.go' 'schema\.Content\b|\bContentItem\b|\bContentType\b|TextContent\(|MultiContent\(|ReasoningContent|\bFunctionCall\b|RoleFunction|\.FunctionID\(|func \(m Message\) Text\(\)' pkg extensions internal
```

Expected: no whitespace errors and no production legacy symbols.

- [ ] **Step 7: Commit documentation and verification updates**

```bash
git add README.md docs
git commit -m "docs: document ordered message blocks"
```

## Completion Criteria

- `schema.Message` has only `Role`, `ContentBlocks`, `ResponseMeta`, and `Extra`.
- Every built-in and opaque block survives strict clone and JSON round-trip tests.
- Parallel function calls remain in one assistant message and execute in order.
- Context assembly never flattens or reorders blocks implicitly.
- Checkpoints and caches preserve nested blocks and response metadata.
- Unsupported provider blocks fail explicitly instead of disappearing.
- Current code and documentation contain no legacy message API usage.
- `go vet ./...` and `go test -count=1 ./...` pass.
