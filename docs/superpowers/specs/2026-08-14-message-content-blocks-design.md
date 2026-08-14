# Message Content Blocks Design

## Goal

Replace the current fixed-field `schema.Message` representation with one
ordered content-block representation that can carry complete model history
without flattening text, reasoning, multimodal content, tool calls or tool
results.

The framework provides the storage, cloning, validation and traversal
primitives. Applications and provider adapters decide what to preserve, how to
encode it, whether a history can be replayed, and how prompt caching is used.

This is a breaking migration. The repository has not shipped, so the design
does not retain the old message fields, JSON shape or accessor behavior.

## Non-Goals

- Define prompt-cache keys, breakpoints, TTLs or prefix-matching policy.
- Guarantee replay across or within any particular provider.
- Add transcript, branch, lineage or prompt-fingerprint abstractions.
- Add Claude, Gemini or another provider adapter.
- Add a new context summarization or truncation strategy.
- Preserve the old `Content`, `ContentItem` or `Message.Text()` APIs.

## Public Model

### Message

`ContentBlocks` is the only source of message content:

```go
type Message struct {
	Role          Role           `json:"role"`
	ContentBlocks []ContentBlock `json:"content_blocks,omitempty"`
	ResponseMeta  *ResponseMeta  `json:"response_meta,omitempty"`
	Extra         map[string]any `json:"extra,omitempty"`
}
```

The block slice uses values rather than pointers, so a message cannot contain
a nil block. Slice order is significant and must survive cloning, JSON
round-trips, checkpoints and provider conversions.

`Message` no longer contains `Content`, `ReasoningContent`, `Name` or
`FunctionCall`. The standalone `Content` and `ContentItem` abstractions are
removed after their callers migrate to blocks.

The provider-neutral role set is `system`, `user`, `assistant` and `tool`.
`RoleTool` replaces the function-specific `RoleFunction`; provider adapters
remain free to encode a tool-result block under a different wire role when
their protocol requires it.

### Content Block

`ContentBlock` is a tagged union. A valid built-in block has exactly one
payload, and that payload matches `Type`.

```go
type ContentBlock struct {
	Type ContentBlockType `json:"type"`

	Text               *TextContent       `json:"text,omitempty"`
	Image              *MediaContent      `json:"image,omitempty"`
	File               *MediaContent      `json:"file,omitempty"`
	Audio              *MediaContent      `json:"audio,omitempty"`
	Video              *MediaContent      `json:"video,omitempty"`
	Reasoning          *Reasoning          `json:"reasoning,omitempty"`
	FunctionToolCall   *FunctionToolCall   `json:"function_tool_call,omitempty"`
	FunctionToolResult *FunctionToolResult `json:"function_tool_result,omitempty"`
	Opaque             *OpaqueContent      `json:"opaque,omitempty"`
}
```

The built-in block types are:

```text
text
image
file
audio
video
reasoning
function_tool_call
function_tool_result
opaque
```

The payloads use provider-neutral fields and retain an escape hatch for data
the framework does not interpret:

```go
type TextContent struct {
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
```

Function arguments remain strings. This preserves the received representation,
avoids JSON number coercion and supports partial argument fragments while a
stream is being reduced.

`OpaqueContent` carries a complete custom payload when a provider adapter or
application needs a block that the framework does not understand. Core code
does not assign semantics to `Kind` or `Raw`.

### Response Metadata

Response metadata belongs on the generated message so it can survive storage
with that message:

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

`llm.Response` remains the runtime envelope used for streaming, callbacks and
one complete model invocation. `llm.Usage` becomes an alias of
`schema.TokenUsage` so runtime and persisted metadata do not diverge. When an
invocation completes, response-level ID, model, finish reason and usage are
copied into its generated message's `ResponseMeta`. A usage-only stream event
may remain only on `llm.Response` until a message is complete.

## Construction and Access

The new API is block-first:

```go
schema.NewMessage(role, blocks...)
schema.NewTextBlock(text)
schema.NewImageBlock(uri, mimeType)
schema.NewReasoningBlock(text, signature)
schema.NewFunctionToolCallBlock(id, name, arguments)
schema.NewFunctionToolResultBlock(id, name, blocks...)
schema.NewOpaqueBlock(kind, raw)
```

Payload accessors return defensive payload copies, not wrapper blocks:

```go
func (m Message) TextBlocks() []TextContent
func (m Message) JoinedText(separator string) string
func (m Message) ReasoningBlocks() []Reasoning
func (m Message) FunctionToolCalls() []FunctionToolCall
func (m Message) FunctionToolResults() []FunctionToolResult
```

The specific method names deliberately retain `Function`. A future general
tool-call abstraction can use `ToolCalls` without colliding with this API.

These filtered accessors discard the positions of unrelated blocks. Code that
needs the exact relationship between text, reasoning and tool calls iterates
`Message.ContentBlocks` directly.

Filtered accessors inspect only top-level blocks. They do not recursively pull
text, reasoning or calls out of a function result. Callers traverse
`FunctionToolResult.ContentBlocks` when they need nested result content.

There is no `Message.Text()` compatibility method. Callers must choose whether
they need individual text blocks or an explicit joined representation. Media
URIs, reasoning and nested tool-result text are never silently folded into a
text string.

## Validation and Cloning

`Message.Validate` validates final, persistable messages. It checks:

- the role is supported;
- every block has a supported type;
- every built-in block has exactly one payload matching its type;
- media blocks have exactly one source, either `URI` or `Data`;
- opaque blocks have a non-empty kind and valid JSON payload;
- final function calls and results contain their required IDs and names;
- nested function-result blocks are recursively valid;
- response metadata and token counts are structurally valid.

Validation does not enforce combinations between roles and block types. A
provider adapter or application may impose stricter rules without changing the
storage model.

Known block types with malformed payloads are errors. They are not silently
converted to opaque blocks. A decoder that receives a genuinely unknown
provider block may explicitly wrap the original object in `OpaqueContent`.

`CloneMessages` deep-copies blocks, nested tool results, response metadata,
`json.RawMessage` bytes and all supported values in `Extra`.
`CloneMessagesStrict` rejects unknown mutable values at their complete message
and block paths. Mutating an accessor result or clone must never mutate the
source message.

The new JSON representation is the only supported representation. There is no
legacy unmarshal fallback for the old fixed message fields.

## Runtime Data Flow

### Provider Decode and Encode

A provider decoder creates one message with blocks in provider order. Multiple
function calls in one assistant response remain multiple call blocks in the
same message. Unknown provider content is represented by opaque blocks instead
of being flattened to text.

A provider encoder traverses blocks in order. If it cannot encode a block, it
returns a typed unsupported-block error. It must not silently drop or reorder
content. Provider-specific replay and cache behavior remain adapter policy.

### Streaming

Streaming chunks are transient and may contain incomplete block payloads. A
decoder places deltas in their provider block positions, and
`DeltaAccumulator` merges corresponding blocks by position and type. Text,
reasoning and function-argument fragments append according to the existing
delta rules.

Only the fully reduced message runs final validation and enters agent history
or a checkpoint. A truncated stream with an incomplete block fails the model
invocation and commits no partial history.

### Tool Execution

`ToolsNode` extracts every `FunctionToolCall` block from the contiguous
assistant-call suffix. Calls retain their block order and execute under the
existing concurrency limits. The node returns one function-role result message
per call, in source order, containing a `FunctionToolResult` block.

Tool-result content may itself contain ordered text or multimodal blocks. Call
IDs and error state live in the typed result payload rather than magic keys in
`Message.Extra`.

### Context Engine

The Context Engine counts and transforms blocks without flattening a complete
message to a string:

- text and reasoning are counted from their text payloads;
- media and opaque data use the configured counter's fallback representation;
- tool calls include names and argument payloads;
- tool results include nested blocks;
- configured adjacent-role merging appends block slices without merging block
  payloads;
- truncation edits eligible text blocks and treats media, opaque and tool
  blocks atomically;
- removing reasoning removes reasoning blocks rather than clearing a parallel
  message field.

This migration does not define new cache behavior. Existing cache policy must
be evaluated separately against provider adapters.

### Persistence and Caches

Conversation checkpoints and LLM caches persist the new message JSON,
including response metadata, nested blocks and opaque raw data. They do not
store incomplete streaming deltas. Cache-key implementations that hash
messages must use the new canonical message representation.

## Breaking Migration

All repository callers migrate in one change:

1. Replace `Message.Content` and multimodal `Content` construction with blocks.
2. Replace `ReasoningContent` with reasoning blocks.
3. Replace top-level `FunctionCall`, `Name` and `function_id` metadata with
   function call/result blocks.
4. Replace `Text()` reads with `TextBlocks`, `JoinedText`, or direct block
   traversal according to the caller's actual need.
5. Update OpenAI-compatible decoding so parallel calls remain in one message.
6. Update streaming accumulation, ToolsNode, Context Engine, checkpoints,
   caches, callbacks, examples and documentation to the new model.
7. Remove `Content`, `ContentItem`, legacy message JSON code and unused helper
   constructors after the final caller migrates.

No compatibility fields or adapters remain after the migration. This avoids
two writable representations of the same message.

## Error Handling

- Invalid final messages fail before model invocation, tool execution or
  checkpoint persistence, depending on where they enter the system.
- Provider encoding of an unsupported block returns an error naming the
  message index, block index and block type.
- Provider decoding of a malformed known block returns a decoding error;
  unknown blocks may be preserved as opaque content.
- Invalid nested tool-result content reports the full recursive block path.
- A stream ending before a complete final message returns the existing
  truncated-stream error and commits no history.
- Strict cloning reports the exact `Extra` path containing an unsupported
  mutable value.

## Verification

Focused schema tests prove:

1. Every built-in block survives JSON marshal/unmarshal with order intact.
2. Opaque raw JSON, response metadata and token usage survive checkpoints.
3. Clones isolate block payloads, nested result blocks, raw bytes and extras.
4. Invalid tagged-union states fail with precise block paths.

Cross-package tests prove:

1. One assistant message retains interleaved text, reasoning and multiple
   function calls.
2. Streaming deltas merge multiple sparse tool-call positions correctly.
3. ToolsNode executes calls and correlates typed results by ID in source order.
4. Context assembly counts, merges and truncates without flattening or
   reordering blocks.
5. Checkpoint and LLM-cache round-trips preserve complete messages.
6. Provider adapters return errors instead of dropping unsupported blocks.
7. No Go source, example or current documentation references the removed
   message fields, `Content`, `ContentItem` or `Message.Text()`.

The final gate is the repository's complete Go test suite after all callers
have migrated.
