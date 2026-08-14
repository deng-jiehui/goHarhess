package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

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

	Text               *Text               `json:"text,omitempty"`
	Image              *MediaContent       `json:"image,omitempty"`
	File               *MediaContent       `json:"file,omitempty"`
	Audio              *MediaContent       `json:"audio,omitempty"`
	Video              *MediaContent       `json:"video,omitempty"`
	Reasoning          *Reasoning          `json:"reasoning,omitempty"`
	FunctionToolCall   *FunctionToolCall   `json:"function_tool_call,omitempty"`
	FunctionToolResult *FunctionToolResult `json:"function_tool_result,omitempty"`
	Opaque             *OpaqueContent      `json:"opaque,omitempty"`
}

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

func NewTextBlock(text string) ContentBlock {
	return ContentBlock{Type: ContentBlockTypeText, Text: &Text{Text: text}}
}

func NewImageBlock(uri, mimeType string) ContentBlock {
	return ContentBlock{Type: ContentBlockTypeImage, Image: &MediaContent{URI: uri, MIMEType: mimeType}}
}

func NewFileBlock(uri, mimeType string) ContentBlock {
	return ContentBlock{Type: ContentBlockTypeFile, File: &MediaContent{URI: uri, MIMEType: mimeType}}
}

func NewAudioBlock(uri, mimeType string) ContentBlock {
	return ContentBlock{Type: ContentBlockTypeAudio, Audio: &MediaContent{URI: uri, MIMEType: mimeType}}
}

func NewVideoBlock(uri, mimeType string) ContentBlock {
	return ContentBlock{Type: ContentBlockTypeVideo, Video: &MediaContent{URI: uri, MIMEType: mimeType}}
}

func NewReasoningBlock(text, signature string) ContentBlock {
	return ContentBlock{Type: ContentBlockTypeReasoning, Reasoning: &Reasoning{Text: text, Signature: signature}}
}

func NewFunctionToolCallBlock(id, name, arguments string) ContentBlock {
	return ContentBlock{
		Type:             ContentBlockTypeFunctionToolCall,
		FunctionToolCall: &FunctionToolCall{ID: id, Name: name, Arguments: arguments},
	}
}

func NewFunctionToolResultBlock(callID, name string, isError bool, blocks ...ContentBlock) ContentBlock {
	return ContentBlock{
		Type: ContentBlockTypeFunctionToolResult,
		FunctionToolResult: &FunctionToolResult{
			CallID: callID, Name: name, IsError: isError,
			ContentBlocks: append([]ContentBlock(nil), blocks...),
		},
	}
}

func NewOpaqueBlock(kind string, raw json.RawMessage) ContentBlock {
	return ContentBlock{
		Type:   ContentBlockTypeOpaque,
		Opaque: &OpaqueContent{Kind: kind, Raw: append(json.RawMessage(nil), raw...)},
	}
}

func (b ContentBlock) Validate() error {
	payloads := 0
	for _, present := range []bool{
		b.Text != nil, b.Image != nil, b.File != nil, b.Audio != nil, b.Video != nil,
		b.Reasoning != nil, b.FunctionToolCall != nil, b.FunctionToolResult != nil, b.Opaque != nil,
	} {
		if present {
			payloads++
		}
	}
	if payloads != 1 {
		return fmt.Errorf("content block %q must contain exactly one payload, got %d", b.Type, payloads)
	}

	switch b.Type {
	case ContentBlockTypeText:
		if b.Text == nil {
			return errors.New("content block type text requires text payload")
		}
	case ContentBlockTypeImage:
		if b.Image == nil {
			return errors.New("content block type image requires image payload")
		}
		return validateMedia(b.Image)
	case ContentBlockTypeFile:
		if b.File == nil {
			return errors.New("content block type file requires file payload")
		}
		return validateMedia(b.File)
	case ContentBlockTypeAudio:
		if b.Audio == nil {
			return errors.New("content block type audio requires audio payload")
		}
		return validateMedia(b.Audio)
	case ContentBlockTypeVideo:
		if b.Video == nil {
			return errors.New("content block type video requires video payload")
		}
		return validateMedia(b.Video)
	case ContentBlockTypeReasoning:
		if b.Reasoning == nil {
			return errors.New("content block type reasoning requires reasoning payload")
		}
	case ContentBlockTypeFunctionToolCall:
		if b.FunctionToolCall == nil {
			return errors.New("content block type function_tool_call requires function_tool_call payload")
		}
		if strings.TrimSpace(b.FunctionToolCall.ID) == "" {
			return errors.New("function tool call requires an id")
		}
		if strings.TrimSpace(b.FunctionToolCall.Name) == "" {
			return errors.New("function tool call requires a name")
		}
		var arguments map[string]json.RawMessage
		if err := json.Unmarshal([]byte(b.FunctionToolCall.Arguments), &arguments); err != nil {
			return fmt.Errorf("function tool call arguments must be a JSON object: %w", err)
		}
		if arguments == nil {
			return errors.New("function tool call arguments must be a JSON object")
		}
	case ContentBlockTypeFunctionToolResult:
		if b.FunctionToolResult == nil {
			return errors.New("content block type function_tool_result requires function_tool_result payload")
		}
		if strings.TrimSpace(b.FunctionToolResult.CallID) == "" {
			return errors.New("function tool result requires a call id")
		}
		if strings.TrimSpace(b.FunctionToolResult.Name) == "" {
			return errors.New("function tool result requires a name")
		}
		for i, nested := range b.FunctionToolResult.ContentBlocks {
			if err := nested.Validate(); err != nil {
				return fmt.Errorf("content_blocks[%d]: %w", i, err)
			}
		}
	case ContentBlockTypeOpaque:
		if b.Opaque == nil {
			return errors.New("content block type opaque requires opaque payload")
		}
		if strings.TrimSpace(b.Opaque.Kind) == "" {
			return errors.New("opaque content requires a kind")
		}
		if len(bytes.TrimSpace(b.Opaque.Raw)) == 0 || !json.Valid(b.Opaque.Raw) {
			return errors.New("opaque content raw must be valid JSON")
		}
	default:
		return fmt.Errorf("unsupported content block type %q", b.Type)
	}
	return nil
}

func validateMedia(media *MediaContent) error {
	sources := 0
	if strings.TrimSpace(media.URI) != "" {
		sources++
	}
	if len(media.Data) != 0 {
		sources++
	}
	if sources != 1 {
		return fmt.Errorf("media content must contain exactly one source, got %d", sources)
	}
	if len(media.Data) != 0 && !json.Valid(media.Data) {
		return errors.New("media data must be valid JSON")
	}
	return nil
}

func cloneContentBlocks(blocks []ContentBlock, strict bool, path string) ([]ContentBlock, error) {
	if blocks == nil {
		return nil, nil
	}
	cloned := make([]ContentBlock, len(blocks))
	for i, block := range blocks {
		item, err := cloneContentBlock(block, strict, fmt.Sprintf("%s[%d]", path, i))
		if err != nil {
			return nil, err
		}
		cloned[i] = item
	}
	return cloned, nil
}

func cloneContentBlock(block ContentBlock, strict bool, path string) (ContentBlock, error) {
	cloned := ContentBlock{Type: block.Type}
	var err error
	if block.Text != nil {
		payload := *block.Text
		payload.Extra, err = cloneExtra(block.Text.Extra, strict, path+".text.extra")
		if err != nil {
			return ContentBlock{}, err
		}
		cloned.Text = &payload
	}
	if block.Image != nil {
		cloned.Image, err = cloneMediaContent(block.Image, strict, path+".image")
		if err != nil {
			return ContentBlock{}, err
		}
	}
	if block.File != nil {
		cloned.File, err = cloneMediaContent(block.File, strict, path+".file")
		if err != nil {
			return ContentBlock{}, err
		}
	}
	if block.Audio != nil {
		cloned.Audio, err = cloneMediaContent(block.Audio, strict, path+".audio")
		if err != nil {
			return ContentBlock{}, err
		}
	}
	if block.Video != nil {
		cloned.Video, err = cloneMediaContent(block.Video, strict, path+".video")
		if err != nil {
			return ContentBlock{}, err
		}
	}
	if block.Reasoning != nil {
		payload := *block.Reasoning
		payload.Extra, err = cloneExtra(block.Reasoning.Extra, strict, path+".reasoning.extra")
		if err != nil {
			return ContentBlock{}, err
		}
		cloned.Reasoning = &payload
	}
	if block.FunctionToolCall != nil {
		payload := *block.FunctionToolCall
		payload.Extra, err = cloneExtra(block.FunctionToolCall.Extra, strict, path+".function_tool_call.extra")
		if err != nil {
			return ContentBlock{}, err
		}
		cloned.FunctionToolCall = &payload
	}
	if block.FunctionToolResult != nil {
		payload := *block.FunctionToolResult
		payload.ContentBlocks, err = cloneContentBlocks(
			block.FunctionToolResult.ContentBlocks, strict, path+".function_tool_result.content_blocks",
		)
		if err != nil {
			return ContentBlock{}, err
		}
		payload.Extra, err = cloneExtra(block.FunctionToolResult.Extra, strict, path+".function_tool_result.extra")
		if err != nil {
			return ContentBlock{}, err
		}
		cloned.FunctionToolResult = &payload
	}
	if block.Opaque != nil {
		payload := *block.Opaque
		payload.Raw = append(json.RawMessage(nil), block.Opaque.Raw...)
		cloned.Opaque = &payload
	}
	return cloned, nil
}

func cloneMediaContent(media *MediaContent, strict bool, path string) (*MediaContent, error) {
	cloned := *media
	cloned.Data = append(json.RawMessage(nil), media.Data...)
	extra, err := cloneExtra(media.Extra, strict, path+".extra")
	if err != nil {
		return nil, err
	}
	cloned.Extra = extra
	return &cloned, nil
}

func cloneExtra(extra map[string]any, strict bool, path string) (map[string]any, error) {
	if extra == nil {
		return nil, nil
	}
	cloned := make(map[string]any, len(extra))
	for key, value := range extra {
		item, err := cloneAnyValue(value, strict, fmt.Sprintf("%s[%q]", path, key), 0)
		if err != nil {
			return nil, err
		}
		cloned[key] = item
	}
	return cloned, nil
}
