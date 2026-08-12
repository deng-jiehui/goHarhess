package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type ContentType string

const (
	ContentTypeText  ContentType = "text"
	ContentTypeImage ContentType = "image"
	ContentTypeFile  ContentType = "file"
	ContentTypeAudio ContentType = "audio"
	ContentTypeVideo ContentType = "video"
)

type ContentItem struct {
	Text  *string `json:"text,omitempty"`
	Image string  `json:"image,omitempty"`
	File  string  `json:"file,omitempty"`
	Audio any     `json:"audio,omitempty"`
	Video any     `json:"video,omitempty"`
}

func TextItem(text string) ContentItem {
	return ContentItem{Text: &text}
}

func (i ContentItem) Validate() error {
	provided := 0
	if i.Text != nil {
		provided++
	}
	if i.Image != "" {
		provided++
	}
	if i.File != "" {
		provided++
	}
	if i.Audio != nil {
		provided++
	}
	if i.Video != nil {
		provided++
	}
	if provided != 1 {
		return fmt.Errorf("content item must contain exactly one modality, got %d", provided)
	}
	return nil
}

func (i ContentItem) Type() ContentType {
	switch {
	case i.Text != nil:
		return ContentTypeText
	case i.Image != "":
		return ContentTypeImage
	case i.File != "":
		return ContentTypeFile
	case i.Audio != nil:
		return ContentTypeAudio
	case i.Video != nil:
		return ContentTypeVideo
	default:
		return ""
	}
}

func (i ContentItem) ValueString() string {
	switch i.Type() {
	case ContentTypeText:
		return *i.Text
	case ContentTypeImage:
		return i.Image
	case ContentTypeFile:
		return i.File
	case ContentTypeAudio:
		return stringifyAny(i.Audio)
	case ContentTypeVideo:
		return stringifyAny(i.Video)
	default:
		return ""
	}
}

type Content struct {
	text       string
	items      []ContentItem
	multimodal bool
}

func TextContent(text string) Content {
	return Content{text: text}
}

func MultiContent(items ...ContentItem) Content {
	copied := append([]ContentItem(nil), items...)
	return Content{items: copied, multimodal: true}
}

func (c Content) IsZero() bool {
	return !c.multimodal && c.text == "" && len(c.items) == 0
}

func (c Content) IsMultimodal() bool {
	return c.multimodal
}

func (c Content) Items() []ContentItem {
	if !c.multimodal {
		return nil
	}
	return cloneContentItems(c.items)
}

func (c Content) String() string {
	if !c.multimodal {
		return c.text
	}

	parts := make([]string, 0, len(c.items))
	for _, item := range c.items {
		value := item.ValueString()
		if value != "" {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, "\n")
}

func (c Content) PrependText(prefix string) Content {
	if prefix == "" {
		return c
	}
	if !c.multimodal {
		return TextContent(prefix + c.text)
	}
	items := append([]ContentItem{TextItem(prefix)}, c.items...)
	return MultiContent(items...)
}

func (c Content) AppendText(suffix string) Content {
	if suffix == "" {
		return c
	}
	if !c.multimodal {
		return TextContent(c.text + suffix)
	}
	items := append(cloneContentItems(c.items), TextItem(suffix))
	return MultiContent(items...)
}

// Append combines an incremental content fragment with the current content.
func (c Content) Append(other Content) Content {
	if other.IsZero() {
		return c
	}
	if c.IsZero() {
		return other
	}
	if !c.multimodal && !other.multimodal {
		return TextContent(c.text + other.text)
	}

	items := make([]ContentItem, 0)
	if c.multimodal {
		items = append(items, c.Items()...)
	} else if c.text != "" {
		items = append(items, TextItem(c.text))
	}
	if other.multimodal {
		items = append(items, other.Items()...)
	} else if other.text != "" {
		items = append(items, TextItem(other.text))
	}
	return MultiContent(items...)
}

func (c Content) Validate() error {
	if !c.multimodal {
		return nil
	}
	for idx, item := range c.items {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("content item %d: %w", idx, err)
		}
	}
	return nil
}

func (c Content) MarshalJSON() ([]byte, error) {
	if c.multimodal {
		return json.Marshal(c.items)
	}
	return json.Marshal(c.text)
}

func (c *Content) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		*c = TextContent("")
		return nil
	}
	if data[0] == '[' {
		var items []ContentItem
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		*c = MultiContent(items...)
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	*c = TextContent(text)
	return nil
}

func cloneContent(c Content) Content {
	if !c.multimodal {
		return TextContent(c.text)
	}
	return MultiContent(c.items...)
}

func cloneContentItems(items []ContentItem) []ContentItem {
	copied := make([]ContentItem, len(items))
	for i, item := range items {
		copied[i] = item
		if item.Text != nil {
			text := *item.Text
			copied[i].Text = &text
		}
	}
	return copied
}

func stringifyAny(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(data)
	}
}
