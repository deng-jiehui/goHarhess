package loader_test

import (
	"context"
	"strings"
	"testing"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/extensions/retrieval/loader"
)

func TestTextLoader(t *testing.T) {
	docs, err := loader.Text().Invoke(context.Background(), retrieval.Source{
		Name: "readme.txt",
		Body: strings.NewReader("hello world"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0].ID != "readme.txt" || docs[0].Text != "hello world" {
		t.Fatalf("docs = %+v", docs)
	}
	if docs[0].Metadata["source_name"] != "readme.txt" {
		t.Fatalf("metadata = %v", docs[0].Metadata)
	}
}

func TestTextLoaderEmptyBody(t *testing.T) {
	if _, err := loader.Text().Invoke(context.Background(), retrieval.Source{}); err == nil {
		t.Fatal("nil body should fail")
	}
}

func TestMarkdownLoaderSections(t *testing.T) {
	md := `# Guide

Intro text.

## Install

Step one.

## Usage

Run it.

### Nested

Deep section.
`
	docs, err := loader.Markdown().Invoke(context.Background(), retrieval.Source{
		Name: "guide.md",
		Body: strings.NewReader(md),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 4 {
		t.Fatalf("docs = %d, want 4 sections", len(docs))
	}
	// 第三个文档（## Usage）标题路径应反映层级。
	usage := docs[2]
	path, _ := usage.Metadata["heading_path"].([]string)
	if len(path) != 2 || path[0] != "Guide" || path[1] != "Usage" {
		t.Fatalf("heading_path = %v", usage.Metadata["heading_path"])
	}
	if !strings.Contains(usage.Text, "Run it.") {
		t.Fatalf("usage body = %q", usage.Text)
	}
	// 嵌套 h3 的路径是 [Guide Usage Nested]。
	nested := docs[3]
	npath, _ := nested.Metadata["heading_path"].([]string)
	if len(npath) != 3 || npath[2] != "Nested" {
		t.Fatalf("nested path = %v", npath)
	}
}

func TestMarkdownLoaderNoHeadings(t *testing.T) {
	docs, err := loader.Markdown().Invoke(context.Background(), retrieval.Source{
		Body: strings.NewReader("plain text\n\nmore text"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("docs = %d", len(docs))
	}
}

func TestHTMLLoader(t *testing.T) {
	page := `<html><head><title>T</title><style>body{color:red}</style></head>
<body><h1>Title</h1><p>First paragraph.</p><script>alert(1)</script><p>Second.</p></body></html>`
	docs, err := loader.HTML().Invoke(context.Background(), retrieval.Source{
		Name: "page.html",
		Body: strings.NewReader(page),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("docs = %d", len(docs))
	}
	text := docs[0].Text
	if strings.Contains(text, "alert") || strings.Contains(text, "color") {
		t.Fatalf("script/style leaked into text: %q", text)
	}
	if !strings.Contains(text, "First paragraph.") || !strings.Contains(text, "Second.") {
		t.Fatalf("body text missing: %q", text)
	}
}
