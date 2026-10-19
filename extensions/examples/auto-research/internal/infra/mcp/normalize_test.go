package mcp

import (
	"testing"
)

func TestParseCandidatesNormalizesArxivFixture(t *testing.T) {
	raw := []byte(`{"papers":[{"id":"2401.00001v2","title":"Fixture Research","published":"2024-01-02","summary":"摘要","authors":[{"name":"A. Author"}],"url":"https://arxiv.org/abs/2401.00001"}]}`)
	items, err := ParseCandidates("arxiv", raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].SourceKey != "arxiv:2401.00001" || items[0].Year != 2024 || items[0].Authors[0] != "A. Author" {
		t.Fatalf("归一化结果=%+v", items)
	}
}

func TestParseCandidatesRejectsMissingPaperList(t *testing.T) {
	if _, err := ParseCandidates("arxiv", []byte(`{"ok":true}`)); err == nil {
		t.Fatal("缺少论文列表时未返回错误")
	}
}
