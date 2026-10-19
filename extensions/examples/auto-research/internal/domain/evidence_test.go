package domain

import (
	"errors"
	"testing"
)

func TestValidateEvidenceRequiresExactRuneQuote(t *testing.T) {
	doc := SourceDocument{ID: "doc-1", PaperID: "paper-1", Text: "测试使用了十个样本。"}
	doc.SHA256 = SourceTextSHA256(doc.Text)
	valid := Evidence{PaperID: "paper-1", DocumentID: "doc-1", StartRune: 0, EndRune: 9, Quote: "测试使用了十个样本"}
	if err := ValidateEvidence(doc, valid); err != nil {
		t.Fatal(err)
	}
	valid.Quote = "测试使用了一百万样本"
	if err := ValidateEvidence(doc, valid); !errors.Is(err, ErrInvalid) {
		t.Fatalf("伪造片段未拒绝: %v", err)
	}
}

func TestValidateEvidenceRejectsOutOfRange(t *testing.T) {
	doc := SourceDocument{ID: "doc-1", PaperID: "paper-1", Text: "abc", SHA256: SourceTextSHA256("abc")}
	err := ValidateEvidence(doc, Evidence{PaperID: "paper-1", DocumentID: "doc-1", StartRune: 0, EndRune: 4, Quote: "abc"})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("越界证据未拒绝: %v", err)
	}
}
