package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

type Evidence struct {
	ID         string `json:"id"`
	ProjectID  string `json:"project_id"`
	PaperID    string `json:"paper_id"`
	DocumentID string `json:"document_id"`
	Field      string `json:"field"`
	Value      string `json:"value"`
	Quote      string `json:"quote"`
	Locator    string `json:"locator"`
	StartRune  int    `json:"start_rune"`
	EndRune    int    `json:"end_rune"`
}

func NormalizeSourceText(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	return strings.ReplaceAll(text, "\r", "\n")
}

func SourceTextSHA256(text string) string {
	digest := sha256.Sum256([]byte(NormalizeSourceText(text)))
	return hex.EncodeToString(digest[:])
}

func ValidateEvidence(document SourceDocument, evidence Evidence) error {
	text := []rune(NormalizeSourceText(document.Text))
	if evidence.PaperID == "" || evidence.PaperID != document.PaperID || evidence.DocumentID == "" || evidence.DocumentID != document.ID {
		return fmt.Errorf("%w：证据来源不属于当前文档", ErrInvalid)
	}
	if evidence.StartRune < 0 || evidence.EndRune <= evidence.StartRune || evidence.EndRune > len(text) {
		return fmt.Errorf("%w：证据区间越界", ErrInvalid)
	}
	if evidence.Quote != string(text[evidence.StartRune:evidence.EndRune]) {
		return fmt.Errorf("%w：引用片段与冻结文档不一致", ErrInvalid)
	}
	if document.SHA256 != "" && document.SHA256 != SourceTextSHA256(document.Text) {
		return fmt.Errorf("%w：文档校验和不匹配", ErrInvalid)
	}
	if utf8.RuneCountInString(evidence.Quote) == 0 {
		return fmt.Errorf("%w：引用片段不能为空", ErrInvalid)
	}
	return nil
}
