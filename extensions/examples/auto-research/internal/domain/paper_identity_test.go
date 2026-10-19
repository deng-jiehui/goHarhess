package domain

import "testing"

func TestNormalizePaperKeyNormalizesDOIAndArxivVersions(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "doi", in: "https://doi.org/10.1234/ABC", want: "doi:10.1234/abc"},
		{name: "arxiv version", in: "arXiv:2401.00001v2", want: "arxiv:2401.00001"},
		{name: "arxiv url", in: "https://arxiv.org/abs/2401.00001v3", want: "arxiv:2401.00001"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizePaperKey(tt.in)
			if err != nil {
				t.Fatalf("NormalizePaperKey() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("NormalizePaperKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizePaperKeyRejectsUnknownReference(t *testing.T) {
	if _, err := NormalizePaperKey("not a paper reference"); err == nil {
		t.Fatal("NormalizePaperKey() error = nil")
	}
}
