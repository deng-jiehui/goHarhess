package domain

import "context"

type Paper struct {
	ID           string   `json:"id"`
	ProjectID    string   `json:"project_id"`
	SourceKey    string   `json:"source_key"`
	Title        string   `json:"title"`
	Authors      []string `json:"authors"`
	Year         int      `json:"year"`
	DOI          string   `json:"doi,omitempty"`
	ArxivID      string   `json:"arxiv_id,omitempty"`
	URL          string   `json:"url,omitempty"`
	Abstract     string   `json:"abstract,omitempty"`
	Tags         []string `json:"tags"`
	Note         string   `json:"note"`
	ReadingState string   `json:"reading_state"`
	Revision     int64    `json:"revision"`
}

type Candidate struct {
	SourceKey string   `json:"source_key"`
	Title     string   `json:"title"`
	URL       string   `json:"url,omitempty"`
	DOI       string   `json:"doi,omitempty"`
	ArxivID   string   `json:"arxiv_id,omitempty"`
	Authors   []string `json:"authors"`
	Year      int      `json:"year"`
	Abstract  string   `json:"abstract,omitempty"`
	Provider  string   `json:"provider"`
}

type PaperQuery struct {
	Text     string `json:"text"`
	Provider string `json:"provider"`
	YearFrom int    `json:"year_from"`
	YearTo   int    `json:"year_to"`
	Limit    int    `json:"limit"`
}

type SourceDocument struct {
	ID         string `json:"id"`
	PaperID    string `json:"paper_id"`
	ArtifactID string `json:"artifact_id"`
	Text       string `json:"text"`
	SHA256     string `json:"sha256"`
	Locator    string `json:"locator"`
}

type PaperSource interface {
	Search(context.Context, PaperQuery) ([]Candidate, error)
	Resolve(context.Context, string) (Candidate, error)
	Read(context.Context, string, string) (SourceDocument, error)
}
