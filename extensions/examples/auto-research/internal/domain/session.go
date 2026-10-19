package domain

type Session struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Title     string `json:"title"`
	Archived  bool   `json:"archived"`
}
