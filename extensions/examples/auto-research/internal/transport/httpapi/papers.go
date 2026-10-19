package httpapi

import (
	"net/http"
	"strconv"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
)

func (a *api) listPapers(w http.ResponseWriter, r *http.Request) {
	if a.papers == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	query := domain.PaperQuery{
		Text:     r.URL.Query().Get("q"),
		Provider: r.URL.Query().Get("provider"),
	}
	var err error
	if value := r.URL.Query().Get("year_from"); value != "" {
		query.YearFrom, err = strconv.Atoi(value)
		if err != nil {
			a.fail(w, r, domain.ErrInvalid)
			return
		}
	}
	if value := r.URL.Query().Get("year_to"); value != "" {
		query.YearTo, err = strconv.Atoi(value)
		if err != nil {
			a.fail(w, r, domain.ErrInvalid)
			return
		}
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		query.Limit, err = strconv.Atoi(value)
		if err != nil {
			a.fail(w, r, domain.ErrInvalid)
			return
		}
	}
	items, err := a.papers.List(r.Context(), scope(r), query)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *api) importPaper(w http.ResponseWriter, r *http.Request) {
	if a.papers == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	var in domain.Candidate
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	paper, err := a.papers.Import(r.Context(), scope(r), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/projects/"+paper.ProjectID+"/papers/"+paper.ID)
	writeJSON(w, http.StatusCreated, paper)
}

func (a *api) getPaper(w http.ResponseWriter, r *http.Request) {
	if a.papers == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	paper, err := a.papers.Get(r.Context(), scope(r), r.PathValue("paperID"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, paper)
}

func (a *api) updatePaper(w http.ResponseWriter, r *http.Request) {
	if a.papers == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	var in app.PaperPatch
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	paper, err := a.papers.Update(r.Context(), scope(r), r.PathValue("paperID"), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, paper)
}

func (a *api) listPaperDocuments(w http.ResponseWriter, r *http.Request) {
	if a.evidence == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	items, err := a.evidence.ListDocuments(r.Context(), scope(r), r.PathValue("paperID"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *api) savePaperDocument(w http.ResponseWriter, r *http.Request) {
	if a.evidence == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	var in domain.SourceDocument
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	in.PaperID = r.PathValue("paperID")
	document, err := a.evidence.SaveDocument(r.Context(), scope(r), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, document)
}

func (a *api) listPaperEvidence(w http.ResponseWriter, r *http.Request) {
	if a.evidence == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	items, err := a.evidence.List(r.Context(), scope(r), r.PathValue("paperID"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *api) savePaperEvidence(w http.ResponseWriter, r *http.Request) {
	if a.evidence == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	var in domain.Evidence
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	in.PaperID = r.PathValue("paperID")
	evidence, err := a.evidence.Save(r.Context(), scope(r), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, evidence)
}
