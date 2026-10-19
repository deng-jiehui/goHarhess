package httpapi

import (
	"net/http"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
)

func (a *api) listProjects(w http.ResponseWriter, r *http.Request) {
	archived, err := archivedFilter(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	items, err := a.projects.List(r.Context(), scope(r).UserID, archived)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (a *api) createProject(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title string `json:"title"`
	}
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	p, err := a.projects.Create(r.Context(), scope(r).UserID, in.Title)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/projects/"+p.ID)
	writeJSON(w, 201, p)
}

func (a *api) getProject(w http.ResponseWriter, r *http.Request) {
	p, err := a.projects.Get(r.Context(), scope(r))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, p)
}

func (a *api) updateProject(w http.ResponseWriter, r *http.Request) {
	var in app.ProjectPatch
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	p, err := a.projects.Update(r.Context(), scope(r), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, p)
}

func (a *api) archiveProject(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Revision int64 `json:"revision"`
		Archived *bool `json:"archived"`
	}
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if in.Archived == nil {
		a.fail(w, r, domain.ErrInvalid)
		return
	}
	p, err := a.projects.Archive(r.Context(), scope(r), in.Revision, *in.Archived)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, p)
}

func (a *api) deleteProject(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Revision     int64  `json:"revision"`
		ConfirmTitle string `json:"confirm_title"`
	}
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := a.projects.Delete(r.Context(), scope(r), in.ConfirmTitle, in.Revision); err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
