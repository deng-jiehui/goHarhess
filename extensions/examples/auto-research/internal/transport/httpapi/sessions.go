package httpapi

import (
	"net/http"

	"github.com/goharness/autoresearch/internal/app"
)

func (a *api) listSessions(w http.ResponseWriter, r *http.Request) {
	archived, err := archivedFilter(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	items, err := a.sessions.List(r.Context(), scope(r), archived)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (a *api) createSession(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title string `json:"title"`
	}
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	session, err := a.sessions.Create(r.Context(), scope(r), in.Title)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 201, session)
}

func (a *api) getSession(w http.ResponseWriter, r *http.Request) {
	session, err := a.sessions.Get(r.Context(), scope(r), r.PathValue("sessionID"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, session)
}

func (a *api) updateSession(w http.ResponseWriter, r *http.Request) {
	var in app.SessionPatch
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	session, err := a.sessions.Update(r.Context(), scope(r), r.PathValue("sessionID"), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, 200, session)
}

func (a *api) deleteSession(w http.ResponseWriter, r *http.Request) {
	if err := a.sessions.Delete(r.Context(), scope(r), r.PathValue("sessionID")); err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
