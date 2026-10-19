package httpapi

import (
	"net/http"

	"github.com/goharness/autoresearch/internal/domain"
)

func (a *api) searchAcademic(w http.ResponseWriter, r *http.Request) {
	if a.academic == nil {
		a.fail(w, r, domain.ErrUnavailable)
		return
	}
	var query domain.PaperQuery
	if err := decode(w, r, &query); err != nil {
		a.fail(w, r, err)
		return
	}
	items, err := a.academic.Search(r.Context(), query)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
