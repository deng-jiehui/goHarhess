package httpapi

import (
	"net/http"

	"github.com/goharness/autoresearch/internal/app"
)

func (a *api) getModelSettings(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	writeJSON(w, http.StatusOK, a.turns.ModelSettings())
}

func (a *api) updateModelSettings(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	var input app.ModelConfigInput
	if err := decode(w, r, &input); err != nil {
		a.fail(w, r, err)
		return
	}
	settings, err := a.turns.ConfigureModel(input)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, settings)
}
