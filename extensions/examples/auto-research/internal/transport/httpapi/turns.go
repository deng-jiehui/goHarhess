package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
)

func unavailableRuntimeError() error { return domain.ErrUnavailable }

func (a *api) sendMessage(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	var in app.SendInput
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	result, err := a.turns.Send(r.Context(), scope(r), r.PathValue("sessionID"), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (a *api) getTurn(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	turn, err := a.turns.Get(r.Context(), scope(r), r.PathValue("sessionID"), r.PathValue("turnID"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, turn)
}

func (a *api) getSnapshot(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	limit := queryInt(r, "limit", 100)
	snapshot, err := a.turns.Snapshot(r.Context(), scope(r), r.PathValue("sessionID"), limit)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (a *api) listEvents(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		flusher, ok := w.(http.Flusher)
		if !ok {
			a.fail(w, r, domain.ErrUnavailable)
			return
		}
		after := int64(queryInt(r, "after", 0))
		if header := r.Header.Get("Last-Event-ID"); header != "" {
			if parsed, parseErr := strconv.ParseInt(header, 10, 64); parseErr == nil && parsed > after {
				after = parsed
			}
		}
		limit := queryInt(r, "limit", 100)
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		for {
			events, err := a.turns.Events(r.Context(), scope(r), r.PathValue("sessionID"), after, limit)
			if err != nil {
				return
			}
			for _, event := range events {
				data, _ := json.Marshal(event)
				_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.ID, event.Kind, data)
				after = event.ID
			}
			if len(events) > 0 {
				flusher.Flush()
			}
			snapshot, snapshotErr := a.turns.Snapshot(r.Context(), scope(r), r.PathValue("sessionID"), 1)
			if snapshotErr != nil || !snapshot.Turn.Active() {
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			case <-deadline.C:
				return
			}
		}
	}
	after := int64(queryInt(r, "after", 0))
	events, err := a.turns.Events(r.Context(), scope(r), r.PathValue("sessionID"), after, queryInt(r, "limit", 100))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": events})
}

func (a *api) listNotes(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	notes, err := a.turns.Notes(r.Context(), scope(r), queryInt(r, "limit", 20))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": notes})
}

func (a *api) deleteNote(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	if err := a.turns.DeleteNote(r.Context(), scope(r), r.PathValue("noteID")); err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) resumeTurn(w http.ResponseWriter, r *http.Request) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	var in app.ResumeInput
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	result, err := a.turns.Resume(r.Context(), scope(r), r.PathValue("sessionID"), r.PathValue("turnID"), in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (a *api) retryTurn(w http.ResponseWriter, r *http.Request) {
	a.actionTurn(w, r, func(in app.TurnActionInput) (app.TurnActionResult, error) {
		return a.turns.Retry(r.Context(), scope(r), r.PathValue("sessionID"), r.PathValue("turnID"), in)
	})
}

func (a *api) cancelTurn(w http.ResponseWriter, r *http.Request) {
	a.actionTurn(w, r, func(in app.TurnActionInput) (app.TurnActionResult, error) {
		return a.turns.Cancel(r.Context(), scope(r), r.PathValue("sessionID"), r.PathValue("turnID"), in)
	})
}

func (a *api) actionTurn(w http.ResponseWriter, r *http.Request, action func(app.TurnActionInput) (app.TurnActionResult, error)) {
	if a.turns == nil {
		a.fail(w, r, unavailableRuntimeError())
		return
	}
	var in app.TurnActionInput
	if err := decode(w, r, &in); err != nil {
		a.fail(w, r, err)
		return
	}
	result, err := action(in)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func queryInt(r *http.Request, key string, fallback int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}
