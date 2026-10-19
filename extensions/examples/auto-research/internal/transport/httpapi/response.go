package httpapi

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
)

type Problem struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeProblem(w http.ResponseWriter, status int, code, message string) {
	id := w.Header().Get("X-Request-ID")
	if id == "" {
		id = rand.Text()
		w.Header().Set("X-Request-ID", id)
	}
	writeJSON(w, status, Problem{Code: code, Message: message, RequestID: id})
}
