package httpapi

import (
	"context"
	"net/http"
	"time"
)

type Pinger interface{ Ping(context.Context) error }

func NewHealthHandler(store Pinger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			writeProblem(w, 503, "unavailable", "本地数据服务暂不可用")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	return mux
}
