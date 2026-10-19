package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

func Protect(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" {
			header := r.Header.Get("Authorization")
			actual := sha256.Sum256([]byte(strings.TrimPrefix(header, "Bearer ")))
			expected := sha256.Sum256([]byte(token))
			if !strings.HasPrefix(header, "Bearer ") || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeProblem(w, 401, "unauthorized", "需要有效的访问凭据")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
