package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/goharness/autoresearch/internal/transport/httpapi"
)

func TestBearerProtectionRejectsMissingAndIncorrectTokens(t *testing.T) {
	handler := httpapi.Protect("only-this-token", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, tc := range []struct {
		header string
		want   int
	}{{"", 401}, {"Bearer wrong", 401}, {"Bearer only-this-token", 204}} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/projects", nil)
		req.Header.Set("Authorization", tc.header)
		handler.ServeHTTP(rr, req)
		if rr.Code != tc.want {
			t.Fatalf("认证结果 = %d, want %d", rr.Code, tc.want)
		}
	}
}
