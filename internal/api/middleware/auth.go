package middleware

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
)

// APIKeyAuth protects all routes behind a static API key.
// Header: Authorization: Bearer <key>
// In production replace with JWT or mTLS.
func APIKeyAuth(apiKey string, log *slog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// Health + metrics endpoints are always public
			if r.URL.Path == "/health/live" ||
				r.URL.Path == "/health/ready" ||
				r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			token := extractBearer(r.Header.Get("Authorization"))
			if token == "" {
				token = r.Header.Get("X-API-Key") // also accept X-API-Key header
			}

			// constant-time comparison — prevents timing attacks
			if subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
				log.WarnContext(r.Context(), "auth.rejected",
					"path", r.URL.Path,
					"ip", r.RemoteAddr,
				)
				w.Header().Set("WWW-Authenticate", `Bearer realm="job-processor"`)
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func extractBearer(header string) string {
	if strings.HasPrefix(header, "Bearer ") {
		return strings.TrimPrefix(header, "Bearer ")
	}
	return header
}
