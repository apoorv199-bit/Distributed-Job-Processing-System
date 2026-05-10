package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"
)

// PanicRecovery catches any unhandled panic in a handler,
// logs the full stack trace, and returns 500 instead of crashing the process.
func PanicRecovery(log *slog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					log.ErrorContext(r.Context(), "panic.recovered",
						"error", err,
						"stack", string(debug.Stack()),
						"path", r.URL.Path,
						"method", r.Method,
					)
					http.Error(w,
						`{"error":"internal server error"}`,
						http.StatusInternalServerError,
					)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
