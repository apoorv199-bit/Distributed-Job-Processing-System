package middleware

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/apoorv/distributed-job-processor/internal/repository/postgres"
)

type ContextKey string

const ClientNameKey ContextKey = "client_id"

// APIKeyAuth protects routes using Database-backed and Redis-cached key lookup.
func APIKeyAuth(db domain.JobRepository, redisClient domain.QueueBroker, masterKey string, log *slog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// Health + admin endpoints are always public or have their own auth
			if r.URL.Path == "/health/live" ||
				r.URL.Path == "/health/ready" ||
				r.URL.Path == "/api/v1/admin/clients" {
				next.ServeHTTP(w, r)
				return
			}

			adminKey := r.Header.Get("X-Admin-Key")
			clientID := r.Header.Get("X-Client-ID")
			apiKey := r.Header.Get("X-API-Key")

			var resolvedClient string
			var status string
			var err error

			// Check if administrative request using X-Admin-Key
			if adminKey != "" && masterKey != "" && subtle.ConstantTimeCompare([]byte(adminKey), []byte(masterKey)) == 1 {
				resolvedClient = "admin"
			} else if clientID != "" && apiKey != "" {
				// Hash incoming api key
				hashedInput := postgres.HashAPIKey(apiKey)

				// Check Redis Cache
				cacheKey := fmt.Sprintf("auth:client:%s", clientID)
				cachedVal, cacheErr := redisClient.Raw().Get(ctx, cacheKey).Result()

				var expectedHash string
				if cacheErr == nil && cachedVal != "" {
					// Format in cache is "hashedKey:status"
					parts := strings.SplitN(cachedVal, ":", 2)
					if len(parts) == 2 {
						expectedHash = parts[0]
						status = parts[1]
					}
				}

				// If cache miss, fetch from Postgres
				if expectedHash == "" {
					expectedHash, status, err = db.GetAPIKeyByClientID(ctx, clientID)
					if err == nil {
						// Cache in Redis for 5 minutes
						valToCache := fmt.Sprintf("%s:%s", expectedHash, status)
						_ = redisClient.Raw().Set(ctx, cacheKey, valToCache, 5*time.Minute).Err()
					} else {
						log.WarnContext(ctx, "auth.db_lookup_failed", "client_id", clientID, "error", err)
					}
				}

				// Verify hash and status
				if expectedHash != "" && status == "active" {
					if subtle.ConstantTimeCompare([]byte(hashedInput), []byte(expectedHash)) == 1 {
						resolvedClient = clientID
					}
				}
			}

			// Rejection
			if resolvedClient == "" {
				log.WarnContext(ctx, "auth.rejected",
					"path", r.URL.Path,
					"ip", r.RemoteAddr,
					"client_id", clientID,
				)
				w.Header().Set("WWW-Authenticate", `Custom realm="job-processor"`)
				http.Error(w, `{"error":"unauthorized: invalid credentials"}`, http.StatusUnauthorized)
				return
			}

			// Inject into Context
			ctx = context.WithValue(ctx, ClientNameKey, resolvedClient)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireAdmin ensures that the authenticated client in context is 'admin'.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID, _ := r.Context().Value(ClientNameKey).(string)
		if clientID != "admin" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"forbidden: admin credentials required"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}
