package handler

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/apoorv/distributed-job-processor/internal/repository/postgres"
)

type ClientCreator interface {
	CreateClientAPIKey(ctx context.Context, clientID, prefix, hashedKey string) error
}

type AdminHandler struct {
	db        ClientCreator
	masterKey string
	log       *slog.Logger
}

func NewAdminHandler(db ClientCreator, masterKey string, log *slog.Logger) *AdminHandler {
	return &AdminHandler{
		db:        db,
		masterKey: masterKey,
		log:       log,
	}
}

type CreateClientRequest struct {
	ClientID string `json:"client_id"`
}

type CreateClientResponse struct {
	ClientID string `json:"client_id"`
	APIKey   string `json:"api_key"`
}

func (h *AdminHandler) CreateClient(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Verify X-Admin-Key against MasterKey
	adminKey := r.Header.Get("X-Admin-Key")
	if adminKey == "" || h.masterKey == "" || subtle.ConstantTimeCompare([]byte(adminKey), []byte(h.masterKey)) != 1 {
		h.log.WarnContext(ctx, "admin.unauthorized", "path", r.URL.Path, "ip", r.RemoteAddr)
		w.Header().Set("WWW-Authenticate", `Custom realm="admin"`)
		http.Error(w, `{"error":"unauthorized admin key"}`, http.StatusUnauthorized)
		return
	}

	var req CreateClientRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	req.ClientID = strings.TrimSpace(req.ClientID)
	if req.ClientID == "" {
		http.Error(w, `{"error":"client_id is required"}`, http.StatusBadRequest)
		return
	}

	// Generate cryptographically secure API key
	bytes := make([]byte, 24)
	if _, err := rand.Read(bytes); err != nil {
		h.log.ErrorContext(ctx, "admin.generate_key_failed", "error", err)
		http.Error(w, `{"error":"failed to generate API key"}`, http.StatusInternalServerError)
		return
	}
	apiKey := fmt.Sprintf("apikey_live_%s", hex.EncodeToString(bytes))

	// Hash and store the API key
	prefix := postgres.GetAPIKeyPrefix(apiKey)
	hashedKey := postgres.HashAPIKey(apiKey)

	err := h.db.CreateClientAPIKey(ctx, req.ClientID, prefix, hashedKey)
	if err != nil {
		h.log.ErrorContext(ctx, "admin.create_client_failed", "client_id", req.ClientID, "error", err)
		http.Error(w, `{"error":"failed to register client in database"}`, http.StatusInternalServerError)
		return
	}

	h.log.InfoContext(ctx, "admin.client_created", "client_id", req.ClientID)

	// Return plaintext token only once
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(CreateClientResponse{
		ClientID: req.ClientID,
		APIKey:   apiKey,
	})
}
