package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// HashAPIKey returns the SHA-256 hash of the API key
func HashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// GetAPIKeyPrefix returns the visual prefix for key storage
func GetAPIKeyPrefix(key string) string {
	if len(key) <= 8 {
		return key
	}
	return key[:8]
}

// SeedAPIKeys seeds configured API keys at startup if the database table is empty.
func (db *DB) SeedAPIKeys(ctx context.Context, apiKeys map[string]string) error {
	if len(apiKeys) == 0 {
		return nil
	}

	// Start a transaction
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if table is empty
	var count int
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM client_api_keys").Scan(&count)
	if err != nil {
		return fmt.Errorf("check client_api_keys count: %w", err)
	}

	// If empty, insert the keys
	if count == 0 {
		for clientID, key := range apiKeys {
			prefix := GetAPIKeyPrefix(key)
			hashedKey := HashAPIKey(key)

			_, err = tx.Exec(ctx, `
				INSERT INTO client_api_keys (client_id, key_prefix, hashed_key, status)
				VALUES ($1, $2, $3, 'active')
				ON CONFLICT (client_id) DO NOTHING`,
				clientID, prefix, hashedKey,
			)
			if err != nil {
				return fmt.Errorf("seed client %s: %w", clientID, err)
			}
		}
	}

	return tx.Commit(ctx)
}

// CreateClientAPIKey inserts a new client API key record.
func (db *DB) CreateClientAPIKey(ctx context.Context, clientID, prefix, hashedKey string) error {
	_, err := db.pool.Exec(ctx, `
		INSERT INTO client_api_keys (client_id, key_prefix, hashed_key, status)
		VALUES ($1, $2, $3, 'active')
		ON CONFLICT (client_id) DO UPDATE 
		SET key_prefix = EXCLUDED.key_prefix, 
		    hashed_key = EXCLUDED.hashed_key, 
		    status = 'active',
		    updated_at = NOW()`,
		clientID, prefix, hashedKey,
	)
	if err != nil {
		return fmt.Errorf("create client API key: %w", err)
	}
	return nil
}

// GetAPIKeyByClientID retrieves the key details for a specific client.
func (db *DB) GetAPIKeyByClientID(ctx context.Context, clientID string) (string, string, error) {
	var hashedKey, status string
	err := db.pool.QueryRow(ctx, `
		SELECT hashed_key, status 
		FROM client_api_keys 
		WHERE client_id = $1`,
		clientID,
	).Scan(&hashedKey, &status)
	if err != nil {
		return "", "", err
	}
	return hashedKey, status, nil
}

// GetClientIDByHashedKey resolves client_id from the hashed token key.
func (db *DB) GetClientIDByHashedKey(ctx context.Context, hashedKey string) (string, string, error) {
	var clientID, status string
	err := db.pool.QueryRow(ctx, `
		SELECT client_id, status 
		FROM client_api_keys 
		WHERE hashed_key = $1`,
		hashedKey,
	).Scan(&clientID, &status)
	if err != nil {
		return "", "", err
	}
	return clientID, status, nil
}
